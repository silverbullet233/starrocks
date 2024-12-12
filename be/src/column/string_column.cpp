// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "column/string_column.h"
#include <sstream>
#include <stdexcept>
#include "column/column_helper.h"
#include "util/string_view.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "column/bytes.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/raw_container.h"

namespace starrocks {

void StringColumn::check_or_die() const {
    // @TODO how to check
    for (size_t i = 0;i < _views.size(); i++) {

    }
    
}


void StringColumn::append(const Slice& str) {
    if (str.get_size() <= StringView::kInlineBytes) {
        _views.emplace_back(StringView(str.get_data(), str.get_size()));
    } else {
        StringView sv = _buffer.add_string(str.get_data(), str.get_size());
        _views.emplace_back(std::move(sv));
    }
}
void StringColumn::append(const StringView& str) {
    if (str.get_size() < StringView::kInlineBytes) {
        _views.emplace_back(str);
    } else {
        StringView sv = _buffer.add_string(str.get_data(), str.get_size());
        _views.emplace_back(std::move(sv));
    }
}


void StringColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK(offset + count <= src.size());
    const auto& src_column = down_cast<const StringColumn&>(src);
    for (size_t i = offset;i < offset + count; i++) {
        append(src_column.get_view(i));
    }
}


void StringColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_column = down_cast<const StringColumn&>(src);
    for (size_t i = 0;i < size;i++) {
        uint32_t idx = indexes[from + i];
        append(src_column.get_view(idx));
    }
}


void StringColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    auto& src_column = down_cast<const StringColumn&>(src);
    auto value = src_column.get_view(index);

    if (value.get_size() > StringView::kInlineBytes) {
        value = _buffer.add_string(value.get_data(), value.get_size());
    }
    for(size_t i = 0;i < size;i++) {
        append(value);

    }
}

void StringColumn::append_string(const std::string& data) {
    append_string(data.data(), data.size());
}

void StringColumn::append_string(const char* data, size_t size) {
    StringView sv;
    if (size > StringView::kInlineBytes) {
        sv = _buffer.add_string(data, size);
    } else {
        sv = StringView(data, size);
    }
    append(sv);
}

bool StringColumn::append_strings(const Slice* data, size_t size) {
    StringView sv;
    if (data->get_size() > StringView::kInlineBytes) {
        sv = _buffer.add_string(data->get_data(), data->get_size());
    } else {
        sv = StringView(data->get_data(), data->get_size());
    }
    for (size_t i = 0; i < size; i++) {
        append(sv);
    }
    return true;
}

bool StringColumn::append_strings_overflow(const Slice* data, size_t size, size_t max_length) {
    return false;
}

bool StringColumn::append_continuous_strings(const Slice* data, size_t size) {
    if (size == 0) {
        return true;
    }
    // @TODO should we optimize this?
    for (size_t i = 0; i < size; i++) {
        const auto& s = data[i];
        append_string(s.get_data(), s.get_size());
    }
    return true;
}


bool StringColumn::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    if (size == 0) return true;
    // @TODO
    if (fixed_length <= StringView::kInlineBytes) {
        // append sv
        size_t offset = 0;
        for (size_t i = 0;i < size;i++, offset += fixed_length) {
            const auto* ptr = reinterpret_cast<const uint8_t*>(data + offset);
            append(StringView(ptr, fixed_length));
        }
    } else {
        // alloc data
        size_t data_size = size * fixed_length;
        StringView sv = _buffer.add_string(data, data_size);
        const auto* ptr = reinterpret_cast<const uint8_t*>(sv.get_data());
        // split sv
        size_t offset = 0;
        for (size_t i = 0;i < size;i++, offset += fixed_length) {
            append(StringView(ptr + offset, fixed_length));
        }
    }
    return true;
}


void StringColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* slice = reinterpret_cast<const Slice*>(value);
    StringView sv;
    if (slice->get_size() > StringView::kInlineBytes) {
        sv = _buffer.add_string(slice->get_data(), slice->get_size());
    } else {
        sv = StringView(slice->get_data(), slice->get_size());
    }
    for (size_t i = 0; i < count; i++) {
        append(sv);
    }
}

void StringColumn::fill_default(const Filter& filter) {
    CHECK(false) << "fill_default not implemented";
}


void StringColumn::update_rows(const Column& src, const uint32_t* indexes) {
    CHECK(false) << "update_rows not implemented";
}


void StringColumn::assign(size_t n, size_t idx) {
    StringView sv = _views[idx];
    // @TODO should we reset buffer?
    _views.clear();
    for(size_t i = 0;i < n;i++) {
        _views.emplace_back(sv);
    }
}

//TODO(kks): improve this

void StringColumn::remove_first_n_values(size_t count) {
    DCHECK_LE(count, size());
    if (count == 0) {
        return;
    }
    size_t remain_size = size() - count;

    Container new_views;
    new_views.reserve(remain_size);
    for (size_t i = 0;i < remain_size;i++) {
        new_views.emplace_back(_views[count + i]);
    }
    _views.swap(new_views);
}

size_t StringColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    size_t size = ColumnHelper::filter_range<StringView>(filter, _views.data(), from, to);
    this->resize(size);
    return size;
}


int StringColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& right_column = down_cast<const StringColumn&>(rhs);
    return StringView::compare(get_view(left), right_column.get_view(right));
}


uint32_t StringColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    for (const auto& view: _views) {
        max_size = std::max(max_size, static_cast<uint32_t>(view.get_size()));
    }
    return max_size + sizeof(uint32_t);
}


uint32_t StringColumn::serialize(size_t idx, uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = _views[idx].get_size();
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    strings::memcpy_inlined(pos + sizeof(uint32_t), _views[idx].get_data(), binary_size);

    return sizeof(uint32_t) + binary_size;
}


uint32_t StringColumn::serialize_default(uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = 0;
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}


void StringColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                          uint32_t max_one_row_size) {
    // @TODO ???
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}


const uint8_t* StringColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "deserialize_and_append not support";
    throw std::runtime_error("deserialize_and_append not support");
    return pos;
}


void StringColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "DOn't support string column deserialize_and_append_batch";
    throw std::runtime_error("deserialize_and_append_batch not support");
}


void StringColumn::fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hashes[i] = HashUtil::fnv_hash(_views[i].get_data(), _views[i].get_size(), hashes[i]);
    }
}


void StringColumn::crc32_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    // @TODO skip empty?
    for (uint32_t i = from; i < to && !_views[i].empty(); ++i) {
        hashes[i] = HashUtil::zlib_crc_hash(_views[i].get_data(), _views[i].get_size(), hashes[i]);
    }
}


int64_t StringColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of StringColumn
    // For one string, treat it as a number of 64-bit integers and 8-bit integers.
    // XOR all of the integers to get a checksum for one string.
    // XOR all of the checksums to get xor_checksum.
    int64_t xor_checksum = 0;

    for (size_t i = from; i < to; ++i) {
        size_t num = _views[i].get_size();
        const auto* src = reinterpret_cast<const uint8_t*>(_views[i].get_data());

#ifdef __AVX2__
        // AVX2 intructions can improve the speed of XOR procedure of one string.
        __m256i avx2_checksum = _mm256_setzero_si256();
        size_t step = sizeof(__m256i) / sizeof(uint8_t);

        while (num >= step) {
            const __m256i left = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
            avx2_checksum = _mm256_xor_si256(left, avx2_checksum);
            src += step;
            num -= step;
        }
        auto* checksum_vec = reinterpret_cast<int64_t*>(&avx2_checksum);
        size_t eight_byte_step = sizeof(__m256i) / sizeof(int64_t);
        for (size_t j = 0; j < eight_byte_step; ++j) {
            xor_checksum ^= checksum_vec[j];
        }
#endif

        while (num >= 8) {
            xor_checksum ^= *reinterpret_cast<const int64_t*>(src);
            src += 8;
            num -= 8;
        }
        for (size_t j = 0; j < num; ++j) {
            xor_checksum ^= src[j];
        }
    }

    return xor_checksum;
}


void StringColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    buf->push_string(_views[idx].get_data(), _views[idx].get_size());
}


std::string StringColumn::debug_item(size_t idx) const {
    std::string s;
    auto view = get_view(idx);
    s.reserve(view.get_size() + 2);
    s.push_back('\'');
    s.append(view.get_data(), view.get_size());
    s.push_back('\'');
    return s;
}


std::string StringColumn::raw_item_value(size_t idx) const {
    std::string s;
    auto view = get_view(idx);
    s.reserve(view.get_size());
    s.append(view.get_data(), view.get_size());
    return s;
}

StatusOr<ColumnPtr> StringColumn::upgrade_if_overflow() {
    return nullptr;
}


StatusOr<ColumnPtr> StringColumn::downgrade() {
    return nullptr;
}

Status StringColumn::capacity_limit_reached() const {
    return Status::OK();
}
} // namespace starrocks
