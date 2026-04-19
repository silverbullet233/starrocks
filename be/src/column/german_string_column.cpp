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

#include "column/german_string_column.h"

#include "column/mysql_row_buffer.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "types/datum.h"

namespace starrocks {

// ---------- helpers ---------------------------------------------------------

void GermanStringColumn::build_german_string(GermanString* dst, const char* data, size_t len,
                                             GermanStringArena* arena) {
    if (len <= GermanString::INLINE_MAX_LENGTH) {
        // Inline: the two-arg constructor fills inline bytes and zero-pads the tail.
        new (dst) GermanString(data, len);
    } else {
        DCHECK(arena != nullptr);
        void* buf = arena->allocate(len);
        // Three-arg constructor writes prefix, copies payload into |buf|, and sets ptr.
        new (dst) GermanString(data, len, buf);
    }
}

void GermanStringColumn::_append_raw(const char* data, size_t len) {
    // Construct the new element directly at the back of `_data` instead of
    // emplacing a default-zeroed GermanString and then re-constructing over
    // it via placement new. The default ctor zero-fills 16 bytes, and the
    // inline ctor zero-fills again before memcpy-ing the payload -- skipping
    // the default step halves the memory traffic of the hot scan/build path
    // (measured ~20% scan overhead on 500K row short-string columns vs the
    // BinaryColumn baseline; this is where most of it sits).
    if (len <= GermanString::INLINE_MAX_LENGTH) {
        _data.emplace_back(data, len);
    } else {
        void* buf = _get_arena()->allocate(len);
        _data.emplace_back(data, len, buf);
    }
}

void GermanStringColumn::_append_from(const GermanString& gs) {
    _append_raw(gs.get_data(), gs.len);
}

// ---------- appends ---------------------------------------------------------

void GermanStringColumn::append(const Slice& str) {
    _append_raw(str.data, str.size);
}

void GermanStringColumn::append(const GermanString& gs) {
    _append_from(gs);
}

void GermanStringColumn::append_string(const std::string& str) {
    _append_raw(str.data(), str.size());
}

void GermanStringColumn::append_bytes(const char* data, size_t len) {
    _append_raw(data, len);
}

void GermanStringColumn::append_datum(const Datum& datum) {
    // Datum stores strings as `Slice`; `Datum::get_german_string()` is just a
    // thin wrapper over the same bytes, so we route through `get_slice()` and
    // copy into this column's arena for long strings.
    const Slice& s = datum.get_slice();
    _append_raw(s.data, s.size);
}

void GermanStringColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK_LE(offset + count, src.size());
    const auto& s = down_cast<const GermanStringColumn&>(src);
    _data.reserve(_data.size() + count);
    for (size_t i = 0; i < count; ++i) {
        _append_from(s._data[offset + i]);
    }
}

void GermanStringColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& s = down_cast<const GermanStringColumn&>(src);
    _data.reserve(_data.size() + size);
    for (uint32_t i = 0; i < size; ++i) {
        _append_from(s._data[indexes[from + i]]);
    }
}

void GermanStringColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    const auto& s = down_cast<const GermanStringColumn&>(src);
    const auto& gs = s._data[index];
    _data.reserve(_data.size() + size);
    for (uint32_t i = 0; i < size; ++i) {
        _append_from(gs);
    }
}

void GermanStringColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* slice = reinterpret_cast<const Slice*>(value);
    _data.reserve(_data.size() + count);
    for (size_t i = 0; i < count; ++i) {
        _append_raw(slice->data, slice->size);
    }
}

bool GermanStringColumn::append_strings(const Slice* data, size_t size) {
    _data.reserve(_data.size() + size);
    for (size_t i = 0; i < size; ++i) {
        _append_raw(data[i].data, data[i].size);
    }
    return true;
}

bool GermanStringColumn::append_continuous_strings(const Slice* data, size_t size) {
    // Regardless of contiguity in source memory, we must make an owned copy for
    // long strings, so this is functionally identical to append_strings.
    return append_strings(data, size);
}

// ---------- replicate / update / fill ---------------------------------------

StatusOr<MutableColumnPtr> GermanStringColumn::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = GermanStringColumn::create();
    auto src_size = offsets.size() - 1;
    DCHECK_LE(src_size, this->size());
    size_t total_rows = offsets.back();
    dest->_data.reserve(total_rows);
    for (size_t i = 0; i < src_size; ++i) {
        const auto& gs = _data[i];
        for (uint32_t j = offsets[i]; j < offsets[i + 1]; ++j) {
            dest->_append_from(gs);
        }
    }
    return dest;
}

void GermanStringColumn::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); ++i) {
        if (filter[i] == 1 && _data[i].len > 0) {
            indexes.push_back(static_cast<uint32_t>(i));
        }
    }
    if (indexes.empty()) {
        return;
    }
    auto default_column = clone_empty();
    default_column->append_default(indexes.size());
    update_rows(*default_column, indexes.data());
}

void GermanStringColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& s = down_cast<const GermanStringColumn&>(src);
    size_t replace_num = s.size();
    auto* arena = _get_arena();
    for (size_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _data.size());
        // Rebuild destination row from source bytes. For long strings this copies
        // into this column's arena; for inline strings it is a 16-byte overwrite.
        const auto& src_gs = s._data[i];
        build_german_string(&_data[indexes[i]], src_gs.get_data(), src_gs.len, arena);
    }
}

void GermanStringColumn::assign(size_t n, size_t idx) {
    // Materialize the current value at |idx| into an owned std::string first so
    // that clearing the arena does not invalidate the source bytes.
    const std::string value = static_cast<std::string>(_data[idx]);
    _data.clear();
    if (_arena != nullptr) {
        _arena->clear();
    }
    _data.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        _append_raw(value.data(), value.size());
    }
}

void GermanStringColumn::remove_first_n_values(size_t count) {
    DCHECK_LE(count, _data.size());
    if (count == 0) {
        return;
    }
    // Rebuild into a fresh column so the new arena owns surviving long-rep bytes.
    auto tmp = GermanStringColumn::create();
    tmp->append(*this, count, _data.size() - count);
    swap_column(*tmp);
}

// ---------- filter ----------------------------------------------------------

size_t GermanStringColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    // Long-rep byte storage is already owned by _arena; in-place compaction is
    // safe because we only reorder 16-byte GermanString entries that point into
    // the same arena.
    size_t result_offset = from;
    for (size_t i = from; i < to; ++i) {
        if (filter[i]) {
            if (result_offset != i) {
                _data[result_offset] = _data[i];
            }
            ++result_offset;
        }
    }
    this->resize(result_offset);
    return result_offset;
}

// ---------- comparison ------------------------------------------------------

int GermanStringColumn::compare_at(size_t left, size_t right, const Column& rhs, int /*nan_direction_hint*/) const {
    const auto& r = down_cast<const GermanStringColumn&>(rhs);
    return _data[left].compare(r._data[right]);
}

// ---------- serialization ---------------------------------------------------

uint32_t GermanStringColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    for (const auto& gs : _data) {
        max_size = std::max<uint32_t>(max_size, gs.len);
    }
    return max_size + sizeof(uint32_t);
}

uint32_t GermanStringColumn::serialize_default(uint8_t* pos) const {
    uint32_t binary_size = 0;
    memcpy(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}

void GermanStringColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

size_t GermanStringColumn::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                       uint32_t max_row_size, size_t start, size_t count) const {
    dst += byte_offset;
    for (size_t i = start; i < start + count; ++i, dst += byte_interval) {
        const auto& gs = _data[i];
        const size_t length = gs.len;
        if (length > max_row_size) {
            *dst = 0xFF;
        } else if (length > 0 && gs.get_data()[length - 1] == 0) {
            *dst = 0xFF;
        } else {
            memcpy(dst, gs.get_data(), length);
        }
    }
    return max_row_size;
}

const uint8_t* GermanStringColumn::deserialize_and_append(const uint8_t* pos) {
    uint32_t string_size{};
    memcpy(&string_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    _append_raw(reinterpret_cast<const char*>(pos), string_size);
    return pos + string_size;
}

void GermanStringColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    _data.reserve(_data.size() + chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

// ---------- checksums -------------------------------------------------------

int64_t GermanStringColumn::xor_checksum(uint32_t from, uint32_t to) const {
    int64_t xor_checksum = 0;
    for (uint32_t i = from; i < to; ++i) {
        const auto& gs = _data[i];
        size_t num = gs.len;
        const auto* src = reinterpret_cast<const uint8_t*>(gs.get_data());
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

// ---------- misc ------------------------------------------------------------

void GermanStringColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool /*is_binary_protocol*/) const {
    const auto& gs = _data[idx];
    buf->push_string(gs.get_data(), gs.len);
}

Datum GermanStringColumn::get(size_t n) const {
    // Canonical storage for a string-valued Datum is `Slice`. The bytes are
    // owned by this column's arena (for long strings) or live inline inside the
    // GermanString row, so the Slice borrows into memory that outlives the
    // caller's use of the Datum.
    return Datum(Slice(_data[n].get_data(), _data[n].len));
}

std::string GermanStringColumn::debug_item(size_t idx) const {
    const auto& gs = _data[idx];
    std::string s;
    s.reserve(gs.len + 2);
    s.push_back('\'');
    s.append(gs.get_data(), gs.len);
    s.push_back('\'');
    return s;
}

std::string GermanStringColumn::raw_item_value(size_t idx) const {
    const auto& gs = _data[idx];
    return std::string(gs.get_data(), gs.len);
}

Status GermanStringColumn::capacity_limit_reached() const {
    if (_data.size() >= Column::MAX_CAPACITY_LIMIT) {
        return Status::CapacityLimitExceed(strings::Substitute(
                "Total row count of german_string column exceed the limit: $0",
                std::to_string(Column::MAX_CAPACITY_LIMIT)));
    }
    return Status::OK();
}

void GermanStringColumn::check_or_die() const {
    for (size_t i = 0; i < _data.size(); ++i) {
        const auto& gs = _data[i];
        if (!gs.is_inline()) {
            CHECK_NE(gs.long_rep.ptr, static_cast<uintptr_t>(0));
        }
    }
}

} // namespace starrocks
