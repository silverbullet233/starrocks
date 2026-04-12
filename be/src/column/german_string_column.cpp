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

#include <cstring>
#include <sstream>

#include "column/binary_column.h"
#include "column/mysql_row_buffer.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

GermanStringColumn::~GermanStringColumn() {
    _arena.free_all();
}

GermanStringColumn::GermanStringColumn(GermanStringColumn&& rhs) noexcept
        : _german_strings(std::move(rhs._german_strings)) {
    _arena.acquire_data(&rhs._arena, false);
}

GermanStringColumn& GermanStringColumn::operator=(GermanStringColumn&& rhs) noexcept {
    if (this != &rhs) {
        _german_strings = std::move(rhs._german_strings);
        _arena.free_all();
        _arena.acquire_data(&rhs._arena, false);
    }
    return *this;
}

// ---- Append (GermanString-specific) ----

void GermanStringColumn::_append_to_arena(const char* data, size_t len) {
    auto* ptr = _arena.allocate(static_cast<int64_t>(len));
    strings::memcpy_inlined(ptr, data, len);
    // Use 2-arg constructor: data is already in arena, just store the pointer.
    _german_strings.emplace_back(static_cast<const void*>(ptr), len);
}

void GermanStringColumn::append(const Slice& str) {
    if (str.size <= GermanString::INLINE_MAX_LENGTH) {
        // Inline: data fits in the GermanString itself, no arena needed.
        _german_strings.emplace_back(str);
    } else {
        // Long string: allocate in arena.
        _append_to_arena(str.data, str.size);
    }
}

void GermanStringColumn::append(const GermanString& gs) {
    if (gs.is_inline()) {
        // Inline: just copy the 16-byte struct directly.
        _german_strings.push_back(gs);
    } else {
        // Long string: the GermanString's pointer points to external memory.
        // We must copy the data into our arena for pointer stability.
        _append_to_arena(gs.get_data(), gs.len);
    }
}

// ---- Column virtual method implementations ----

void GermanStringColumn::append_datum(const Datum& datum) {
    append(datum.get_german_string());
}

void GermanStringColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& src_col = down_cast<const GermanStringColumn&>(src);
    _german_strings.reserve(_german_strings.size() + count);
    for (size_t i = 0; i < count; ++i) {
        append(src_col._german_strings[offset + i]);
    }
}

void GermanStringColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_col = down_cast<const GermanStringColumn&>(src);
    _german_strings.reserve(_german_strings.size() + size);
    for (uint32_t i = 0; i < size; ++i) {
        append(src_col._german_strings[indexes[from + i]]);
    }
}

void GermanStringColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    const auto& src_col = down_cast<const GermanStringColumn&>(src);
    const auto& gs = src_col._german_strings[index];
    _german_strings.reserve(_german_strings.size() + size);
    for (uint32_t i = 0; i < size; ++i) {
        append(gs);
    }
}

bool GermanStringColumn::append_strings(const Slice* data, size_t size) {
    _german_strings.reserve(_german_strings.size() + size);
    for (size_t i = 0; i < size; ++i) {
        append(data[i]);
    }
    return true;
}

void GermanStringColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* gs = reinterpret_cast<const GermanString*>(value);
    _german_strings.reserve(_german_strings.size() + count);
    for (size_t i = 0; i < count; ++i) {
        append(*gs);
    }
}

void GermanStringColumn::append_default() {
    _german_strings.emplace_back();
}

void GermanStringColumn::append_default(size_t count) {
    _german_strings.resize(_german_strings.size() + count);
}

// ---- Assign / Remove ----

void GermanStringColumn::assign(size_t n, size_t idx) {
    const auto& gs = _german_strings[idx];
    // Build a new column with n copies of this element
    Container new_gs;
    MemPool new_arena;
    new_gs.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        if (gs.is_inline()) {
            new_gs.push_back(gs);
        } else {
            auto* ptr = new_arena.allocate(static_cast<int64_t>(gs.len));
            strings::memcpy_inlined(ptr, gs.get_data(), gs.len);
            // Use 2-arg constructor: data is already in arena, just store the pointer.
            new_gs.emplace_back(static_cast<const void*>(ptr), gs.len);
        }
    }
    _german_strings = std::move(new_gs);
    _arena.free_all();
    _arena.acquire_data(&new_arena, false);
}

void GermanStringColumn::remove_first_n_values(size_t count) {
    if (count >= _german_strings.size()) {
        _german_strings.clear();
        return;
    }
    // Shift elements left. Arena data is not reclaimed (lazy compaction model).
    size_t new_size = _german_strings.size() - count;
    for (size_t i = 0; i < new_size; ++i) {
        _german_strings[i] = _german_strings[i + count];
    }
    _german_strings.resize(new_size);
}

// ---- Clone ----

MutableColumnPtr GermanStringColumn::clone() const {
    auto p = GermanStringColumn::create();
    auto& dest = down_cast<GermanStringColumn&>(*p);
    dest._german_strings.reserve(_german_strings.size());
    for (size_t i = 0; i < _german_strings.size(); ++i) {
        dest.append(_german_strings[i]);
    }
    return p;
}

// ---- Filter ----

size_t GermanStringColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    // Only updates _german_strings; arena data is untouched (lazy compaction).
    size_t new_size = from;
    for (size_t i = from; i < to; ++i) {
        if (filter[i]) {
            if (new_size != i) {
                _german_strings[new_size] = _german_strings[i];
            }
            ++new_size;
        }
    }
    // Append elements after 'to' range
    size_t old_size = _german_strings.size();
    for (size_t i = to; i < old_size; ++i) {
        _german_strings[new_size] = _german_strings[i];
        ++new_size;
    }
    _german_strings.resize(new_size);
    return new_size;
}

// ---- Fill / Update ----

void GermanStringColumn::fill_default(const Filter& filter) {
    GermanString empty_gs;
    for (size_t i = 0; i < filter.size(); ++i) {
        if (filter[i]) {
            _german_strings[i] = empty_gs;
        }
    }
}

void GermanStringColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& src_col = down_cast<const GermanStringColumn&>(src);
    size_t src_size = src_col.size();
    for (size_t i = 0; i < src_size; ++i) {
        uint32_t idx = indexes[i];
        const auto& gs = src_col._german_strings[i];
        if (gs.is_inline()) {
            _german_strings[idx] = gs;
        } else {
            // Need to copy long string data into our arena.
            auto* ptr = _arena.allocate(static_cast<int64_t>(gs.len));
            strings::memcpy_inlined(ptr, gs.get_data(), gs.len);
            // Use 2-arg constructor: data is already in arena, just store the pointer.
            _german_strings[idx] = GermanString(static_cast<const void*>(ptr), gs.len);
        }
    }
}

// ---- Compare ----

int GermanStringColumn::compare_at(size_t left, size_t right, const Column& rhs,
                                    int nan_direction_hint) const {
    const auto& rhs_col = down_cast<const GermanStringColumn&>(rhs);
    return _german_strings[left].compare(rhs_col._german_strings[right]);
}

// ---- Serialize / Deserialize (stubs) ----

uint32_t GermanStringColumn::max_one_element_serialize_size() const {
    uint32_t max_size = sizeof(uint32_t); // length prefix
    for (size_t i = 0; i < _german_strings.size(); ++i) {
        max_size = std::max(max_size, static_cast<uint32_t>(sizeof(uint32_t) + _german_strings[i].len));
    }
    return max_size;
}

uint32_t GermanStringColumn::serialize(size_t idx, uint8_t* pos) const {
    uint32_t str_len = _german_strings[idx].len;
    strings::memcpy_inlined(pos, &str_len, sizeof(uint32_t));
    strings::memcpy_inlined(pos + sizeof(uint32_t), _german_strings[idx].get_data(), str_len);
    return sizeof(uint32_t) + str_len;
}

uint32_t GermanStringColumn::serialize_default(uint8_t* pos) const {
    uint32_t zero = 0;
    memcpy(pos, &zero, sizeof(uint32_t));
    return sizeof(uint32_t);
}

void GermanStringColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                          uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* GermanStringColumn::deserialize_and_append(const uint8_t* pos) {
    uint32_t str_len = 0;
    memcpy(&str_len, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    append(Slice(reinterpret_cast<const char*>(pos), str_len));
    return pos + str_len;
}

void GermanStringColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

uint32_t GermanStringColumn::serialize_size(size_t idx) const {
    return sizeof(uint32_t) + _german_strings[idx].len;
}

// ---- Byte size ----

size_t GermanStringColumn::byte_size() const {
    return _german_strings.size() * sizeof(GermanString) + _arena.total_allocated_bytes();
}

size_t GermanStringColumn::byte_size(size_t from, size_t size) const {
    size_t total = size * sizeof(GermanString);
    for (size_t i = from; i < from + size; ++i) {
        if (!_german_strings[i].is_inline()) {
            total += _german_strings[i].len;
        }
    }
    return total;
}

size_t GermanStringColumn::byte_size(size_t idx) const {
    size_t total = sizeof(GermanString);
    if (!_german_strings[idx].is_inline()) {
        total += _german_strings[idx].len;
    }
    return total;
}

// ---- MySQL result (stub) ----

void GermanStringColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx,
                                               bool is_binary_protocol) const {
    const auto& gs = _german_strings[idx];
    buf->push_string(gs.get_data(), gs.len);
}

// ---- Name / Debug / Get ----

Datum GermanStringColumn::get(size_t n) const {
    return Datum(_german_strings[n]);
}

std::string GermanStringColumn::debug_item(size_t idx) const {
    const auto& gs = _german_strings[idx];
    return std::string(gs.get_data(), gs.len);
}

std::string GermanStringColumn::debug_string() const {
    std::stringstream ss;
    size_t sz = _german_strings.size();
    ss << "[";
    for (size_t i = 0; i + 1 < sz; ++i) {
        ss << "'" << debug_item(i) << "', ";
    }
    if (sz > 0) {
        ss << "'" << debug_item(sz - 1) << "'";
    }
    ss << "]";
    return ss.str();
}

// ---- Memory usage ----

size_t GermanStringColumn::container_memory_usage() const {
    return _german_strings.capacity() * sizeof(GermanString) + _arena.total_allocated_bytes();
}

// ---- Swap / Reset ----

void GermanStringColumn::swap_column(Column& rhs) {
    auto& r = down_cast<GermanStringColumn&>(rhs);
    using std::swap;
    swap(this->_delete_state, r._delete_state);
    swap(_german_strings, r._german_strings);
    _arena.exchange_data(&r._arena);
}

void GermanStringColumn::reset_column() {
    Column::reset_column();
    _german_strings.clear();
    _arena.free_all();
}

// ---- Capacity limit ----

Status GermanStringColumn::capacity_limit_reached() const {
    if (_german_strings.size() >= Column::MAX_CAPACITY_LIMIT) {
        return Status::InternalError("GermanStringColumn capacity limit reached");
    }
    return Status::OK();
}

// ---- Check ----

void GermanStringColumn::check_or_die() const {
    // Basic sanity: all long strings should have non-null data pointer.
    for (size_t i = 0; i < _german_strings.size(); ++i) {
        const auto& gs = _german_strings[i];
        if (!gs.is_inline()) {
            DCHECK(gs.get_data() != nullptr) << "GermanString at index " << i << " has null data pointer";
        }
    }
}

// ---- Compaction ----

size_t GermanStringColumn::live_arena_bytes() const {
    size_t total = 0;
    for (const auto& gs : _german_strings) {
        if (!gs.is_inline()) {
            total += gs.len;
        }
    }
    return total;
}

bool GermanStringColumn::needs_compaction() const {
    return _arena.total_allocated_bytes() > 2 * static_cast<int64_t>(live_arena_bytes());
}

void GermanStringColumn::compact() {
    MemPool new_arena;
    Container new_gs;
    new_gs.reserve(_german_strings.size());
    for (size_t i = 0; i < _german_strings.size(); ++i) {
        const auto& gs = _german_strings[i];
        if (gs.is_inline()) {
            new_gs.push_back(gs);
        } else {
            auto* ptr = new_arena.allocate(static_cast<int64_t>(gs.len));
            strings::memcpy_inlined(ptr, gs.get_data(), gs.len);
            // Use 2-arg constructor: data is already in arena, just store the pointer.
            new_gs.emplace_back(static_cast<const void*>(ptr), gs.len);
        }
    }
    _german_strings = std::move(new_gs);
    _arena.free_all();
    _arena.acquire_data(&new_arena, false);
}

// ---- Conversion ----

ColumnPtr GermanStringColumn::to_binary_column() const {
    auto bc = BinaryColumn::create();
    bc->reserve(size());
    for (size_t i = 0; i < size(); ++i) {
        const auto& gs = _german_strings[i];
        bc->append(Slice(gs.get_data(), gs.len));
    }
    return bc;
}

} // namespace starrocks
