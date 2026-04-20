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

#pragma once

#include <algorithm>
#include <memory>
#include <span>
#include <sstream>
#include <vector>

#include "base/container/raw_container.h"
#include "base/string/slice.h"
#include "column/column.h"
#include "column/german_string.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

// A simple chunk-list arena used to own bytes backing long-rep GermanString
// entries. Allocations never move, so `long_rep.ptr` remains stable for the
// lifetime of the arena.
//
// Allocations are backed by `Buffer<uint8_t>` (ColumnAllocator) to stay within
// the ColumnCore module boundary.
class GermanStringArena {
public:
    GermanStringArena() = default;
    ~GermanStringArena() = default;

    GermanStringArena(const GermanStringArena&) = delete;
    GermanStringArena& operator=(const GermanStringArena&) = delete;

    // Allocate |size| bytes. Returned pointer is valid until clear() / dtor.
    // Uses resize_uninitialized on the backing chunk so the caller's memcpy
    // is not shadowed by a zero-fill (hot path for long-rep scan decode).
    char* allocate(size_t size) {
        if (size == 0) {
            return nullptr;
        }
        // Oversized allocations get their own chunk so we do not waste space.
        if (size > kChunkSize / 2) {
            _chunks.emplace_back();
            auto& chunk = _chunks.back();
            raw::stl_vector_resize_uninitialized(&chunk, size);
            return reinterpret_cast<char*>(chunk.data());
        }
        if (_chunks.empty() || _chunks.back().size() + size > _chunks.back().capacity()) {
            _chunks.emplace_back();
            _chunks.back().reserve(kChunkSize);
        }
        auto& chunk = _chunks.back();
        const size_t offset = chunk.size();
        raw::stl_vector_resize_uninitialized(&chunk, offset + size);
        return reinterpret_cast<char*>(chunk.data() + offset);
    }

    // Pre-reserve enough contiguous capacity for a bulk sequence of small
    // allocations totalling |total| bytes. Subsequent allocate() calls with
    // size <= kChunkSize/2 skip the chunk-growth branch until the reserved
    // capacity is exhausted. Oversized allocations bypass this path.
    void reserve(size_t total) {
        if (total == 0) return;
        if (total > kChunkSize / 2) {
            // For bulk totals larger than a normal chunk, start a dedicated
            // chunk sized to the full request so per-row allocate() has a
            // single straight-line path.
            _chunks.emplace_back();
            _chunks.back().reserve(total);
            return;
        }
        if (_chunks.empty() || _chunks.back().capacity() - _chunks.back().size() < total) {
            _chunks.emplace_back();
            _chunks.back().reserve(kChunkSize);
        }
    }

    size_t allocated_bytes() const {
        size_t total = 0;
        for (const auto& c : _chunks) {
            total += c.capacity();
        }
        return total;
    }

    void clear() { _chunks.clear(); }

private:
    static constexpr size_t kChunkSize = 64 * 1024;
    std::vector<Buffer<uint8_t>> _chunks;
};

// GermanStringColumn stores strings as 16-byte `GermanString` values:
//   - inline when len <= 12;
//   - otherwise prefix + pointer into an internal arena owned by this column.
//
// Cross-column ingest (append/append_selective/update_rows/filter clone/...)
// MUST copy bytes of long strings into the destination column's arena so that
// `long_rep.ptr` remains valid for the lifetime of the destination column.
//
// The wire serialization format matches BinaryColumn (length-prefixed bytes)
// so serde is cross-compatible.
class GermanStringColumn final : public CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn> {
    friend class CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn>;

public:
    using ValueType = GermanString;
    using Container = Buffer<GermanString>;
    // `ImmContainer` is the read-only surface RunTimeTypeTraits exposes for batch
    // code (used e.g. by `ColumnViewer<TYPE_GERMAN_STRING>`). We expose a
    // `std::span<const GermanString>` — a lightweight view that aliases the
    // backing `Buffer<GermanString>`, so viewer construction does not copy data.
    using ImmContainer = std::span<const GermanString>;

    GermanStringColumn() = default;
    explicit GermanStringColumn(size_t size) : _data(size) {}

    GermanStringColumn(GermanStringColumn&& rhs) noexcept
            : _data(std::move(rhs._data)), _arena(std::move(rhs._arena)) {
        if (_arena == nullptr) {
            _arena = std::make_unique<GermanStringArena>();
        }
    }

    GermanStringColumn& operator=(GermanStringColumn&& rhs) noexcept {
        GermanStringColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    DISALLOW_COPY(GermanStringColumn);

    ~GermanStringColumn() override = default;

    // --- Identity -----------------------------------------------------------

    bool is_binary() const override { return true; }
    bool is_large_binary() const override { return false; }

    std::string get_name() const override { return "german_string"; }

    // --- Sizing / capacity --------------------------------------------------

    size_t size() const override { return _data.size(); }
    size_t capacity() const override { return _data.capacity(); }

    // Size of the row value in the container (one GermanString is 16 bytes).
    size_t type_size() const override { return sizeof(GermanString); }

    // Approximate memory footprint: container + long-rep byte arena.
    size_t byte_size() const override { return _data.size() * sizeof(GermanString) + _arena_bytes(); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        size_t total = size * sizeof(GermanString);
        for (size_t i = 0; i < size; ++i) {
            if (!_data[from + i].is_inline()) {
                total += _data[from + i].len;
            }
        }
        return total;
    }

    // serialize_size uses length-prefix wire format like BinaryColumn.
    size_t byte_size(size_t idx) const override { return sizeof(uint32_t) + _data[idx].len; }

    void reserve(size_t n) override { _data.reserve(n); }

    void resize(size_t n) override { _data.resize(n); }

    void assign(size_t n, size_t idx) override;

    void remove_first_n_values(size_t count) override;

    // --- Appends ------------------------------------------------------------

    // Accept a string-valued Datum (canonical storage is `Slice`). Long strings
    // are copied into this column's arena.
    void append_datum(const Datum& datum) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    // Slice-based appends. Long strings are materialized into the arena.
    DIAGNOSTIC_PUSH
    DIAGNOSTIC_IGNORE("-Woverloaded-virtual")
    void append(const Slice& str);
    void append(const GermanString& gs);
    DIAGNOSTIC_POP

    void append_string(const std::string& str);

    void append_bytes(const char* data, size_t len);

    bool append_strings(const Slice* data, size_t size) override;

    // GermanStringColumn cannot reuse BinaryColumn's fixed-stride overread trick
    // because each row is materialized into an owned 16-byte GermanString (with
    // long payloads copied into the arena). We simply forward to append_strings,
    // which already copies per-row bytes.
    bool append_strings_overflow(const Slice* data, size_t size, size_t /*max_length*/) override {
        return append_strings(data, size);
    }

    bool append_continuous_strings(const Slice* data, size_t size) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    // Append *value |count| times where value is a `Slice*` (default-value loading path).
    void append_value_multiple_times(const void* value, size_t count) override;

    // Empty string is represented by an inline GermanString with len == 0.
    void append_default() override { _data.emplace_back(); }
    void append_default(size_t count) override { _data.insert(_data.end(), count, GermanString()); }

    StatusOr<MutableColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    // --- Element access -----------------------------------------------------

    Slice get_slice(size_t idx) const {
        const auto& gs = _data[idx];
        return Slice(gs.get_data(), gs.len);
    }

    const GermanString& get_german_string(size_t idx) const { return _data[idx]; }

    Container& get_data() { return _data; }
    const Container& get_data() const { return _data; }

    // Read-only view used by ColumnViewer<TYPE_GERMAN_STRING>. Returns a
    // lightweight `std::span` over the underlying `Buffer<GermanString>` so
    // viewer construction does not copy the container.
    ImmContainer immutable_data() const { return ImmContainer(_data.data(), _data.size()); }

    Datum get(size_t n) const override;

    // --- Mutation helpers ---------------------------------------------------

    void swap_column(Column& rhs) override {
        auto& r = down_cast<GermanStringColumn&>(rhs);
        using std::swap;
        swap(this->_delete_state, r._delete_state);
        swap(_data, r._data);
        swap(_arena, r._arena);
    }

    void reset_column() override {
        Column::reset_column();
        _data.clear();
        if (_arena != nullptr) {
            _arena->clear();
        } else {
            _arena = std::make_unique<GermanStringArena>();
        }
    }

    // --- Filter / clone -----------------------------------------------------

    MutableColumnPtr clone_empty() const override { return GermanStringColumn::create(); }

    MutableColumnPtr clone() const override {
        auto p = GermanStringColumn::create();
        p->append(*this, 0, size());
        return p;
    }

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    // --- Comparison / equality ---------------------------------------------

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    // --- Serialization ------------------------------------------------------

    uint32_t max_one_element_serialize_size() const override;

    ALWAYS_INLINE uint32_t serialize(size_t idx, uint8_t* pos) const override {
        const auto& gs = _data[idx];
        const auto binary_size = static_cast<uint32_t>(gs.len);
        memcpy(pos, &binary_size, sizeof(uint32_t));
        if (binary_size > 0) {
            memcpy(pos + sizeof(uint32_t), gs.get_data(), binary_size);
        }
        return sizeof(uint32_t) + binary_size;
    }

    uint32_t serialize_default(uint8_t* pos) const override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, uint32_t max_row_size,
                                       size_t start, size_t count) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override {
        return static_cast<uint32_t>(sizeof(uint32_t) + _data[idx].len);
    }

    // --- Checksums ----------------------------------------------------------

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    // --- Mysql wire emission ------------------------------------------------

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    // --- Debug --------------------------------------------------------------

    std::string debug_item(size_t idx) const override;
    std::string raw_item_value(size_t idx) const override;

    std::string debug_string() const override {
        std::stringstream ss;
        size_t n = this->size();
        ss << "[";
        for (size_t i = 0; i + 1 < n; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (n > 0) {
            ss << debug_item(n - 1);
        }
        ss << "]";
        return ss.str();
    }

    // --- Upgrade / downgrade / limit ---------------------------------------

    StatusOr<MutableColumnPtr> upgrade_if_overflow() override { return nullptr; }
    StatusOr<MutableColumnPtr> downgrade() override { return nullptr; }
    bool has_large_column() const override { return false; }

    Status capacity_limit_reached() const override;

    size_t container_memory_usage() const override {
        return _data.capacity() * sizeof(GermanString) + _arena_bytes();
    }

    size_t reference_memory_usage(size_t /*from*/, size_t /*size*/) const override { return 0; }

    void check_or_die() const override;

    // --- Helpers ------------------------------------------------------------

    // Materialize a GermanString at |*dst| for bytes [data, data+len).
    // If |len| <= 12 the value is stored inline. Otherwise |len| bytes are
    // copied into |arena| and dst->long_rep.ptr is set accordingly.
    static void build_german_string(GermanString* dst, const char* data, size_t len, GermanStringArena* arena);

private:
    GermanStringArena* _get_arena() {
        if (_arena == nullptr) {
            _arena = std::make_unique<GermanStringArena>();
        }
        return _arena.get();
    }

    size_t _arena_bytes() const { return _arena == nullptr ? 0 : _arena->allocated_bytes(); }

    // Append bytes from an arbitrary pointer; materializes long strings into the arena.
    void _append_raw(const char* data, size_t len);

    // Append a row copied from another GermanStringColumn, duplicating long-rep bytes.
    void _append_from(const GermanString& gs);

    Container _data;
    // Arena that owns bytes backing long-rep entries in _data. Lazily created
    // on first long-string append.
    std::unique_ptr<GermanStringArena> _arena;
};

} // namespace starrocks
