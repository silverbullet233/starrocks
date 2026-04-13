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

#include <sstream>
#include <string>

#include "base/string/slice.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "runtime/mem_pool.h"
#include "types/datum.h"
#include "types/german_string.h"

namespace starrocks {

class GermanStringColumn;
template <typename T>
class BinaryColumnBase;

// Immutable view that yields GermanString per row from any byte-string column:
//   - GermanStringColumn (TYPE_STRING_V2 — native, zero overhead)
//   - BinaryColumn / LargeBinaryColumn (compute/storage boundary — constructs a
//     GermanString view per row; inline-copies for ≤12 bytes, long_rep.ptr =
//     slice.data for >12 bytes so the lifetime depends on the column staying
//     alive, which is the caller's responsibility).
//
// Used by `GetContainer<TYPE_STRING_V2>::get_data()` so a STRING_V2-typed
// runtime filter, bloom filter or aggregator can probe a BinaryColumn at the
// storage boundary without an explicit translation step.
class GermanStringImmContainer {
public:
    GermanStringImmContainer() = default;
    explicit GermanStringImmContainer(const GermanStringColumn& column);

    template <typename T>
    explicit GermanStringImmContainer(const BinaryColumnBase<T>& column) {
        _init_from_binary(column);
    }

    GermanString operator[](size_t index) const;
    size_t size() const;

private:
    template <typename T>
    void _init_from_binary(const BinaryColumnBase<T>& column);

    const Column* _column = nullptr;
    bool _is_german = true;
    bool _is_large = false;
};

class GermanStringColumn final
        : public CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn> {
    friend class CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn>;

public:
    using ValueType = GermanString;
    using Container = Buffer<GermanString>;
    using ImmContainer = GermanStringImmContainer;

    GermanStringColumn() = default;
    explicit GermanStringColumn(size_t size) : _german_strings(size) {}

    ~GermanStringColumn() override;

    // Prevent copy (arena is not trivially copyable).
    GermanStringColumn(const GermanStringColumn&) = delete;
    GermanStringColumn& operator=(const GermanStringColumn&) = delete;

    // Move
    GermanStringColumn(GermanStringColumn&& rhs) noexcept;
    GermanStringColumn& operator=(GermanStringColumn&& rhs) noexcept;

    bool is_german_string() const override { return true; }

    // ---- Size / capacity ----
    size_t size() const override { return _german_strings.size(); }
    size_t capacity() const override { return _german_strings.capacity(); }
    size_t type_size() const override { return sizeof(GermanString); } // 16

    // ---- Raw data access ----
    const uint8_t* raw_data() const override {
        return reinterpret_cast<const uint8_t*>(_german_strings.data());
    }

    // ---- GermanString-specific accessors ----
    const GermanString& get_german_string(size_t idx) const { return _german_strings[idx]; }
    Slice get_slice(size_t idx) const {
        const auto& gs = _german_strings[idx];
        return Slice(gs.get_data(), gs.len);
    }

    const Container& get_german_strings_container() const { return _german_strings; }

    ImmContainer immutable_data() const { return ImmContainer(*this); }

    // ---- Append (GermanString-specific) ----

    // No complain about the overloaded-virtual for these functions
    DIAGNOSTIC_PUSH
    DIAGNOSTIC_IGNORE("-Woverloaded-virtual")
    void append(const Slice& str);
    void append(const GermanString& gs);
    DIAGNOSTIC_POP

    // ---- Column virtual method overrides ----
    void append_datum(const Datum& datum) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    bool append_strings(const Slice* data, size_t size) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override;
    void append_default(size_t count) override;

    // ---- Reserve / Resize ----
    void reserve(size_t n) override { _german_strings.reserve(n); }
    void resize(size_t n) override { _german_strings.resize(n); }

    // ---- Assign / Remove ----
    void assign(size_t n, size_t idx) override;
    void remove_first_n_values(size_t count) override;

    // ---- Clone ----
    MutableColumnPtr clone_empty() const override { return GermanStringColumn::create(); }
    MutableColumnPtr clone() const override;

    // ---- Filter ----
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    // ---- Fill / Update ----
    void fill_default(const Filter& filter) override;
    void update_rows(const Column& src, const uint32_t* indexes) override;

    // ---- Compare ----
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    // ---- Serialize / Deserialize (stubs) ----
    uint32_t max_one_element_serialize_size() const override;
    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_default(uint8_t* pos) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;
    uint32_t serialize_size(size_t idx) const override;

    // ---- Byte size ----
    size_t byte_size() const override;
    size_t byte_size(size_t from, size_t size) const override;
    size_t byte_size(size_t idx) const override;

    // ---- Upgrade / Downgrade ----
    StatusOr<MutableColumnPtr> upgrade_if_overflow() override { return nullptr; }
    StatusOr<MutableColumnPtr> downgrade() override { return nullptr; }
    bool has_large_column() const override { return false; }

    // ---- Checksum ----
    int64_t xor_checksum(uint32_t from, uint32_t to) const override { return 0; }

    // ---- MySQL result ----
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    // ---- Name / Debug ----
    std::string get_name() const override { return "german-string"; }
    Datum get(size_t n) const override;
    std::string debug_item(size_t idx) const override;
    std::string debug_string() const override;

    // ---- Memory usage ----
    size_t container_memory_usage() const override;
    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    // ---- Swap / Reset ----
    void swap_column(Column& rhs) override;
    void reset_column() override;

    // ---- Build slices (for join hash map compatibility) ----
    // Populates a Slice buffer from the GermanString data, matching BinaryColumn::build_slices interface.
    void build_slices(Buffer<Slice>& slices) const;

    // ---- Capacity limit ----
    Status capacity_limit_reached() const override;

    // ---- Check ----
    void check_or_die() const override;

    // ---- Visitor (stub — integration in Task 2.7) ----
    // Note: accept/accept_mutable are provided by ColumnFactory<Column, GermanStringColumn>.
    // The default ColumnVisitor::visit(const GermanStringColumn&) returns NotSupported,
    // which is correct for the skeleton stage. No override needed here.

    // ---- Arena / Compaction ----
    size_t arena_memory_usage() const { return _arena.total_allocated_bytes(); }

    // Sum of long-string lengths actually referenced by live GermanStrings.
    size_t live_arena_bytes() const;

    // True when arena has >2x more allocated bytes than live data needs.
    bool needs_compaction() const;

    // Rebuild column with a fresh arena containing only live data.
    void compact();

    // Convert to a legacy BinaryColumn with identical data.
    ColumnPtr to_binary_column() const;

    // Create a GermanStringColumn from a BinaryColumn.
    static MutableColumnPtr from_binary_column(const BinaryColumn& bc);

private:
    Container _german_strings;
    MemPool _arena;

    // Helper: allocate in arena and construct a GermanString pointing to it.
    void _append_to_arena(const char* data, size_t len);
};

// ---- GermanStringImmContainer inline implementations ----
//
// Note: only `size()` is inline-defined here. `operator[]` and the
// `BinaryColumnBase<T>`-accepting ctor live in german_string_column.cpp so we
// can dispatch over BinaryColumn / LargeBinaryColumn without pulling
// binary_column.h into every translation unit that uses this header.

inline GermanStringImmContainer::GermanStringImmContainer(const GermanStringColumn& column)
        : _column(&column), _is_german(true), _is_large(false) {}

inline size_t GermanStringImmContainer::size() const {
    return _column == nullptr ? 0 : _column->size();
}

} // namespace starrocks
