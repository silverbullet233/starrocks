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

#include <stdexcept>
#include "column/bytes.h"
#include "column/column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "runtime/memory/allocator.h"
#include "util/slice.h"
#include "util/string_view.h"
#include "column/string_buffer.h"

namespace starrocks {

class StringColumn final : public ColumnFactory<Column, StringColumn> {
    friend class ColumnFactory<Column, StringColumn>;

public:
    using ValueType = StringView;

    using Container = Buffer<ValueType>;

    StringColumn() = default;
    // Default value is empty string
    explicit StringColumn(size_t size): _views(size, StringView()) {}

    StringColumn(Container string_views, StringBuffer string_buffer): _views(std::move(string_views)), _buffer(std::move(string_buffer)) {}
    

    StringColumn(const StringColumn& rhs): _views(rhs._views), _buffer(rhs._buffer) {}

    StringColumn(StringColumn&& rhs) noexcept
            : _views(std::move(rhs._views)), _buffer(std::move(rhs._buffer)) {}

    StringColumn& operator=(const StringColumn& rhs) {
        StringColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    StringColumn& operator=(StringColumn&& rhs) noexcept {
        StringColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override {
        return false;
    }

    ~StringColumn() override;

    bool is_binary() const override { return false; }
    bool is_large_binary() const override { return false; }


    const uint8_t* raw_data() const override {
        DCHECK(false) << "raw_data is not supported";
        throw std::runtime_error("raw_data is not supported");
    }

    uint8_t* mutable_raw_data() override {
        DCHECK(false) << "mutable_raw_data is not supported";
        throw std::runtime_error("mutable_raw_data is not supported");
    }

    size_t size() const override { return _views.size(); }

    // @TODO pending fix
    size_t capacity() const override { return size(); }

    size_t type_size() const override { return sizeof(StringView); }

    size_t byte_size() const override {
        return _views.size() * sizeof(StringView) + _buffer.allocated_bytes();
    }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        // @TODO dont'know how to do it
        return _views.size() * sizeof(StringView) + _buffer.allocated_bytes();
    }

    size_t byte_size(size_t idx) const override {
        return _views[idx].get_size();
    }

    // Slice get_slice(size_t idx) const {
    //     return Slice(_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    // }

    StringView get_view(size_t idx) const {
        return _views[idx];
        // return StringView(_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    }

    StringView get_slice(size_t idx) const {
        return _views[idx];
    }

    void check_or_die() const override;

    // For n value, the offsets size is n + 1
    // For example, for string "I","love","you"
    // the _bytes array is "Iloveyou"
    // the _offsets array is [0,1,5,8]
    void reserve(size_t n) override {
        _views.reserve(n);
    }

    // If you know the size of the Byte array in advance, you can call this method,
    // n means the number of strings, byte_size is the total length of the string
    void reserve(size_t n, size_t byte_size) {
        _views.reserve(n);
        // @TODO handle buffer
    }

    void resize(size_t n) override {
        _views.resize(n);
        // @TODO handle release memory in buffer
   }

    void assign(size_t n, size_t idx) override;

    void remove_first_n_values(size_t count) override;

    // No complain about the overloaded-virtual for this function
    DIAGNOSTIC_PUSH
    DIAGNOSTIC_IGNORE("-Woverloaded-virtual")
    void append(const Slice& str);
    // @TODO append sv
    void append(const StringView& sv);
    DIAGNOSTIC_POP


    void append_datum(const Datum& datum) override {
        append(datum.get_slice());
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    void append_string(const std::string& str);
    void append_string(const char* data, size_t size);

    bool append_strings(const Slice* data, size_t size) override;

    bool append_strings_overflow(const Slice* data, size_t size, size_t max_length) override;

    bool append_continuous_strings(const Slice* data, size_t size) override;

    bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override;

    void append_default(size_t count) override;

    // ColumnPtr replicate(const Buffer<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t max_one_element_serialize_size() const override;

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override {
        // max size of one string is 2^32, so use sizeof(uint32_t) not sizeof(T)
        return static_cast<uint32_t>(sizeof(uint32_t) + _views[idx].get_size());
    }

    MutableColumnPtr clone_empty() const override { return StringColumn::create_mutable(); }

    // ColumnPtr cut(size_t start, size_t length) const;
    size_t filter_range(const Filter& filter, size_t start, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string get_name() const override {
        return "string";
    }

    // @TODO do we need this?
    Container& get_data() {
        return _views;
    }
    
    const Container& get_data() const {
        return _views;
    }

    const Container& get_proxy_data() const {
        return _views;
    }

    const uint8_t* continuous_data() const override { 
        DCHECK(false) << "continuous_data() is not supported for StringColumn";
        throw std::runtime_error("continuous_data() is not supported for StringColumn");
    }

    Datum get(size_t n) const override { return Datum(get_view(n)); }

    size_t container_memory_usage() const override {
        return _views.capacity() * sizeof(StringView) + _buffer.allocated_bytes();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override {
        // @TODO
        auto& r = down_cast<StringColumn&>(rhs);
        using std::swap;
        swap(_views, r._views);
        swap(_buffer, r._buffer);
    }

    void reset_column() override {
        Column::reset_column();
        _views.clear();
        // @TODO clear buffer?
    }

    std::string debug_item(size_t idx) const override;

    std::string raw_item_value(size_t idx) const override;

    std::string debug_string() const override {
        std::stringstream ss;
        size_t size = this->size();
        ss << "[";
        for (size_t i = 0; i + 1 < size; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (size > 0) {
            ss << debug_item(size - 1);
        }
        ss << "]";
        return ss.str();
    }

    Status capacity_limit_reached() const override;

private:

    Container _views;
    StringBuffer _buffer;

};

} // namespace starrocks
