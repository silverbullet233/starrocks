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

#include <runtime/decimalv3.h>

#include "column/column.h"
#include "column/fixed_length_column_base.h"
#include "runtime/memory/allocator_v2.h"
#include "util/decimal_types.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

template <typename T>
class DecimalV3Column final
        : public CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                            DecimalV3Column<DecimalType<T>>, Column> {
    friend class CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                            DecimalV3Column<DecimalType<T>>, Column>;

public:
    using Base = CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                            DecimalV3Column<DecimalType<T>>, Column>;
    DecimalV3Column(memory::Allocator* allocator);
    explicit DecimalV3Column(memory::Allocator* allocator, size_t num_rows);
    DecimalV3Column(memory::Allocator* allocator, int precision, int scale);
    DecimalV3Column(memory::Allocator* allocator, int precision, int scale, size_t num_rows);

    bool is_decimal() const override;
    bool is_numeric() const override;
    void set_precision(int precision);
    void set_scale(int scale);
    int precision() const;
    int scale() const;

    MutableColumnPtr clone_empty(memory::Allocator* allocator = nullptr) const override {
        allocator = allocator == nullptr ? this->get_allocator() : allocator;
        return this->create(allocator, _precision, _scale);
    }
    MutableColumnPtr clone(memory::Allocator* allocator = nullptr) const override {
        allocator = allocator == nullptr ? this->get_allocator() : allocator;
        auto p = clone_empty(allocator);
        p->append(*this, 0, this->size());
        return p;
    }

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;
    std::string debug_item(size_t idx) const override;
    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

private:
    int _precision;
    int _scale;

    DISALLOW_COPY(DecimalV3Column);
};

} // namespace starrocks
