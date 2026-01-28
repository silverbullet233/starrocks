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

#include "column/fixed_length_column_base.h"
namespace starrocks {
template <typename T>
class FixedLengthColumn final : public CowFactory<ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>>,
                                                  FixedLengthColumn<T>, Column> {
    friend class CowFactory<ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>>, FixedLengthColumn<T>,
                            Column>;

public:
    using ValueType = T;
    using Container = Buffer<ValueType>;
    using SuperClass =
            CowFactory<ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>>, FixedLengthColumn<T>, Column>;
    explicit FixedLengthColumn(memory::Allocator* allocator) : SuperClass(allocator) {}

    FixedLengthColumn(memory::Allocator* allocator, const size_t n) : SuperClass(allocator, n) {}

    FixedLengthColumn(memory::Allocator* allocator, const size_t n, const ValueType x)
            : SuperClass(allocator, n, x) {}

    MutableColumnPtr clone_empty(memory::Allocator* allocator = nullptr) const override {
        allocator = allocator == nullptr ? this->get_allocator() : allocator;
        return this->create(allocator);
    }
    MutableColumnPtr clone(memory::Allocator* allocator = nullptr) const override {
        allocator = allocator == nullptr ? this->get_allocator() : allocator;
        auto dst = this->create(allocator);
        auto span = this->immutable_data();
        auto* dst_col = down_cast<FixedLengthColumn*>(dst.get());
        dst_col->get_data().assign(span.begin(), span.end());
        return dst;
    }

private:
    DISALLOW_COPY(FixedLengthColumn);
};
} // namespace starrocks
