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

#include "column/column_viewer.h"

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "types/logical_type_infra.h"
#include "types/percentile_value.h"

namespace starrocks {

namespace {

const NullColumnPtr& one_size_not_null_column() {
    static NullColumnPtr one_size_not_null_column = NullColumn::create(1, 0);
    return one_size_not_null_column;
}

const NullColumnPtr& one_size_null_column() {
    static NullColumnPtr one_size_null_column = NullColumn::create(1, 1);
    return one_size_null_column;
}

} // namespace

static inline size_t not_const_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() ? -1 : 0;
}

static inline size_t null_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() && column->is_nullable() ? -1 : 0;
}

// Whether `ColumnViewer<Type>` should route data reads through
// `GetContainer<Type>` instead of a strict `cast_to<Type>` + `immutable_data()`.
//
// With `enable_german_string=true`, a VARCHAR-typed consumer may receive a
// GermanStringColumn (FE TypeSerializer cascades) and a STRING_V2-typed
// consumer may receive a BinaryColumn (literals still materialize as
// BinaryColumn, see literal.cpp). Both directions crash under a strict
// down_cast. `GetContainer<T>` already implements the symmetric boundary
// wrapping — StringSliceView for TYPE_VARCHAR, GermanStringImmContainer
// for TYPE_STRING_V2 — so routing through it makes the viewer tolerate
// either backing column.
template <LogicalType Type>
constexpr bool kColumnViewerNeedsBoundaryWrap =
        (Type == TYPE_VARCHAR || Type == TYPE_CHAR || Type == TYPE_STRING_V2);

template <LogicalType Type>
ColumnViewer<Type>::ColumnViewer(const ColumnPtr& column)
        : _not_const_mask(not_const_mask(column)), _null_mask(null_mask(column)) {
    if (column->only_null()) {
        _null_column = one_size_null_column();
        auto col = RunTimeColumnType<Type>::create();
        col->append_default();
        _column = std::move(col);
    } else if (column->is_constant()) {
        auto v = ColumnHelper::as_raw_column<ConstColumn>(column);
        if constexpr (kColumnViewerNeedsBoundaryWrap<Type>) {
            // Skip the strict cast; `_data` below reads through
            // GetContainer<Type> which handles the cross-column case.
            _column = RunTimeColumnType<Type>::create();
        } else {
            _column = ColumnHelper::cast_to<Type>(v->data_column());
        }
        _null_column = one_size_not_null_column();
    } else if (column->is_nullable()) {
        auto v = ColumnHelper::as_raw_column<NullableColumn>(column);
        if constexpr (kColumnViewerNeedsBoundaryWrap<Type>) {
            _column = RunTimeColumnType<Type>::create();
        } else {
            _column = ColumnHelper::cast_to<Type>(v->data_column());
        }
        _null_column = ColumnHelper::as_column<NullColumn>(v->null_column());
    } else {
        if constexpr (kColumnViewerNeedsBoundaryWrap<Type>) {
            _column = RunTimeColumnType<Type>::create();
        } else {
            _column = ColumnHelper::cast_to<Type>(column);
        }
        _null_column = one_size_not_null_column();
    }

    if constexpr (kColumnViewerNeedsBoundaryWrap<Type>) {
        // GetContainer<Type> returns StringSliceView or GermanStringImmContainer
        // depending on Type, and transparently adapts both BinaryColumn and
        // GermanStringColumn underlying inputs. For only_null columns there's
        // no byte-string data column underneath, so fall back to the
        // synthesized empty `_column`.
        if (column->only_null()) {
            _data = _column->immutable_data();
        } else {
            _data = GetContainer<Type>::get_data(column);
        }
    } else {
        _data = _column->immutable_data();
    }
    _null_data = _null_column->get_data().data();
}

#define M(TYPE) template class ColumnViewer<TYPE>;

APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(M);
M(TYPE_STRING_V2);
#undef M

template class ColumnViewer<TYPE_HLL>;
template class ColumnViewer<TYPE_OBJECT>;
template class ColumnViewer<TYPE_PERCENTILE>;

} // namespace starrocks
