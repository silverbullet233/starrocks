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

#include "column/column.h"
#include "column/nullable_column.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/json_column.h"
#include "gutil/casts.h"

namespace starrocks {

// Helper macros to simplify the usage of as_mutable_raw_ptr()
#define AS_MUTABLE_RAW_PTR(col) (col)->as_mutable_raw_ptr()

// Helper functions to safely access column internals
inline Column* get_mutable_data_column(Column* col) {
    if (col->is_nullable()) {
        auto* nullable_col = down_cast<NullableColumn*>(col);
        return AS_MUTABLE_RAW_PTR(nullable_col->data_column().get());
    }
    return AS_MUTABLE_RAW_PTR(col);
}

inline const Column* get_data_column(const Column* col) {
    if (col->is_nullable()) {
        const auto* nullable_col = down_cast<const NullableColumn*>(col);
        return nullable_col->data_column().get();
    }
    return col;
}

inline NullColumn* get_mutable_null_column(NullableColumn* col) {
    return down_cast<NullColumn*>(AS_MUTABLE_RAW_PTR(col->null_column().get()));
}

inline const NullColumn* get_null_column(const NullableColumn* col) {
    return down_cast<const NullColumn*>(col->null_column().get());
}

// For ArrayColumn
inline Column* get_mutable_elements_column(ArrayColumn* col) {
    return AS_MUTABLE_RAW_PTR(col->elements_column().get());
}

inline UInt32Column* get_mutable_offsets_column(ArrayColumn* col) {
    return down_cast<UInt32Column*>(AS_MUTABLE_RAW_PTR(col->offsets_column().get()));
}

// For MapColumn
inline Column* get_mutable_keys_column(MapColumn* col) {
    return AS_MUTABLE_RAW_PTR(col->keys_column().get());
}

inline Column* get_mutable_values_column(MapColumn* col) {
    return AS_MUTABLE_RAW_PTR(col->values_column().get());
}

inline UInt32Column* get_mutable_map_offsets_column(MapColumn* col) {
    return down_cast<UInt32Column*>(AS_MUTABLE_RAW_PTR(col->offsets_column().get()));
}

// For StructColumn
inline Column* get_mutable_field_column(StructColumn* col, size_t idx) {
    return AS_MUTABLE_RAW_PTR(col->fields_column()[idx].get());
}

} // namespace starrocks