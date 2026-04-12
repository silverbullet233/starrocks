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
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

class GermanStringColumn;

// Native STRING_V2 (GermanStringColumn) function implementations.
//
// Every function operates directly on GermanStringColumn data via
// GermanString::get_data()/len, avoiding any conversion to BinaryColumn.
// String-returning functions produce GermanStringColumn output.
class StringV2Functions {
public:
    // ---- Core string functions ----

    DEFINE_VECTORIZED_FN(length);
    DEFINE_VECTORIZED_FN(utf8_length);

    DEFINE_VECTORIZED_FN(substring);
    static Status sub_str_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status sub_str_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(left);
    DEFINE_VECTORIZED_FN(right);
    static Status left_or_right_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status left_or_right_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(concat);
    static Status concat_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status concat_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(concat_ws);

    DEFINE_VECTORIZED_FN(lower);
    static Status lower_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status lower_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(upper);
    static Status upper_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status upper_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(trim);
    DEFINE_VECTORIZED_FN(ltrim);
    DEFINE_VECTORIZED_FN(rtrim);
    static Status trim_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status trim_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(reverse);

    DEFINE_VECTORIZED_FN(ascii);

    DEFINE_VECTORIZED_FN(starts_with);
    DEFINE_VECTORIZED_FN(ends_with);
    DEFINE_VECTORIZED_FN(null_or_empty);

    DEFINE_VECTORIZED_FN(instr);
    DEFINE_VECTORIZED_FN(locate);
    DEFINE_VECTORIZED_FN(locate_pos);

    DEFINE_VECTORIZED_FN(lpad);
    DEFINE_VECTORIZED_FN(rpad);
    static Status pad_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status pad_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(repeat);
    DEFINE_VECTORIZED_FN(split_part);
    DEFINE_VECTORIZED_FN(replace);
    static Status replace_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status replace_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(find_in_set);
    DEFINE_VECTORIZED_FN(strcmp);

    DEFINE_VECTORIZED_FN(regexp_extract);
    DEFINE_VECTORIZED_FN(regexp_replace);
    static Status regexp_extract_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status regexp_replace_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status regexp_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // ---- Helpers ----

    // Unwrap Const/Nullable wrappers to get the underlying GermanStringColumn.
    static const GermanStringColumn* get_gs_column(const ColumnPtr& col);

    // Get the Slice for row |idx| from a column that may be Const/Nullable/bare GermanStringColumn.
    // Also returns whether the row is null via |is_null|.
    static Slice get_gs_slice(const ColumnPtr& col, size_t idx, bool* is_null);

    // Check if a given row in a column is null.
    static bool is_row_null(const ColumnPtr& col, size_t idx);
};

} // namespace starrocks
