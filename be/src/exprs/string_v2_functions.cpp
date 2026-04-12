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

#include "exprs/string_v2_functions.h"

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/german_string_column.h"
#include "column/nullable_column.h"
#include "exprs/string_functions.h"

namespace starrocks {

// ============================================================================
// Conversion helpers
// ============================================================================

// Convert a single column: if the underlying data column is GermanStringColumn,
// convert it to BinaryColumn while preserving Nullable/Const wrappers.
static ColumnPtr convert_one_column(const ColumnPtr& col) {
    // Case 1: ConstColumn wrapping something
    if (col->is_constant()) {
        const auto* const_col = down_cast<const ConstColumn*>(col.get());
        auto inner = const_col->data_column();
        auto converted = convert_one_column(inner);
        if (converted.get() != inner.get()) {
            return ConstColumn::create(std::move(converted), col->size());
        }
        return col;
    }
    // Case 2: NullableColumn wrapping GermanStringColumn
    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        const auto& data_col = nullable->data_column();
        if (data_col->is_german_string()) {
            const auto* gs = down_cast<const GermanStringColumn*>(data_col.get());
            auto binary_col = gs->to_binary_column();
            return NullableColumn::create(std::move(binary_col), nullable->null_column());
        }
        return col;
    }
    // Case 3: bare GermanStringColumn
    if (col->is_german_string()) {
        const auto* gs = down_cast<const GermanStringColumn*>(col.get());
        return gs->to_binary_column();
    }
    // Not a GermanStringColumn — pass through
    return col;
}

Columns StringV2Functions::convert_inputs(const Columns& columns) {
    Columns result;
    result.reserve(columns.size());
    for (const auto& col : columns) {
        result.emplace_back(convert_one_column(col));
    }
    return result;
}

// Convert BinaryColumn result to GermanStringColumn.
// Handles Const/Nullable wrappers.
static ColumnPtr convert_binary_to_german(const ColumnPtr& col) {
    // ConstColumn
    if (col->is_constant()) {
        const auto* const_col = down_cast<const ConstColumn*>(col.get());
        auto inner = const_col->data_column();
        auto converted = convert_binary_to_german(inner);
        if (converted.get() != inner.get()) {
            return ConstColumn::create(std::move(converted), col->size());
        }
        return col;
    }
    // NullableColumn
    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        const auto& data_col = nullable->data_column();
        if (data_col->is_binary()) {
            const auto* bc = down_cast<const BinaryColumn*>(data_col.get());
            auto gs_col = GermanStringColumn::create();
            gs_col->reserve(bc->size());
            for (size_t i = 0; i < bc->size(); ++i) {
                gs_col->append(bc->get_slice(i));
            }
            return NullableColumn::create(std::move(gs_col), nullable->null_column());
        }
        return col;
    }
    // Bare BinaryColumn
    if (col->is_binary()) {
        const auto* bc = down_cast<const BinaryColumn*>(col.get());
        auto gs_col = GermanStringColumn::create();
        gs_col->reserve(bc->size());
        for (size_t i = 0; i < bc->size(); ++i) {
            gs_col->append(bc->get_slice(i));
        }
        return gs_col;
    }
    return col;
}

ColumnPtr StringV2Functions::maybe_convert_result(const ColumnPtr& result) {
    return convert_binary_to_german(result);
}

// ============================================================================
// Macro: generate a bridge function that converts inputs, calls the VARCHAR
// implementation, and converts the result back to GermanStringColumn.
// ============================================================================

// For functions whose result is a string (VARCHAR) — convert result back.
#define BRIDGE_STRING_FN(FN_NAME, DELEGATE)                                                  \
    StatusOr<ColumnPtr> StringV2Functions::FN_NAME(FunctionContext* context,                  \
                                                   const Columns& columns) {                 \
        Columns converted = convert_inputs(columns);                                         \
        ASSIGN_OR_RETURN(auto result, DELEGATE(context, converted));                         \
        return maybe_convert_result(result);                                                 \
    }

// For functions whose result is NOT a string (e.g. INT, BOOLEAN) — no result conversion.
#define BRIDGE_NONSTRING_FN(FN_NAME, DELEGATE)                                               \
    StatusOr<ColumnPtr> StringV2Functions::FN_NAME(FunctionContext* context,                  \
                                                   const Columns& columns) {                 \
        Columns converted = convert_inputs(columns);                                         \
        return DELEGATE(context, converted);                                                 \
    }

// For prepare/close functions — delegate directly.
#define BRIDGE_PREPARE(FN_NAME, DELEGATE)                                                    \
    Status StringV2Functions::FN_NAME(FunctionContext* context,                               \
                                     FunctionContext::FunctionStateScope scope) {             \
        return DELEGATE(context, scope);                                                     \
    }

// ============================================================================
// Function implementations — core MVP set
// ============================================================================

// length(STRING_V2) -> INT
BRIDGE_NONSTRING_FN(length, StringFunctions::length)

// char_length / character_length (utf8)
BRIDGE_NONSTRING_FN(utf8_length, StringFunctions::utf8_length)

// substr / substring
BRIDGE_STRING_FN(substring, StringFunctions::substring)
BRIDGE_PREPARE(sub_str_prepare, StringFunctions::sub_str_prepare)
BRIDGE_PREPARE(sub_str_close, StringFunctions::sub_str_close)

// left / right
BRIDGE_STRING_FN(left, StringFunctions::left)
BRIDGE_STRING_FN(right, StringFunctions::right)
BRIDGE_PREPARE(left_or_right_prepare, StringFunctions::left_or_right_prepare)
BRIDGE_PREPARE(left_or_right_close, StringFunctions::left_or_right_close)

// concat
BRIDGE_STRING_FN(concat, StringFunctions::concat)
BRIDGE_PREPARE(concat_prepare, StringFunctions::concat_prepare)
BRIDGE_PREPARE(concat_close, StringFunctions::concat_close)

// concat_ws
BRIDGE_STRING_FN(concat_ws, StringFunctions::concat_ws)

// lower / lcase
BRIDGE_STRING_FN(lower, StringFunctions::lower)
BRIDGE_PREPARE(lower_prepare, StringFunctions::lower_prepare)
BRIDGE_PREPARE(lower_close, StringFunctions::lower_close)

// upper / ucase
BRIDGE_STRING_FN(upper, StringFunctions::upper)
BRIDGE_PREPARE(upper_prepare, StringFunctions::upper_prepare)
BRIDGE_PREPARE(upper_close, StringFunctions::upper_close)

// trim / ltrim / rtrim
BRIDGE_STRING_FN(trim, StringFunctions::trim)
BRIDGE_STRING_FN(ltrim, StringFunctions::ltrim)
BRIDGE_STRING_FN(rtrim, StringFunctions::rtrim)
BRIDGE_PREPARE(trim_prepare, StringFunctions::trim_prepare)
BRIDGE_PREPARE(trim_close, StringFunctions::trim_close)

// reverse
BRIDGE_STRING_FN(reverse, StringFunctions::reverse)

// ascii -> INT
BRIDGE_NONSTRING_FN(ascii, StringFunctions::ascii)

// starts_with / ends_with -> BOOLEAN
BRIDGE_NONSTRING_FN(starts_with, StringFunctions::starts_with)
BRIDGE_NONSTRING_FN(ends_with, StringFunctions::ends_with)

// null_or_empty -> BOOLEAN
BRIDGE_NONSTRING_FN(null_or_empty, StringFunctions::null_or_empty)

// instr -> INT
BRIDGE_NONSTRING_FN(instr, StringFunctions::instr)

// locate -> INT
BRIDGE_NONSTRING_FN(locate, StringFunctions::locate)
BRIDGE_NONSTRING_FN(locate_pos, StringFunctions::locate_pos)

// lpad / rpad
BRIDGE_STRING_FN(lpad, StringFunctions::lpad)
BRIDGE_STRING_FN(rpad, StringFunctions::rpad)
BRIDGE_PREPARE(pad_prepare, StringFunctions::pad_prepare)
BRIDGE_PREPARE(pad_close, StringFunctions::pad_close)

// repeat
BRIDGE_STRING_FN(repeat, StringFunctions::repeat)

// split_part
BRIDGE_STRING_FN(split_part, StringFunctions::split_part)

// replace
BRIDGE_STRING_FN(replace, StringFunctions::replace)
BRIDGE_PREPARE(replace_prepare, StringFunctions::replace_prepare)
BRIDGE_PREPARE(replace_close, StringFunctions::replace_close)

// find_in_set -> INT
BRIDGE_NONSTRING_FN(find_in_set, StringFunctions::find_in_set)

// strcmp -> INT
BRIDGE_NONSTRING_FN(strcmp, StringFunctions::strcmp)

// regexp_extract -> STRING_V2
BRIDGE_STRING_FN(regexp_extract, StringFunctions::regexp_extract)
BRIDGE_PREPARE(regexp_extract_prepare, StringFunctions::regexp_extract_prepare)

// regexp_replace -> STRING_V2
BRIDGE_STRING_FN(regexp_replace, StringFunctions::regexp_replace)
BRIDGE_PREPARE(regexp_replace_prepare, StringFunctions::regexp_replace_prepare)

BRIDGE_PREPARE(regexp_close, StringFunctions::regexp_close)

} // namespace starrocks

#include "gen_cpp/opcode/StringV2Functions.inc"
