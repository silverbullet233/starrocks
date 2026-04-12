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

#include <re2/re2.h>

#include <algorithm>
#include <cstring>

#include "base/string/utf8.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/german_string_column.h"
#include "column/nullable_column.h"
#include "exprs/string_functions.h"
#include "storage/olap_define.h"

namespace starrocks {

// ============================================================================
// Helper functions
// ============================================================================

// No conversion helpers needed — columns are natively GermanStringColumn
// when TYPE_STRING_V2 flows through the dispatch layer.

const GermanStringColumn* StringV2Functions::get_gs_column(const ColumnPtr& col) {
    const Column* c = col.get();
    if (c->is_constant()) {
        c = down_cast<const ConstColumn*>(c)->data_column().get();
    }
    if (c->is_nullable()) {
        c = down_cast<const NullableColumn*>(c)->data_column().get();
    }
    return down_cast<const GermanStringColumn*>(c);
}

Slice StringV2Functions::get_gs_slice(const ColumnPtr& col, size_t idx, bool* is_null) {
    const Column* c = col.get();
    size_t real_idx = idx;

    if (c->is_constant()) {
        const auto* const_col = down_cast<const ConstColumn*>(c);
        c = const_col->data_column().get();
        real_idx = 0;
    }
    if (c->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(c);
        if (nullable->is_null(real_idx)) {
            *is_null = true;
            return Slice();
        }
        c = nullable->data_column().get();
    }
    *is_null = false;
    // Column is natively GermanStringColumn now.
    DCHECK(c->is_german_string());
    const auto* gs_col = down_cast<const GermanStringColumn*>(c);
    const auto& gs = gs_col->get_german_string(real_idx);
    return Slice(gs.get_data(), gs.len);
}

bool StringV2Functions::is_row_null(const ColumnPtr& col, size_t idx) {
    const Column* c = col.get();
    if (c->only_null()) return true;
    if (c->is_constant()) {
        c = down_cast<const ConstColumn*>(c)->data_column().get();
        idx = 0;
    }
    if (c->is_nullable()) {
        return down_cast<const NullableColumn*>(c)->is_null(idx);
    }
    return false;
}

// Get Slice for row idx, handling const columns (for non-GermanStringColumn args like INT).
static int32_t get_int_value(const ColumnPtr& col, size_t idx) {
    const Column* c = col.get();
    if (c->is_constant()) {
        c = down_cast<const ConstColumn*>(c)->data_column().get();
        idx = 0;
    }
    if (c->is_nullable()) {
        c = down_cast<const NullableColumn*>(c)->data_column().get();
    }
    return down_cast<const Int32Column*>(c)->get_data()[idx];
}

static int64_t get_int64_value(const ColumnPtr& col, size_t idx) {
    const Column* c = col.get();
    if (c->is_constant()) {
        c = down_cast<const ConstColumn*>(c)->data_column().get();
        idx = 0;
    }
    if (c->is_nullable()) {
        c = down_cast<const NullableColumn*>(c)->data_column().get();
    }
    return down_cast<const Int64Column*>(c)->get_data()[idx];
}

// Utility: index_of (same as StringFunctions::index_of)
static int index_of(const char* source, int source_count, const char* target, int target_count, int from_index) {
    if (from_index >= source_count) {
        return (target_count == 0 ? source_count : -1);
    }
    if (from_index < 0) {
        from_index = 0;
    }
    if (target_count == 0) {
        return from_index;
    }

    const char first = *target;
    int max_idx = source_count - target_count;
    for (int i = from_index; i <= max_idx; i++) {
        while (i <= max_idx && source[i] != first) {
            i++;
        }
        if (i <= max_idx) {
            int j = i + 1;
            int end = j + target_count - 1;
            for (int k = 1; j < end && source[j] == target[k]; j++, k++) {
            }
            if (j == end) {
                return i;
            }
        }
    }
    return -1;
}

// ============================================================================
// Prepare / Close delegates
// ============================================================================

// For prepare/close functions that only set up FunctionContext state (no column data),
// we delegate to the existing StringFunctions implementations.
#define BRIDGE_PREPARE(FN_NAME, DELEGATE)                                            \
    Status StringV2Functions::FN_NAME(FunctionContext* context,                       \
                                     FunctionContext::FunctionStateScope scope) {     \
        return DELEGATE(context, scope);                                             \
    }

BRIDGE_PREPARE(sub_str_prepare, StringFunctions::sub_str_prepare)
BRIDGE_PREPARE(sub_str_close, StringFunctions::sub_str_close)
BRIDGE_PREPARE(left_or_right_prepare, StringFunctions::left_or_right_prepare)
BRIDGE_PREPARE(left_or_right_close, StringFunctions::left_or_right_close)
BRIDGE_PREPARE(concat_prepare, StringFunctions::concat_prepare)
BRIDGE_PREPARE(concat_close, StringFunctions::concat_close)
BRIDGE_PREPARE(lower_prepare, StringFunctions::lower_prepare)
BRIDGE_PREPARE(lower_close, StringFunctions::lower_close)
BRIDGE_PREPARE(upper_prepare, StringFunctions::upper_prepare)
BRIDGE_PREPARE(upper_close, StringFunctions::upper_close)
BRIDGE_PREPARE(trim_prepare, StringFunctions::trim_prepare)
BRIDGE_PREPARE(trim_close, StringFunctions::trim_close)
BRIDGE_PREPARE(pad_prepare, StringFunctions::pad_prepare)
BRIDGE_PREPARE(pad_close, StringFunctions::pad_close)
BRIDGE_PREPARE(replace_prepare, StringFunctions::replace_prepare)
BRIDGE_PREPARE(replace_close, StringFunctions::replace_close)
BRIDGE_PREPARE(regexp_extract_prepare, StringFunctions::regexp_extract_prepare)
BRIDGE_PREPARE(regexp_replace_prepare, StringFunctions::regexp_replace_prepare)
BRIDGE_PREPARE(regexp_close, StringFunctions::regexp_close)

#undef BRIDGE_PREPARE

// ============================================================================
// length(STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::length(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    for (size_t i = 0; i < actual_rows; i++) {
        data[i] = static_cast<int32_t>(gs_col->get_german_string(i).len);
    }

    if (is_const) {
        return ConstColumn::create(std::move(result), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result), nullable->null_column());
    }

    return result;
}

// ============================================================================
// utf8_length(STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::utf8_length(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        const char* str_data = gs.get_data();
        data[i] = utf8_len(str_data, str_data + gs.len);
    }

    if (is_const) {
        return ConstColumn::create(std::move(result), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result), nullable->null_column());
    }

    return result;
}

// ============================================================================
// substring(STRING_V2, INT [, INT]) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::substring(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto gs_input = columns[0];
    const auto num_rows = gs_input->size();
    const auto* gs_col = get_gs_column(gs_input);
    bool has_len_arg = columns.size() >= 3;

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    bool col0_const = gs_input->is_constant();

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(gs_input, i) || is_row_null(columns[1], i) ||
            (has_len_arg && is_row_null(columns[2], i))) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        size_t gs_idx = col0_const ? 0 : i;
        const auto& gs = gs_col->get_german_string(gs_idx);
        const char* str_data = gs.get_data();
        int32_t str_len = static_cast<int32_t>(gs.len);

        int32_t pos = get_int_value(columns[1], i);
        int32_t len = has_len_arg ? get_int_value(columns[2], i) : str_len;

        if (len <= 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        // Check if string is ASCII for fast path
        bool is_ascii = validate_ascii_fast(str_data, str_len);

        if (is_ascii) {
            // ASCII path: 1 byte = 1 char
            if (pos > 0) {
                pos = pos - 1; // 1-based to 0-based
            } else if (pos < 0) {
                pos = str_len + pos;
            } else {
                // pos == 0 is treated as pos == 1 in StarRocks
                pos = 0;
            }

            if (pos < 0 || pos >= str_len) {
                result_col->append(Slice("", 0));
                continue;
            }

            int32_t actual_len = std::min(len, str_len - pos);
            result_col->append(Slice(str_data + pos, actual_len));
        } else {
            // UTF-8 path
            int32_t char_len = utf8_len(str_data, str_data + str_len);

            if (pos > 0) {
                pos = pos - 1;
            } else if (pos < 0) {
                pos = char_len + pos;
            }

            if (pos < 0 || pos >= char_len) {
                result_col->append(Slice("", 0));
                continue;
            }

            // Find byte offset for pos
            const char* p = str_data;
            for (int32_t j = 0; j < pos && p < str_data + str_len; j++) {
                p += UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(*p)];
            }

            // Find byte length for len chars
            const char* end = p;
            for (int32_t j = 0; j < len && end < str_data + str_len; j++) {
                end += UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(*end)];
            }

            result_col->append(Slice(p, end - p));
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// left(STRING_V2, INT) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::left(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    Columns values;
    values.emplace_back(columns[0]);
    values.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, columns[0]->size()));
    values.emplace_back(columns[1]);
    return substring(context, values);
}

// ============================================================================
// right(STRING_V2, INT) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::right(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto gs_input = columns[0];
    const auto num_rows = gs_input->size();
    const auto* gs_col = get_gs_column(gs_input);

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    bool col0_const = gs_input->is_constant();

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(gs_input, i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        size_t gs_idx = col0_const ? 0 : i;
        const auto& gs = gs_col->get_german_string(gs_idx);
        const char* str_data = gs.get_data();
        int32_t str_len = static_cast<int32_t>(gs.len);

        int32_t len = get_int_value(columns[1], i);

        if (len <= 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        if (len >= str_len) {
            result_col->append(Slice(str_data, str_len));
            continue;
        }

        bool is_ascii = validate_ascii_fast(str_data, str_len);

        if (is_ascii) {
            int32_t start = str_len - len;
            result_col->append(Slice(str_data + start, len));
        } else {
            int32_t char_len = utf8_len(str_data, str_data + str_len);
            if (len >= char_len) {
                result_col->append(Slice(str_data, str_len));
                continue;
            }
            int32_t skip = char_len - len;
            const char* p = str_data;
            for (int32_t j = 0; j < skip && p < str_data + str_len; j++) {
                p += UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(*p)];
            }
            result_col->append(Slice(p, str_data + str_len - p));
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// concat(STRING_V2, ...) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::concat(FunctionContext* context, const Columns& columns) {
    if (columns.size() == 1) {
        return std::move(*columns[0]).mutate();
    }
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    std::string tmp;
    for (size_t i = 0; i < num_rows; i++) {
        tmp.clear();
        bool row_null = false;
        for (size_t c = 0; c < columns.size(); c++) {
            if (is_row_null(columns[c], i)) {
                row_null = true;
                break;
            }
            bool dummy;
            Slice s = get_gs_slice(columns[c], i, &dummy);
            tmp.append(s.data, s.size);
        }
        if (row_null) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
        } else {
            if (tmp.size() > get_olap_string_max_length()) {
                has_null = true;
                null_col->get_data()[i] = 1;
                result_col->append_default();
            } else {
                result_col->append(Slice(tmp));
            }
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                      : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                                  : ColumnPtr(std::move(result_col));
}

// ============================================================================
// concat_ws(STRING_V2, STRING_V2, ...) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::concat_ws(FunctionContext* context, const Columns& columns) {
    const auto column_num = columns.size();
    if (column_num <= 1 || columns[0]->only_null()) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }
    if (columns.size() == 2) {
        return std::move(*columns[1]).mutate();
    }

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    std::string tmp;
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        bool dummy;
        Slice sep = get_gs_slice(columns[0], i, &dummy);

        tmp.clear();
        bool first = true;
        bool oversize = false;
        for (size_t c = 1; c < columns.size(); c++) {
            if (is_row_null(columns[c], i)) {
                continue;
            }
            Slice s = get_gs_slice(columns[c], i, &dummy);
            if (!first) {
                if (tmp.size() + sep.size + s.size > get_olap_string_max_length()) {
                    oversize = true;
                    break;
                }
                tmp.append(sep.data, sep.size);
            }
            if (tmp.size() + s.size > get_olap_string_max_length()) {
                oversize = true;
                break;
            }
            tmp.append(s.data, s.size);
            first = false;
        }
        if (oversize) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
        } else {
            result_col->append(Slice(tmp));
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// lower(STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::lower(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    std::string tmp;
    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        tmp.assign(gs.get_data(), gs.len);
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), [](unsigned char c) { return std::tolower(c); });
        result_col->append(Slice(tmp));
    }

    if (is_const) {
        return ConstColumn::create(std::move(result_col), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result_col), nullable->null_column());
    }

    return result_col;
}

// ============================================================================
// upper(STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::upper(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    std::string tmp;
    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        tmp.assign(gs.get_data(), gs.len);
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), [](unsigned char c) { return std::toupper(c); });
        result_col->append(Slice(tmp));
    }

    if (is_const) {
        return ConstColumn::create(std::move(result_col), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result_col), nullable->null_column());
    }

    return result_col;
}

// ============================================================================
// trim / ltrim / rtrim
// ============================================================================

enum StringV2TrimType { SV2_TRIM_BOTH, SV2_TRIM_LEFT, SV2_TRIM_RIGHT };

// Compatible with TrimState defined in string_functions.cpp.
// trim_prepare stores a TrimState* in FRAGMENT_LOCAL state.
struct StringV2TrimState {
    std::string remove_chars;
    bool is_utf8;
    std::vector<size_t> utf8_index;
};

template <StringV2TrimType trim_type>
static StatusOr<ColumnPtr> trim_impl_v2(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = StringV2Functions::get_gs_column(col);

    // Get the characters to trim (from prepare state)
    auto* state = reinterpret_cast<StringV2TrimState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::string remove_chars = " ";
    if (state != nullptr) {
        remove_chars = state->remove_chars;
    }

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        const char* data = gs.get_data();
        int32_t len = static_cast<int32_t>(gs.len);

        int32_t start = 0;
        int32_t end = len;

        if constexpr (trim_type == SV2_TRIM_BOTH || trim_type == SV2_TRIM_LEFT) {
            while (start < end && remove_chars.find(data[start]) != std::string::npos) {
                start++;
            }
        }
        if constexpr (trim_type == SV2_TRIM_BOTH || trim_type == SV2_TRIM_RIGHT) {
            while (end > start && remove_chars.find(data[end - 1]) != std::string::npos) {
                end--;
            }
        }
        result_col->append(Slice(data + start, end - start));
    }

    if (is_const) {
        return ConstColumn::create(std::move(result_col), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result_col), nullable->null_column());
    }

    return result_col;
}

StatusOr<ColumnPtr> StringV2Functions::trim(FunctionContext* context, const Columns& columns) {
    return trim_impl_v2<SV2_TRIM_BOTH>(context, columns);
}

StatusOr<ColumnPtr> StringV2Functions::ltrim(FunctionContext* context, const Columns& columns) {
    return trim_impl_v2<SV2_TRIM_LEFT>(context, columns);
}

StatusOr<ColumnPtr> StringV2Functions::rtrim(FunctionContext* context, const Columns& columns) {
    return trim_impl_v2<SV2_TRIM_RIGHT>(context, columns);
}

// ============================================================================
// reverse(STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::reverse(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    std::string tmp;
    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        const char* data = gs.get_data();
        uint32_t len = gs.len;

        bool is_ascii = validate_ascii_fast(data, len);

        if (is_ascii) {
            tmp.assign(data, len);
            std::reverse(tmp.begin(), tmp.end());
            result_col->append(Slice(tmp));
        } else {
            // UTF-8 aware reverse
            tmp.clear();
            const char* p = data + len;
            while (p > data) {
                const char* prev = p - 1;
                while (prev > data && (static_cast<unsigned char>(*prev) & 0xC0) == 0x80) {
                    prev--;
                }
                tmp.append(prev, p - prev);
                p = prev;
            }
            result_col->append(Slice(tmp));
        }
    }

    if (is_const) {
        return ConstColumn::create(std::move(result_col), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result_col), nullable->null_column());
    }

    return result_col;
}

// ============================================================================
// ascii(STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::ascii(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto col = columns[0];
    const auto num_rows = col->size();
    const auto* gs_col = get_gs_column(col);

    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    bool is_const = col->is_constant();
    size_t actual_rows = is_const ? 1 : num_rows;

    for (size_t i = 0; i < actual_rows; i++) {
        const auto& gs = gs_col->get_german_string(i);
        if (gs.len == 0) {
            data[i] = 0;
        } else {
            data[i] = static_cast<uint8_t>(gs.get_data()[0]);
        }
    }

    if (is_const) {
        return ConstColumn::create(std::move(result), num_rows);
    }

    if (col->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result), nullable->null_column());
    }

    return result;
}

// ============================================================================
// starts_with(STRING_V2, STRING_V2) -> BOOLEAN
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::starts_with(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();

    auto result = BooleanColumn::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        Slice prefix = get_gs_slice(columns[1], i, &dummy);

        if (prefix.size > str.size) {
            data[i] = 0;
        } else {
            data[i] = (memcmp(str.data, prefix.data, prefix.size) == 0) ? 1 : 0;
        }
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// ends_with(STRING_V2, STRING_V2) -> BOOLEAN
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::ends_with(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();

    auto result = BooleanColumn::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        Slice suffix = get_gs_slice(columns[1], i, &dummy);

        if (suffix.size > str.size) {
            data[i] = 0;
        } else {
            data[i] = (memcmp(str.data + str.size - suffix.size, suffix.data, suffix.size) == 0) ? 1 : 0;
        }
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// null_or_empty(STRING_V2) -> BOOLEAN
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::null_or_empty(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);

    const auto col = columns[0];
    const auto num_rows = col->size();

    auto result = BooleanColumn::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    if (col->only_null()) {
        for (size_t i = 0; i < num_rows; i++) {
            data[i] = 1;
        }
        return result;
    }

    const auto* gs_col = get_gs_column(col);
    bool is_const = col->is_constant();

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(col, i)) {
            data[i] = 1;
        } else {
            size_t gs_idx = is_const ? 0 : i;
            data[i] = (gs_col->get_german_string(gs_idx).len == 0) ? 1 : 0;
        }
    }

    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// instr(STRING_V2, STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::instr(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice haystack = get_gs_slice(columns[0], i, &dummy);
        Slice needle = get_gs_slice(columns[1], i, &dummy);

        int pos = index_of(haystack.data, haystack.size, needle.data, needle.size, 0);
        data[i] = (pos < 0) ? 0 : pos + 1; // 1-based
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// locate(STRING_V2, STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::locate(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    // locate(needle, haystack) - note: arg order is reversed from instr
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice needle = get_gs_slice(columns[0], i, &dummy);
        Slice haystack = get_gs_slice(columns[1], i, &dummy);

        int pos = index_of(haystack.data, haystack.size, needle.data, needle.size, 0);
        data[i] = (pos < 0) ? 0 : pos + 1;
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// locate_pos(STRING_V2, STRING_V2, INT) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::locate_pos(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    // locate(needle, haystack, start_pos)
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice needle = get_gs_slice(columns[0], i, &dummy);
        Slice haystack = get_gs_slice(columns[1], i, &dummy);
        int32_t start_pos = get_int_value(columns[2], i);

        if (start_pos < 1) {
            data[i] = 0;
            continue;
        }

        int pos = index_of(haystack.data, haystack.size, needle.data, needle.size, start_pos - 1);
        data[i] = (pos < 0) ? 0 : pos + 1;
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// lpad(STRING_V2, INT, STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::lpad(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    std::string tmp;
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        int32_t target_len = get_int_value(columns[1], i);
        Slice pad = get_gs_slice(columns[2], i, &dummy);

        if (target_len < 0 || (uint32_t)target_len > get_olap_string_max_length()) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        if (target_len == 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        if (static_cast<int32_t>(str.size) >= target_len) {
            // Truncate from left
            result_col->append(Slice(str.data, target_len));
            continue;
        }

        int32_t pad_len = target_len - str.size;
        tmp.clear();
        tmp.reserve(target_len);
        if (pad.size == 0) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        while (static_cast<int32_t>(tmp.size()) < pad_len) {
            int32_t remain = pad_len - tmp.size();
            int32_t copy_len = std::min(remain, static_cast<int32_t>(pad.size));
            tmp.append(pad.data, copy_len);
        }
        tmp.append(str.data, str.size);
        result_col->append(Slice(tmp));
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// rpad(STRING_V2, INT, STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::rpad(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    std::string tmp;
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        int32_t target_len = get_int_value(columns[1], i);
        Slice pad = get_gs_slice(columns[2], i, &dummy);

        if (target_len < 0 || (uint32_t)target_len > get_olap_string_max_length()) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        if (target_len == 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        if (static_cast<int32_t>(str.size) >= target_len) {
            result_col->append(Slice(str.data, target_len));
            continue;
        }

        tmp.clear();
        tmp.reserve(target_len);
        tmp.append(str.data, str.size);
        if (pad.size == 0) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        while (static_cast<int32_t>(tmp.size()) < target_len) {
            int32_t remain = target_len - tmp.size();
            int32_t copy_len = std::min(remain, static_cast<int32_t>(pad.size));
            tmp.append(pad.data, copy_len);
        }
        result_col->append(Slice(tmp));
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// repeat(STRING_V2, INT) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::repeat(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    std::string tmp;
    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        int32_t times = get_int_value(columns[1], i);

        if (times <= 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        size_t result_len = static_cast<size_t>(str.size) * times;
        if (result_len > get_olap_string_max_length()) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        tmp.clear();
        tmp.reserve(result_len);
        for (int32_t t = 0; t < times; t++) {
            tmp.append(str.data, str.size);
        }
        result_col->append(Slice(tmp));
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// split_part(STRING_V2, STRING_V2, INT) -> STRING_V2
// ============================================================================

static bool split_index_v2(const Slice& haystack, const Slice& delimiter, int32_t part_number, Slice& result) {
    if (part_number > 0) {
        int32_t current_part = 1;
        const char* begin = haystack.data;
        const char* end = haystack.data + haystack.size;
        const char* p = begin;
        while (p <= end - static_cast<int32_t>(delimiter.size)) {
            if (memcmp(p, delimiter.data, delimiter.size) == 0) {
                if (current_part == part_number) {
                    result = Slice(begin, p - begin);
                    return true;
                }
                current_part++;
                p += delimiter.size;
                begin = p;
            } else {
                p++;
            }
        }
        if (current_part == part_number) {
            result = Slice(begin, end - begin);
            return true;
        }
    } else {
        // Negative part_number: count from the end
        // First, collect all parts
        std::vector<Slice> parts;
        const char* begin = haystack.data;
        const char* end = haystack.data + haystack.size;
        const char* p = begin;
        while (p <= end - static_cast<int32_t>(delimiter.size)) {
            if (memcmp(p, delimiter.data, delimiter.size) == 0) {
                parts.emplace_back(begin, p - begin);
                p += delimiter.size;
                begin = p;
            } else {
                p++;
            }
        }
        parts.emplace_back(begin, end - begin);

        int32_t idx = static_cast<int32_t>(parts.size()) + part_number;
        if (idx >= 0 && idx < static_cast<int32_t>(parts.size())) {
            result = parts[idx];
            return true;
        }
    }
    return false;
}

StatusOr<ColumnPtr> StringV2Functions::split_part(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    DCHECK_EQ(columns.size(), 3);

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            result_col->append(Slice("", 0));
            continue;
        }
        bool dummy;
        Slice haystack = get_gs_slice(columns[0], i, &dummy);
        Slice delimiter = get_gs_slice(columns[1], i, &dummy);
        int32_t part_number = get_int_value(columns[2], i);

        if (part_number == 0) {
            result_col->append(Slice("", 0));
            continue;
        }

        if (delimiter.size == 0) {
            // Empty delimiter: split by character
            int32_t char_len = utf8_len(haystack.data, haystack.data + haystack.size);
            int32_t real_part = part_number > 0 ? part_number : char_len + part_number + 1;
            if (real_part < 1 || real_part > char_len) {
                result_col->append(Slice("", 0));
            } else {
                const char* p = haystack.data;
                for (int32_t j = 0; j < real_part - 1 && p < haystack.data + haystack.size; j++) {
                    p += UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(*p)];
                }
                if (p >= haystack.data + haystack.size) {
                    result_col->append(Slice("", 0));
                } else {
                    int32_t char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(*p)];
                    result_col->append(Slice(p, char_size));
                }
            }
            continue;
        }

        Slice slice;
        if (split_index_v2(haystack, delimiter, part_number, slice)) {
            result_col->append(slice);
        } else {
            if (part_number == 1 || part_number == -1) {
                result_col->append(haystack);
            } else {
                result_col->append(Slice("", 0));
            }
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// replace(STRING_V2, STRING_V2, STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::replace(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }
        bool dummy;
        Slice str_slice = get_gs_slice(columns[0], i, &dummy);
        Slice ptn_slice = get_gs_slice(columns[1], i, &dummy);
        Slice rpl_slice = get_gs_slice(columns[2], i, &dummy);

        if (str_slice.empty()) {
            result_col->append(str_slice);
            continue;
        }

        if (ptn_slice.empty()) {
            result_col->append(str_slice);
            continue;
        }

        std::string str(str_slice.data, str_slice.size);
        std::string ptn(ptn_slice.data, ptn_slice.size);
        std::string rpl(rpl_slice.data, rpl_slice.size);

        for (auto found = str.find(ptn); found != std::string::npos; found = str.find(ptn, found + rpl.length())) {
            str.replace(found, ptn.length(), rpl);
        }

        result_col->append(Slice(str));
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// find_in_set(STRING_V2, STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::find_in_set(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice needle = get_gs_slice(columns[0], i, &dummy);
        Slice haystack = get_gs_slice(columns[1], i, &dummy);

        // Search for needle in comma-separated haystack
        int position = 1;
        const char* begin = haystack.data;
        const char* end = haystack.data + haystack.size;
        const char* p = begin;

        bool found = false;
        while (p <= end) {
            const char* comma = p;
            while (comma < end && *comma != ',') {
                comma++;
            }
            int32_t part_len = comma - p;
            if (part_len == static_cast<int32_t>(needle.size) && memcmp(p, needle.data, needle.size) == 0) {
                found = true;
                break;
            }
            p = comma + 1;
            position++;
        }
        data[i] = found ? position : 0;
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// strcmp(STRING_V2, STRING_V2) -> INT
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::strcmp(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto num_rows = columns[0]->size();
    auto result = Int32Column::create();
    result->resize(num_rows);
    auto* data = result->get_data().data();

    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            data[i] = 0;
            continue;
        }
        bool dummy;
        Slice lhs = get_gs_slice(columns[0], i, &dummy);
        Slice rhs = get_gs_slice(columns[1], i, &dummy);

        int min_len = std::min(lhs.size, rhs.size);
        int cmp = memcmp(lhs.data, rhs.data, min_len);
        if (cmp == 0) {
            cmp = (lhs.size > rhs.size) - (lhs.size < rhs.size);
        } else {
            cmp = (cmp > 0) ? 1 : -1;
        }
        data[i] = cmp;
    }

    if (has_null) {
        auto res = NullableColumn::create(std::move(result), std::move(null_col));
        res->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(res), num_rows)
                                                   : ColumnPtr(std::move(res));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                               : ColumnPtr(std::move(result));
}

// ============================================================================
// regexp_extract(STRING_V2, STRING_V2, BIGINT) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::regexp_extract(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto* state = reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    const auto num_rows = columns[0]->size();

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        int64_t group_idx = get_int64_value(columns[2], i);

        re2::RE2* regex = nullptr;
        std::unique_ptr<re2::RE2> local_regex;

        if (state->const_pattern) {
            regex = state->get_or_prepare_regex();
        } else {
            Slice pattern = get_gs_slice(columns[1], i, &dummy);
            std::string pattern_str(pattern.data, pattern.size);
            local_regex = std::make_unique<re2::RE2>(pattern_str, *(state->options));
            if (!local_regex->ok()) {
                result_col->append(Slice("", 0));
                continue;
            }
            regex = local_regex.get();
        }

        if (group_idx < 0 || group_idx > regex->NumberOfCapturingGroups()) {
            result_col->append(Slice("", 0));
            continue;
        }

        int max_matches = 1 + regex->NumberOfCapturingGroups();
        std::vector<re2::StringPiece> matches(max_matches);
        re2::StringPiece str_sp(str.data, str.size);

        if (regex->Match(str_sp, 0, str.size, RE2::UNANCHORED, matches.data(), max_matches)) {
            const auto& match = matches[group_idx];
            result_col->append(Slice(match.data(), match.size()));
        } else {
            result_col->append(Slice("", 0));
        }
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

// ============================================================================
// regexp_replace(STRING_V2, STRING_V2, STRING_V2) -> STRING_V2
// ============================================================================

StatusOr<ColumnPtr> StringV2Functions::regexp_replace(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto* state = reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    const auto num_rows = columns[0]->size();

    auto result_col = GermanStringColumn::create();
    result_col->reserve(num_rows);
    auto null_col = NullColumn::create(num_rows, 0);
    bool has_null = false;

    for (size_t i = 0; i < num_rows; i++) {
        if (is_row_null(columns[0], i) || is_row_null(columns[1], i) || is_row_null(columns[2], i)) {
            has_null = true;
            null_col->get_data()[i] = 1;
            result_col->append_default();
            continue;
        }

        bool dummy;
        Slice str = get_gs_slice(columns[0], i, &dummy);
        Slice rpl = get_gs_slice(columns[2], i, &dummy);

        re2::RE2* regex = nullptr;
        std::unique_ptr<re2::RE2> local_regex;

        if (state->const_pattern) {
            regex = state->get_or_prepare_regex();
        } else {
            Slice pattern = get_gs_slice(columns[1], i, &dummy);
            std::string pattern_str(pattern.data, pattern.size);
            local_regex = std::make_unique<re2::RE2>(pattern_str, *(state->options));
            if (!local_regex->ok()) {
                result_col->append(str);
                continue;
            }
            regex = local_regex.get();
        }

        std::string result_str(str.data, str.size);
        re2::StringPiece rpl_sp(rpl.data, rpl.size);
        re2::RE2::GlobalReplace(&result_str, *regex, rpl_sp);
        result_col->append(Slice(result_str));
    }

    if (has_null) {
        auto result = NullableColumn::create(std::move(result_col), std::move(null_col));
        result->set_has_null(true);
        return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result), num_rows)
                                                   : ColumnPtr(std::move(result));
    }
    return ColumnHelper::is_all_const(columns) ? ConstColumn::create(std::move(result_col), num_rows)
                                               : ColumnPtr(std::move(result_col));
}

} // namespace starrocks

#include "gen_cpp/opcode/StringV2Functions.inc"
