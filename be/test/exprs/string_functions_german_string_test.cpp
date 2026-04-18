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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/german_string_column.h"
#include "column/nullable_column.h"
#include "exprs/string_functions.h"

namespace starrocks {

// Build a GermanStringColumn from a vector of strings.
static GermanStringColumn::MutablePtr make_gs_column(const std::vector<std::string>& rows) {
    auto col = GermanStringColumn::create();
    for (const auto& r : rows) {
        col->append_string(r);
    }
    return col;
}

static std::vector<std::string> dump_gs_column(const ColumnPtr& col) {
    std::vector<std::string> out;
    const Column* data_col = col.get();
    const NullColumn* null_col = nullptr;
    if (data_col->is_nullable()) {
        const auto* n = down_cast<const NullableColumn*>(data_col);
        null_col = down_cast<const NullColumn*>(n->null_column().get());
        data_col = n->data_column().get();
    }
    const auto* gs = down_cast<const GermanStringColumn*>(data_col);
    for (size_t i = 0; i < gs->size(); ++i) {
        if (null_col != nullptr && null_col->get_data()[i]) {
            out.emplace_back("<null>");
        } else {
            out.emplace_back(gs->get_slice(i).to_string());
        }
    }
    return out;
}

// ---- length ----

TEST(GermanStringBuiltinsTest, Length) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"", "abc", "hello", "0123456789012345"}));
    auto result = StringFunctions::length_german_string(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_nullable());
    auto col = ColumnHelper::cast_to<TYPE_INT>(result);
    ASSERT_EQ(0, col->get_data()[0]);
    ASSERT_EQ(3, col->get_data()[1]);
    ASSERT_EQ(5, col->get_data()[2]);
    // 16 chars > inline threshold -> exercises long-rep len read.
    ASSERT_EQ(16, col->get_data()[3]);
}

// ---- char_length (utf8_length) ----

TEST(GermanStringBuiltinsTest, CharLength) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    // "你好" is 6 bytes, 2 characters. "abc" is 3 bytes, 3 chars.
    columns.emplace_back(make_gs_column({"abc", "你好", "mix你好ab"}));
    auto result = StringFunctions::utf8_length_german_string(ctx.get(), columns).value();
    auto col = ColumnHelper::cast_to<TYPE_INT>(result);
    ASSERT_EQ(3, col->get_data()[0]);
    ASSERT_EQ(2, col->get_data()[1]);
    // 3 ascii + 2 utf8 chars + 2 ascii = 7
    ASSERT_EQ(7, col->get_data()[2]);
}

// ---- substring ----

TEST(GermanStringBuiltinsTest, SubstringAscii) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"abcdefghij", "short", "0123456789abcdef"}));
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    pos->append(2);
    len->append(3);
    pos->append(1);
    len->append(100);
    pos->append(-5);
    len->append(3);
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));
    auto result = StringFunctions::substring_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("bcd", vals[0]);
    ASSERT_EQ("short", vals[1]);
    // 16-byte string -> long-rep. substr from right 5 chars length 3: "bcd".
    ASSERT_EQ("bcd", vals[2]);
}

TEST(GermanStringBuiltinsTest, SubstringUtf8) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"我是中文字符串"}));
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    pos->append(3);
    len->append(2);
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));
    auto result = StringFunctions::substring_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("中文", vals[0]);
}

// ---- lower / upper ----

TEST(GermanStringBuiltinsTest, LowerUpper) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    {
        Columns columns;
        columns.emplace_back(make_gs_column({"AbCdE", "HELLO WORLD AND A LONGER LINE"}));
        auto result = StringFunctions::lower_german_string(ctx.get(), columns).value();
        auto vals = dump_gs_column(result);
        ASSERT_EQ("abcde", vals[0]);
        ASSERT_EQ("hello world and a longer line", vals[1]);
    }
    {
        Columns columns;
        columns.emplace_back(make_gs_column({"AbCdE", "hello world and a longer line"}));
        auto result = StringFunctions::upper_german_string(ctx.get(), columns).value();
        auto vals = dump_gs_column(result);
        ASSERT_EQ("ABCDE", vals[0]);
        ASSERT_EQ("HELLO WORLD AND A LONGER LINE", vals[1]);
    }
}

// ---- reverse ----

TEST(GermanStringBuiltinsTest, Reverse) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"abc", "hello world longer than 12"}));
    auto result = StringFunctions::reverse_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("cba", vals[0]);
    ASSERT_EQ("21 naht regnol dlrow olleh", vals[1]);
}

// ---- repeat ----

TEST(GermanStringBuiltinsTest, Repeat) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"a", "bc", "hello"}));
    auto times = Int32Column::create();
    times->append(3);
    times->append(4);
    times->append(0);
    columns.emplace_back(std::move(times));
    auto result = StringFunctions::repeat_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("aaa", vals[0]);
    ASSERT_EQ("bcbcbcbc", vals[1]);
    ASSERT_EQ("", vals[2]);
}

// ---- concat ----

TEST(GermanStringBuiltinsTest, Concat) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"a", "foo", ""}));
    columns.emplace_back(make_gs_column({"b", "bar", "baz"}));
    columns.emplace_back(make_gs_column({"c", "baz", "qux"}));
    auto result = StringFunctions::concat_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("abc", vals[0]);
    ASSERT_EQ("foobarbaz", vals[1]);
    ASSERT_EQ("bazqux", vals[2]);
}

// =============================================================================
// Batch (b): trim/ltrim/rtrim/lpad/rpad/replace/split_part/instr/locate.
// =============================================================================

// Build a FunctionContext whose fragment-local trim state has been populated
// with a single-char removal set.
static std::unique_ptr<FunctionContext> make_trim_ctx(const std::string& remove) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto pattern_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(remove, 1);
    Columns const_cols = {nullptr, pattern_col};
    ctx->set_constant_columns(const_cols);
    CHECK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    return ctx;
}

// Build a FunctionContext whose fragment-local pad state has been populated
// with a constant fill string (state->fill_is_const = true).
static std::unique_ptr<FunctionContext> make_pad_ctx(const std::string& fill) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto fill_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(fill, 1);
    Columns const_cols = {nullptr, nullptr, fill_col};
    ctx->set_constant_columns(const_cols);
    CHECK(StringFunctions::pad_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    return ctx;
}

TEST(GermanStringBuiltinsTest, TrimDefaultSpaces) {
    auto ctx = make_trim_ctx(" ");
    Columns columns;
    columns.emplace_back(make_gs_column({"  hello ", "nospace", "   ", "   long  string  with spaces   "}));
    auto result_both = StringFunctions::trim_german_string(ctx.get(), columns).value();
    auto both = dump_gs_column(result_both);
    ASSERT_EQ("hello", both[0]);
    ASSERT_EQ("nospace", both[1]);
    ASSERT_EQ("", both[2]);
    ASSERT_EQ("long  string  with spaces", both[3]);

    auto result_l = StringFunctions::ltrim_german_string(ctx.get(), columns).value();
    auto l = dump_gs_column(result_l);
    ASSERT_EQ("hello ", l[0]);
    ASSERT_EQ("long  string  with spaces   ", l[3]);

    auto result_r = StringFunctions::rtrim_german_string(ctx.get(), columns).value();
    auto r = dump_gs_column(result_r);
    ASSERT_EQ("  hello", r[0]);
    ASSERT_EQ("   long  string  with spaces", r[3]);
    CHECK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST(GermanStringBuiltinsTest, TrimCustomChars) {
    // Multi-char ASCII removal set.
    auto ctx = make_trim_ctx("ab");
    Columns columns;
    columns.emplace_back(make_gs_column({"aabbccddbbaa", "aabbaaabba"}));
    auto result = StringFunctions::trim_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("ccdd", vals[0]);
    ASSERT_EQ("", vals[1]);
    CHECK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST(GermanStringBuiltinsTest, LPadRPad) {
    auto ctx = make_pad_ctx("*.");
    Columns columns;
    columns.emplace_back(make_gs_column({"hi", "hello"}));
    auto len_col = Int32Column::create();
    len_col->append(6);
    len_col->append(3);
    columns.emplace_back(std::move(len_col));
    columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("*.", 2));

    auto lpad_result = StringFunctions::lpad_german_string(ctx.get(), columns).value();
    auto lpad = dump_gs_column(lpad_result);
    ASSERT_EQ("*.*.hi", lpad[0]);
    // target < str length -> prefix-of-str.
    ASSERT_EQ("hel", lpad[1]);

    auto rpad_result = StringFunctions::rpad_german_string(ctx.get(), columns).value();
    auto rpad = dump_gs_column(rpad_result);
    ASSERT_EQ("hi*.*.", rpad[0]);
    ASSERT_EQ("hel", rpad[1]);
    CHECK(StringFunctions::pad_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST(GermanStringBuiltinsTest, Replace) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    // String long enough to land in long-rep after replacement.
    columns.emplace_back(make_gs_column({"hello world", "abc abc abc", "foo"}));
    columns.emplace_back(make_gs_column({"l", "abc", "bar"}));
    columns.emplace_back(make_gs_column({"LL", "xyz", ""}));
    auto result = StringFunctions::replace_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("heLLLLo worLLd", vals[0]);
    ASSERT_EQ("xyz xyz xyz", vals[1]);
    // No match -> identity.
    ASSERT_EQ("foo", vals[2]);
}

TEST(GermanStringBuiltinsTest, SplitPart) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"a,b,c,d,e", "hello--world--foo", "single", ""}));
    columns.emplace_back(make_gs_column({",", "--", "x", ""}));
    auto part_col = Int32Column::create();
    part_col->append(3);
    part_col->append(-1);
    part_col->append(1);
    part_col->append(1);
    columns.emplace_back(std::move(part_col));
    auto result = StringFunctions::split_part_german_string(ctx.get(), columns).value();
    auto vals = dump_gs_column(result);
    ASSERT_EQ("c", vals[0]);
    // Negative index counts from the right.
    ASSERT_EQ("foo", vals[1]);
    // Delimiter absent, part=1 -> entire haystack.
    ASSERT_EQ("single", vals[2]);
    // Empty delimiter + empty haystack + part=1 -> empty.
    ASSERT_EQ("", vals[3]);
}

TEST(GermanStringBuiltinsTest, Instr) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    columns.emplace_back(make_gs_column({"hello world", "starrocks", "abc"}));
    columns.emplace_back(make_gs_column({"world", "rocks", "z"}));
    auto result = StringFunctions::instr_german_string(ctx.get(), columns).value();
    auto col = ColumnHelper::cast_to<TYPE_INT>(result);
    ASSERT_EQ(7, col->get_data()[0]);
    ASSERT_EQ(5, col->get_data()[1]);
    // No match.
    ASSERT_EQ(0, col->get_data()[2]);
}

TEST(GermanStringBuiltinsTest, Locate) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    {
        // locate(needle, haystack): 2-arg form.
        Columns columns;
        columns.emplace_back(make_gs_column({"world", "rocks", "z"}));
        columns.emplace_back(make_gs_column({"hello world", "starrocks", "abc"}));
        auto result = StringFunctions::locate_german_string(ctx.get(), columns).value();
        auto col = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(7, col->get_data()[0]);
        ASSERT_EQ(5, col->get_data()[1]);
        ASSERT_EQ(0, col->get_data()[2]);
    }
    {
        // locate(needle, haystack, start): 3-arg form.
        Columns columns;
        columns.emplace_back(make_gs_column({"ab", "ab"}));
        columns.emplace_back(make_gs_column({"ababab", "ababab"}));
        auto start = Int32Column::create();
        start->append(2);
        start->append(5);
        columns.emplace_back(std::move(start));
        auto result = StringFunctions::locate_pos_german_string(ctx.get(), columns).value();
        auto col = ColumnHelper::cast_to<TYPE_INT>(result);
        // Skips first match at position 1 -> next is at 3.
        ASSERT_EQ(3, col->get_data()[0]);
        // After position 5, only "ab" at pos 5 left.
        ASSERT_EQ(5, col->get_data()[1]);
    }
}

} // namespace starrocks
