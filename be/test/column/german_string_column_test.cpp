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

#include "column/german_string_column.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/german_string.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "types/datum.h"

namespace starrocks {
namespace {

std::string slice_to_string(const Slice& s) {
    return std::string(s.data, s.size);
}

} // namespace

TEST(GermanStringColumnTest, AppendInlineAndLong) {
    auto col = GermanStringColumn::create();
    col->append(Slice("hi"));                                   // inline len=2
    col->append(Slice("twelve char0"));                         // inline len=12
    col->append(Slice("this is a long string payload"));        // long > 12
    col->append_string(std::string("another long one for arena"));

    ASSERT_EQ(4, col->size());
    EXPECT_EQ("hi", slice_to_string(col->get_slice(0)));
    EXPECT_EQ("twelve char0", slice_to_string(col->get_slice(1)));
    EXPECT_EQ("this is a long string payload", slice_to_string(col->get_slice(2)));
    EXPECT_EQ("another long one for arena", slice_to_string(col->get_slice(3)));

    // inline-ness classification matches 12-byte threshold.
    EXPECT_TRUE(col->get_german_string(0).is_inline());
    EXPECT_TRUE(col->get_german_string(1).is_inline());
    EXPECT_FALSE(col->get_german_string(2).is_inline());
    EXPECT_FALSE(col->get_german_string(3).is_inline());
}

TEST(GermanStringColumnTest, GetSliceReflectsInlineAndLong) {
    auto col = GermanStringColumn::create();
    col->append(Slice(""));
    col->append(Slice("abc"));
    col->append(Slice("0123456789ABCDEF")); // 16 bytes, long form

    EXPECT_EQ(0u, col->get_slice(0).size);
    EXPECT_EQ("abc", slice_to_string(col->get_slice(1)));
    EXPECT_EQ("0123456789ABCDEF", slice_to_string(col->get_slice(2)));
}

TEST(GermanStringColumnTest, CompareAtAcrossColumns) {
    auto a = GermanStringColumn::create();
    auto b = GermanStringColumn::create();
    a->append(Slice("alpha"));
    a->append(Slice("this is a longer string value"));
    b->append(Slice("beta"));
    b->append(Slice("this is a longer string zzzzz"));

    EXPECT_LT(a->compare_at(0, 0, *b, -1), 0);                    // alpha < beta
    EXPECT_GT(b->compare_at(0, 0, *a, -1), 0);                    // beta > alpha
    EXPECT_EQ(0, a->compare_at(0, 0, *a, -1));                    // alpha == alpha
    EXPECT_LT(a->compare_at(1, 1, *b, -1), 0);                    // long rep prefix path
}

TEST(GermanStringColumnTest, FilterRetainsLongPayloadAfterSourceFreed) {
    auto src = GermanStringColumn::create();
    src->append(Slice("keep me arooound"));  // 16 bytes -> long
    src->append(Slice("drop this one please!"));
    src->append(Slice("also keep this one!!!"));

    Filter f = {1, 0, 1};
    src->filter_range(f, 0, 3);
    ASSERT_EQ(2, src->size());

    // After filter, the survivors still resolve to the same bytes via the column's own arena.
    EXPECT_EQ("keep me arooound", slice_to_string(src->get_slice(0)));
    EXPECT_EQ("also keep this one!!!", slice_to_string(src->get_slice(1)));
}

TEST(GermanStringColumnTest, SerializeDeserializeRoundTrip) {
    auto src = GermanStringColumn::create();
    src->append(Slice("short"));
    src->append(Slice("a very long string of bytes to force long rep path"));
    src->append(Slice(""));

    // Use max_one_element_serialize_size for buffer size.
    auto restored = GermanStringColumn::create();
    std::vector<uint8_t> buffer(src->max_one_element_serialize_size());
    for (size_t i = 0; i < src->size(); ++i) {
        std::fill(buffer.begin(), buffer.end(), 0);
        uint32_t written = src->serialize(i, buffer.data());
        ASSERT_GE(buffer.size(), written);
        restored->deserialize_and_append(buffer.data());
    }
    ASSERT_EQ(src->size(), restored->size());
    for (size_t i = 0; i < src->size(); ++i) {
        EXPECT_EQ(slice_to_string(src->get_slice(i)), slice_to_string(restored->get_slice(i)));
    }
}

TEST(GermanStringColumnTest, CloneIsIndependent) {
    auto src = GermanStringColumn::create();
    src->append(Slice("one tiny"));
    src->append(Slice("long string needing own arena"));
    auto cloned = src->clone();

    // Mutate the clone.
    down_cast<GermanStringColumn*>(cloned.get())->append(Slice("added on clone only"));
    ASSERT_EQ(2, src->size());
    ASSERT_EQ(3, cloned->size());
    EXPECT_EQ("one tiny", slice_to_string(src->get_slice(0)));
    EXPECT_EQ("long string needing own arena", slice_to_string(src->get_slice(1)));

    auto* gcloned = down_cast<GermanStringColumn*>(cloned.get());
    EXPECT_EQ("long string needing own arena", slice_to_string(gcloned->get_slice(1)));
    EXPECT_EQ("added on clone only", slice_to_string(gcloned->get_slice(2)));

    // Drop the source entirely; cloned must still be usable because it owns its own arena.
    src.reset();
    EXPECT_EQ("long string needing own arena", slice_to_string(gcloned->get_slice(1)));
}

TEST(GermanStringColumnTest, AppendSelectiveCopiesLongRepBytes) {
    auto src = GermanStringColumn::create();
    src->append(Slice("inline"));
    src->append(Slice("this long one must be copied into dst arena"));
    src->append(Slice("another reasonably long string goes here"));

    auto dst = GermanStringColumn::create();
    const uint32_t indexes[] = {2, 0, 1};
    dst->append_selective(*src, indexes, 0, 3);
    ASSERT_EQ(3, dst->size());
    EXPECT_EQ("another reasonably long string goes here", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("inline", slice_to_string(dst->get_slice(1)));
    EXPECT_EQ("this long one must be copied into dst arena", slice_to_string(dst->get_slice(2)));

    // Free the source column; destination must remain valid because long-rep bytes
    // were copied into its own arena.
    src.reset();
    EXPECT_EQ("another reasonably long string goes here", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("inline", slice_to_string(dst->get_slice(1)));
    EXPECT_EQ("this long one must be copied into dst arena", slice_to_string(dst->get_slice(2)));
}

TEST(GermanStringColumnTest, ResetColumnReleasesArenaAndRowBuffer) {
    auto col = GermanStringColumn::create();
    col->append(Slice("some long-enough text"));
    col->append(Slice("another long-enough text"));
    ASSERT_EQ(2, col->size());
    col->reset_column();
    EXPECT_EQ(0, col->size());

    // After reset, the column must be usable again.
    col->append(Slice("fresh start goes here"));
    ASSERT_EQ(1, col->size());
    EXPECT_EQ("fresh start goes here", slice_to_string(col->get_slice(0)));
}

TEST(GermanStringColumnTest, UpdateRowsInPlace) {
    auto col = GermanStringColumn::create();
    col->append(Slice("row0 inline"));
    col->append(Slice("row1 long string value goes here"));
    col->append(Slice("row2 inline"));

    auto replacement = GermanStringColumn::create();
    replacement->append(Slice("R0 new"));
    replacement->append(Slice("R1 brand new long string value"));
    const uint32_t indexes[] = {0, 1};
    col->update_rows(*replacement, indexes);

    EXPECT_EQ("R0 new", slice_to_string(col->get_slice(0)));
    EXPECT_EQ("R1 brand new long string value", slice_to_string(col->get_slice(1)));
    EXPECT_EQ("row2 inline", slice_to_string(col->get_slice(2)));

    // Replacement column dropped; col must still resolve its own long-rep bytes.
    replacement.reset();
    EXPECT_EQ("R1 brand new long string value", slice_to_string(col->get_slice(1)));
}

TEST(GermanStringColumnTest, AppendDatumAndGetRoundTrip) {
    auto col = GermanStringColumn::create();
    // append_datum now routes through Datum's Slice variant; long-rep bytes end
    // up copied into the column's arena.
    Datum short_d(Slice("dshort"));
    Datum long_d(Slice("datum long string long string"));
    col->append_datum(short_d);
    col->append_datum(long_d);

    ASSERT_EQ(2, col->size());
    // Datum get() still returns a Slice-backed Datum aliasing the stored bytes.
    EXPECT_EQ("dshort", slice_to_string(col->get(0).get_slice()));
    EXPECT_EQ("datum long string long string", slice_to_string(col->get(1).get_slice()));
}

TEST(GermanStringColumnTest, DatumGetGermanStringWrapsSliceBytes) {
    // Datum::get_german_string() is a thin view over the stored Slice bytes.
    const char* long_bytes = "long payload owned by the caller";
    Datum d(Slice(long_bytes, strlen(long_bytes)));
    GermanString gs = d.get_german_string();
    EXPECT_EQ(strlen(long_bytes), gs.len);
    EXPECT_EQ(0, std::memcmp(gs.get_data(), long_bytes, gs.len));

    // set_german_string stores bytes as a Slice aliasing the GermanString's payload.
    Datum d2;
    d2.set_german_string(gs);
    EXPECT_EQ(gs.len, d2.get_slice().size);
    EXPECT_EQ(gs.get_data(), d2.get_slice().data);
}

// ---------------------------------------------------------------------------
// MySQL wire emission
// ---------------------------------------------------------------------------
//
// The final MySQL result sink dispatches row-wise through the polymorphic
// `Column::put_mysql_row_buffer`. GermanStringColumn must emit the exact
// same bytes onto the wire as a BinaryColumn with the same logical content,
// so JDBC / MySQL clients see identical VARCHAR rows regardless of which
// column representation the fragment produces.

TEST(GermanStringColumnTest, PutMysqlRowBufferMatchesBinaryColumn) {
    // Mix of empty, short inline, exactly-12 boundary and >12 long-rep rows.
    const std::vector<std::string> rows = {
            "",
            "abc",
            "twelve char0",                              // 12 bytes: boundary, still inline
            "0123456789ABCDEF",                          // 16 bytes: long rep
            "a very long string of bytes for long rep",  // long rep
            "contains \" and \\ escapes",                // ensures escape path equality
    };

    auto gs_col = GermanStringColumn::create();
    auto bin_col = BinaryColumn::create();
    for (const auto& s : rows) {
        gs_col->append(Slice(s));
        bin_col->append(Slice(s));
    }

    // Text protocol
    {
        MysqlRowBuffer gs_buf;
        MysqlRowBuffer bin_buf;
        for (size_t i = 0; i < rows.size(); ++i) {
            gs_col->put_mysql_row_buffer(&gs_buf, i, /*is_binary_protocol=*/false);
            bin_col->put_mysql_row_buffer(&bin_buf, i, /*is_binary_protocol=*/false);
        }
        EXPECT_EQ(bin_buf.data(), gs_buf.data());
    }

    // Binary protocol (prepared statements). For VARCHAR the wire format is
    // the same length-prefixed string blob.
    {
        MysqlRowBuffer gs_buf(/*is_binary_format=*/true);
        MysqlRowBuffer bin_buf(/*is_binary_format=*/true);
        for (size_t i = 0; i < rows.size(); ++i) {
            gs_col->put_mysql_row_buffer(&gs_buf, i, /*is_binary_protocol=*/true);
            bin_col->put_mysql_row_buffer(&bin_buf, i, /*is_binary_protocol=*/true);
        }
        EXPECT_EQ(bin_buf.data(), gs_buf.data());
    }
}

TEST(GermanStringColumnTest, PutMysqlRowBufferInsideNullableMatchesBinary) {
    // Nullable wrappers should dispatch through to the underlying column's
    // put_mysql_row_buffer, so a nullable GermanStringColumn with the same
    // null mask must produce identical bytes to a nullable BinaryColumn.
    auto gs_data = GermanStringColumn::create();
    auto gs_nulls = NullColumn::create();
    auto bin_data = BinaryColumn::create();
    auto bin_nulls = NullColumn::create();

    const std::vector<std::pair<std::string, bool>> rows = {
            {"", false},
            {"short", false},
            {"irrelevant because null", true},
            {"another long one for arena path", false},
            {"hidden too", true},
    };
    for (const auto& [s, is_null] : rows) {
        gs_data->append(Slice(s));
        bin_data->append(Slice(s));
        gs_nulls->append(is_null ? 1 : 0);
        bin_nulls->append(is_null ? 1 : 0);
    }

    auto gs_nullable = NullableColumn::create(std::move(gs_data), std::move(gs_nulls));
    auto bin_nullable = NullableColumn::create(std::move(bin_data), std::move(bin_nulls));

    MysqlRowBuffer gs_buf;
    MysqlRowBuffer bin_buf;
    for (size_t i = 0; i < rows.size(); ++i) {
        gs_nullable->put_mysql_row_buffer(&gs_buf, i);
        bin_nullable->put_mysql_row_buffer(&bin_buf, i);
    }
    EXPECT_EQ(bin_buf.data(), gs_buf.data());
}

} // namespace starrocks
