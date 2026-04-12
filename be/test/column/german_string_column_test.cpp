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

#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "column/binary_column.h"
#include "column/column_hash/column_hash.h"
#include "column/vectorized_fwd.h"
#include "serde/column_array_serde.h"

namespace starrocks {

// ---- Helpers ----

// Short string: fits inline (<=12 bytes)
static const std::string kShort = "hello";             // 5 bytes
static const std::string kShort2 = "world";            // 5 bytes
static const std::string kShortMax = "123456789012";   // exactly 12 bytes

// Long string: exceeds inline threshold (>12 bytes)
static const std::string kLong = "this is a longer string for testing";  // 35 bytes
static const std::string kLong2 = "another long string value here!";     // 31 bytes
static const std::string kLongExact13 = "1234567890123";                 // exactly 13 bytes

// ---- test_create_empty ----
PARALLEL_TEST(GermanStringColumnTest, test_create_empty) {
    auto col = GermanStringColumn::create();
    ASSERT_EQ(0, col->size());
    ASSERT_EQ("german-string", col->get_name());
}

// ---- test_append_short_string ----
PARALLEL_TEST(GermanStringColumnTest, test_append_short_string) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    ASSERT_EQ(1, gc->size());

    // Short string should be inline
    const auto& gs = gc->get_german_string(0);
    ASSERT_TRUE(gs.is_inline());

    // Data should match
    Slice s = gc->get_slice(0);
    ASSERT_EQ(kShort, s.to_string());
}

// ---- test_append_short_string_boundary ----
PARALLEL_TEST(GermanStringColumnTest, test_append_short_string_boundary) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Exactly 12 bytes: should be inline
    gc->append(Slice(kShortMax));
    ASSERT_EQ(1, gc->size());
    ASSERT_TRUE(gc->get_german_string(0).is_inline());
    ASSERT_EQ(kShortMax, gc->get_slice(0).to_string());

    // Exactly 13 bytes: should NOT be inline
    gc->append(Slice(kLongExact13));
    ASSERT_EQ(2, gc->size());
    ASSERT_FALSE(gc->get_german_string(1).is_inline());
    ASSERT_EQ(kLongExact13, gc->get_slice(1).to_string());
}

// ---- test_append_long_string ----
PARALLEL_TEST(GermanStringColumnTest, test_append_long_string) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kLong));
    ASSERT_EQ(1, gc->size());

    // Long string should NOT be inline
    const auto& gs = gc->get_german_string(0);
    ASSERT_FALSE(gs.is_inline());

    // Data should match
    Slice s = gc->get_slice(0);
    ASSERT_EQ(kLong, s.to_string());
}

// ---- test_append_mixed ----
PARALLEL_TEST(GermanStringColumnTest, test_append_mixed) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    std::vector<std::string> data = {kShort, kLong, "", kShort2, kLong2, kShortMax, kLongExact13};
    for (const auto& s : data) {
        gc->append(Slice(s));
    }
    ASSERT_EQ(data.size(), gc->size());

    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_EQ(data[i], gc->get_slice(i).to_string()) << "mismatch at index " << i;
    }

    // Verify inline/long classification
    ASSERT_TRUE(gc->get_german_string(0).is_inline());   // "hello" (5)
    ASSERT_FALSE(gc->get_german_string(1).is_inline());   // kLong (35)
    ASSERT_TRUE(gc->get_german_string(2).is_inline());    // "" (0)
    ASSERT_TRUE(gc->get_german_string(3).is_inline());    // "world" (5)
    ASSERT_FALSE(gc->get_german_string(4).is_inline());   // kLong2 (31)
    ASSERT_TRUE(gc->get_german_string(5).is_inline());    // kShortMax (12)
    ASSERT_FALSE(gc->get_german_string(6).is_inline());   // kLongExact13 (13)
}

// ---- test_append_empty_string ----
PARALLEL_TEST(GermanStringColumnTest, test_append_empty_string) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("", 0));
    ASSERT_EQ(1, gc->size());
    ASSERT_TRUE(gc->get_german_string(0).is_inline());
    ASSERT_EQ(0, gc->get_slice(0).size);
    ASSERT_EQ("", gc->get_slice(0).to_string());
}

// ---- test_get_slice ----
PARALLEL_TEST(GermanStringColumnTest, test_get_slice) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    // Inline
    Slice s0 = gc->get_slice(0);
    ASSERT_EQ(kShort.size(), s0.size);
    ASSERT_EQ(kShort, s0.to_string());

    // Long
    Slice s1 = gc->get_slice(1);
    ASSERT_EQ(kLong.size(), s1.size);
    ASSERT_EQ(kLong, s1.to_string());
}

// ---- test_get_german_string ----
PARALLEL_TEST(GermanStringColumnTest, test_get_german_string) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    const GermanString& gs0 = gc->get_german_string(0);
    ASSERT_EQ(kShort.size(), gs0.len);
    ASSERT_EQ(kShort, std::string(gs0.get_data(), gs0.len));

    const GermanString& gs1 = gc->get_german_string(1);
    ASSERT_EQ(kLong.size(), gs1.len);
    ASSERT_EQ(kLong, std::string(gs1.get_data(), gs1.len));
}

// ---- test_compare_at ----
PARALLEL_TEST(GermanStringColumnTest, test_compare_at) {
    auto c1 = GermanStringColumn::create();
    auto c2 = GermanStringColumn::create();
    auto* gc1 = down_cast<GermanStringColumn*>(c1.get());
    auto* gc2 = down_cast<GermanStringColumn*>(c2.get());

    std::vector<std::string> strings = {"aaa", "bbb", "ccc", kLong, kLong2};
    for (const auto& s : strings) {
        gc1->append(Slice(s));
        gc2->append(Slice(s));
    }

    // Same index: equal
    for (size_t i = 0; i < strings.size(); ++i) {
        ASSERT_EQ(0, gc1->compare_at(i, i, *gc2, -1));
    }

    // "aaa" < "bbb"
    ASSERT_LT(gc1->compare_at(0, 1, *gc2, -1), 0);
    // "ccc" > "bbb"
    ASSERT_GT(gc1->compare_at(2, 1, *gc2, -1), 0);

    // Cross short-long comparison
    // "aaa" < kLong (which starts with "this...")
    ASSERT_LT(gc1->compare_at(0, 3, *gc2, -1), 0);
}

// ---- test_serialize_deserialize ----
PARALLEL_TEST(GermanStringColumnTest, test_serialize_deserialize) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    std::vector<std::string> data = {"", kShort, kLong, kShortMax, kLongExact13};
    for (const auto& s : data) {
        gc->append(Slice(s));
    }

    // Serialize each row and deserialize into a new column
    auto col2 = GermanStringColumn::create();
    auto* gc2 = down_cast<GermanStringColumn*>(col2.get());

    uint32_t max_ser_size = gc->max_one_element_serialize_size();
    std::vector<uint8_t> buf(max_ser_size);

    for (size_t i = 0; i < gc->size(); ++i) {
        uint32_t written = gc->serialize(i, buf.data());
        ASSERT_EQ(written, gc->serialize_size(i));

        const uint8_t* end = gc2->deserialize_and_append(buf.data());
        ASSERT_EQ(end, buf.data() + written);
    }

    ASSERT_EQ(gc->size(), gc2->size());
    for (size_t i = 0; i < gc->size(); ++i) {
        ASSERT_EQ(gc->get_slice(i).to_string(), gc2->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// ---- test_serialize_default ----
PARALLEL_TEST(GermanStringColumnTest, test_serialize_default) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    uint8_t buf[16];
    uint32_t written = gc->serialize_default(buf);
    ASSERT_EQ(sizeof(uint32_t), written);

    // Deserialize the default into a column
    auto col2 = GermanStringColumn::create();
    auto* gc2 = down_cast<GermanStringColumn*>(col2.get());
    gc2->deserialize_and_append(buf);
    ASSERT_EQ(1, gc2->size());
    ASSERT_EQ("", gc2->get_slice(0).to_string());
}

// ---- test_clone ----
PARALLEL_TEST(GermanStringColumnTest, test_clone) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));
    gc->append(Slice(""));

    auto cloned = gc->clone();
    auto* gc2 = down_cast<GermanStringColumn*>(cloned.get());

    ASSERT_EQ(gc->size(), gc2->size());
    for (size_t i = 0; i < gc->size(); ++i) {
        ASSERT_EQ(gc->get_slice(i).to_string(), gc2->get_slice(i).to_string());
    }

    // Verify deep copy: modifying original should not affect clone
    gc->append(Slice("extra"));
    ASSERT_EQ(4, gc->size());
    ASSERT_EQ(3, gc2->size());
}

// ---- test_clone_empty ----
PARALLEL_TEST(GermanStringColumnTest, test_clone_empty) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    auto empty_clone = gc->clone_empty();
    ASSERT_EQ(0, empty_clone->size());
}

// ---- test_filter_range ----
PARALLEL_TEST(GermanStringColumnTest, test_filter_range) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Append 10 strings: mix of short and long
    std::vector<std::string> data;
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            data.push_back(std::string("s") + std::to_string(i));  // short
        } else {
            data.push_back(std::string("long_string_value_") + std::to_string(i));  // long
        }
        gc->append(Slice(data.back()));
    }

    // Filter: keep only odd-indexed elements
    Filter filter(10);
    for (int i = 0; i < 10; ++i) {
        filter[i] = (i % 2 == 1) ? 1 : 0;
    }

    size_t arena_before = gc->arena_memory_usage();
    gc->filter_range(filter, 0, 10);

    // Arena should be unchanged (lazy compaction)
    ASSERT_EQ(arena_before, gc->arena_memory_usage());

    ASSERT_EQ(5, gc->size());
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(data[i * 2 + 1], gc->get_slice(i).to_string());
    }
}

// ---- test_append_selective ----
PARALLEL_TEST(GermanStringColumnTest, test_append_selective) {
    auto src = GermanStringColumn::create();
    auto* gc_src = down_cast<GermanStringColumn*>(src.get());

    std::vector<std::string> data = {"aaa", kLong, "bbb", kLong2, "ccc"};
    for (const auto& s : data) {
        gc_src->append(Slice(s));
    }

    auto dst = GermanStringColumn::create();
    auto* gc_dst = down_cast<GermanStringColumn*>(dst.get());

    // Select indices [0, 2, 4] (the short strings)
    std::vector<uint32_t> indexes = {0, 2, 4};
    gc_dst->append_selective(*gc_src, indexes.data(), 0, 3);

    ASSERT_EQ(3, gc_dst->size());
    ASSERT_EQ("aaa", gc_dst->get_slice(0).to_string());
    ASSERT_EQ("bbb", gc_dst->get_slice(1).to_string());
    ASSERT_EQ("ccc", gc_dst->get_slice(2).to_string());

    // Select indices [1, 3] (the long strings)
    std::vector<uint32_t> indexes2 = {1, 3};
    gc_dst->append_selective(*gc_src, indexes2.data(), 0, 2);

    ASSERT_EQ(5, gc_dst->size());
    ASSERT_EQ(kLong, gc_dst->get_slice(3).to_string());
    ASSERT_EQ(kLong2, gc_dst->get_slice(4).to_string());
}

// ---- test_to_binary_column ----
PARALLEL_TEST(GermanStringColumnTest, test_to_binary_column) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    std::vector<std::string> data = {"", kShort, kLong, kShortMax, kLongExact13};
    for (const auto& s : data) {
        gc->append(Slice(s));
    }

    auto bc = gc->to_binary_column();
    auto* bin_col = down_cast<const BinaryColumn*>(bc.get());

    ASSERT_EQ(gc->size(), bin_col->size());
    for (size_t i = 0; i < gc->size(); ++i) {
        ASSERT_EQ(gc->get_slice(i).to_string(), bin_col->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// ---- test_append_strings_from_slice ----
PARALLEL_TEST(GermanStringColumnTest, test_append_strings_from_slice) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    std::vector<Slice> slices = {Slice(""), Slice(kShort), Slice(kLong), Slice(kShortMax)};
    ASSERT_TRUE(gc->append_strings(slices.data(), slices.size()));

    ASSERT_EQ(4, gc->size());
    ASSERT_EQ("", gc->get_slice(0).to_string());
    ASSERT_EQ(kShort, gc->get_slice(1).to_string());
    ASSERT_EQ(kLong, gc->get_slice(2).to_string());
    ASSERT_EQ(kShortMax, gc->get_slice(3).to_string());
}

// ---- test_compact ----
PARALLEL_TEST(GermanStringColumnTest, test_compact) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Append many long strings so the arena has significant data
    for (int i = 0; i < 100; ++i) {
        std::string s = "long_string_number_" + std::to_string(i) + "_padding";
        gc->append(Slice(s));
    }

    size_t arena_before = gc->arena_memory_usage();
    ASSERT_GT(arena_before, 0);

    // Filter out half the elements
    Filter filter(100);
    for (int i = 0; i < 100; ++i) {
        filter[i] = (i % 2 == 0) ? 1 : 0;
    }
    gc->filter_range(filter, 0, 100);
    ASSERT_EQ(50, gc->size());

    // Arena still holds all data (lazy compaction)
    ASSERT_EQ(arena_before, gc->arena_memory_usage());

    // Now compact
    gc->compact();

    // Arena should be smaller after compaction
    size_t arena_after = gc->arena_memory_usage();
    ASSERT_LT(arena_after, arena_before);

    // Data integrity preserved
    for (int i = 0; i < 50; ++i) {
        std::string expected = "long_string_number_" + std::to_string(i * 2) + "_padding";
        ASSERT_EQ(expected, gc->get_slice(i).to_string());
    }
}

// ---- test_needs_compaction ----
PARALLEL_TEST(GermanStringColumnTest, test_needs_compaction) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Only inline strings: arena should be 0, no compaction needed
    for (int i = 0; i < 10; ++i) {
        gc->append(Slice("hi"));
    }
    ASSERT_FALSE(gc->needs_compaction());

    // Reset and add long strings
    gc->reset_column();
    for (int i = 0; i < 100; ++i) {
        std::string s = "a_very_long_string_for_compaction_test_" + std::to_string(i);
        gc->append(Slice(s));
    }
    ASSERT_FALSE(gc->needs_compaction());

    // Filter out >50% of elements to create arena waste
    Filter filter(100);
    for (int i = 0; i < 100; ++i) {
        filter[i] = (i < 10) ? 1 : 0;  // keep only first 10
    }
    gc->filter_range(filter, 0, 100);
    ASSERT_EQ(10, gc->size());

    // Arena has allocations for 100 strings but only 10 are live
    // arena_allocated > 2 * live_arena_bytes should be true
    ASSERT_TRUE(gc->needs_compaction());
}

// ---- test_swap_column ----
PARALLEL_TEST(GermanStringColumnTest, test_swap_column) {
    auto c1 = GermanStringColumn::create();
    auto c2 = GermanStringColumn::create();
    auto* gc1 = down_cast<GermanStringColumn*>(c1.get());
    auto* gc2 = down_cast<GermanStringColumn*>(c2.get());

    gc1->append(Slice(kShort));
    gc1->append(Slice(kLong));
    gc1->set_delete_state(DEL_PARTIAL_SATISFIED);

    gc2->append(Slice("xyz"));

    gc1->swap_column(*gc2);

    // c1 should have c2's old data
    ASSERT_EQ(1, gc1->size());
    ASSERT_EQ("xyz", gc1->get_slice(0).to_string());
    ASSERT_EQ(DEL_NOT_SATISFIED, gc1->delete_state());

    // c2 should have c1's old data
    ASSERT_EQ(2, gc2->size());
    ASSERT_EQ(kShort, gc2->get_slice(0).to_string());
    ASSERT_EQ(kLong, gc2->get_slice(1).to_string());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, gc2->delete_state());
}

// ---- test_reset_column ----
PARALLEL_TEST(GermanStringColumnTest, test_reset_column) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));
    gc->set_delete_state(DEL_PARTIAL_SATISFIED);

    gc->reset_column();
    ASSERT_EQ(0, gc->size());
    ASSERT_EQ(DEL_NOT_SATISFIED, gc->delete_state());
    ASSERT_EQ(0, gc->arena_memory_usage());
}

// ---- test_large_column ----
PARALLEL_TEST(GermanStringColumnTest, test_large_column) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    const size_t N = 100'000;
    gc->reserve(N);

    for (size_t i = 0; i < N; ++i) {
        if (i % 3 == 0) {
            // Short inline string
            gc->append(Slice(std::to_string(i)));
        } else if (i % 3 == 1) {
            // Long string
            std::string s = "long_prefix_for_test_" + std::to_string(i) + "_suffix";
            gc->append(Slice(s));
        } else {
            // Empty string
            gc->append(Slice("", 0));
        }
    }
    ASSERT_EQ(N, gc->size());

    // Verify all data
    for (size_t i = 0; i < N; ++i) {
        std::string expected;
        if (i % 3 == 0) {
            expected = std::to_string(i);
        } else if (i % 3 == 1) {
            expected = "long_prefix_for_test_" + std::to_string(i) + "_suffix";
        } else {
            expected = "";
        }
        ASSERT_EQ(expected, gc->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// ---- test_append_column ----
PARALLEL_TEST(GermanStringColumnTest, test_append_column) {
    auto c1 = GermanStringColumn::create();
    auto c2 = GermanStringColumn::create();
    auto* gc1 = down_cast<GermanStringColumn*>(c1.get());
    auto* gc2 = down_cast<GermanStringColumn*>(c2.get());

    gc1->append(Slice("first"));

    gc2->append(Slice("second"));
    gc2->append(Slice(kLong));
    gc2->append(Slice("third"));

    // Append 0 elements: no-op
    gc1->append(*gc2, 0, 0);
    ASSERT_EQ(1, gc1->size());

    // Append 2 elements from offset 1
    gc1->append(*gc2, 1, 2);
    ASSERT_EQ(3, gc1->size());
    ASSERT_EQ("first", gc1->get_slice(0).to_string());
    ASSERT_EQ(kLong, gc1->get_slice(1).to_string());
    ASSERT_EQ("third", gc1->get_slice(2).to_string());
}

// ---- test_append_value_multiple_times ----
PARALLEL_TEST(GermanStringColumnTest, test_append_value_multiple_times) {
    auto src = GermanStringColumn::create();
    auto* gc_src = down_cast<GermanStringColumn*>(src.get());
    gc_src->append(Slice(kShort));
    gc_src->append(Slice(kLong));

    auto dst = GermanStringColumn::create();
    auto* gc_dst = down_cast<GermanStringColumn*>(dst.get());

    // Repeat short string 3 times
    gc_dst->append_value_multiple_times(*gc_src, 0, 3);
    ASSERT_EQ(3, gc_dst->size());
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(kShort, gc_dst->get_slice(i).to_string());
    }

    // Repeat long string 2 times
    gc_dst->append_value_multiple_times(*gc_src, 1, 2);
    ASSERT_EQ(5, gc_dst->size());
    ASSERT_EQ(kLong, gc_dst->get_slice(3).to_string());
    ASSERT_EQ(kLong, gc_dst->get_slice(4).to_string());
}

// ---- test_append_default ----
PARALLEL_TEST(GermanStringColumnTest, test_append_default) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append_default();
    ASSERT_EQ(1, gc->size());
    ASSERT_EQ("", gc->get_slice(0).to_string());
    ASSERT_TRUE(gc->get_german_string(0).is_inline());

    gc->append_default(5);
    ASSERT_EQ(6, gc->size());
    for (size_t i = 0; i < 6; ++i) {
        ASSERT_EQ(0, gc->get_slice(i).size);
    }
}

// ---- test_append_nulls_returns_false ----
PARALLEL_TEST(GermanStringColumnTest, test_append_nulls_returns_false) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());
    ASSERT_FALSE(gc->append_nulls(10));
    ASSERT_EQ(0, gc->size());
}

// ---- test_assign ----
PARALLEL_TEST(GermanStringColumnTest, test_assign) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("aaa"));
    gc->append(Slice(kLong));
    gc->append(Slice("ccc"));

    // Assign all elements to be copies of index 1 (long string)
    gc->assign(3, 1);
    ASSERT_EQ(3, gc->size());
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(kLong, gc->get_slice(i).to_string());
    }

    // Assign to short string
    gc->append(Slice("short"));
    gc->assign(2, 3);
    ASSERT_EQ(2, gc->size());
    for (int i = 0; i < 2; ++i) {
        ASSERT_EQ("short", gc->get_slice(i).to_string());
    }
}

// ---- test_remove_first_n_values ----
PARALLEL_TEST(GermanStringColumnTest, test_remove_first_n_values) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("a"));
    gc->append(Slice(kLong));
    gc->append(Slice("c"));
    gc->append(Slice("d"));

    gc->remove_first_n_values(2);
    ASSERT_EQ(2, gc->size());
    ASSERT_EQ("c", gc->get_slice(0).to_string());
    ASSERT_EQ("d", gc->get_slice(1).to_string());

    // Remove more than size: clears
    gc->remove_first_n_values(100);
    ASSERT_EQ(0, gc->size());
}

// ---- test_resize ----
PARALLEL_TEST(GermanStringColumnTest, test_resize) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("aaa"));
    gc->append(Slice("bbb"));
    gc->append(Slice("ccc"));

    gc->resize(1);
    ASSERT_EQ(1, gc->size());
    ASSERT_EQ("aaa", gc->get_slice(0).to_string());

    gc->resize(3);
    ASSERT_EQ(3, gc->size());
    ASSERT_EQ("aaa", gc->get_slice(0).to_string());
    // Resized elements are default-constructed (empty)
    ASSERT_EQ(0, gc->get_german_string(1).len);
    ASSERT_EQ(0, gc->get_german_string(2).len);
}

// ---- test_byte_size ----
PARALLEL_TEST(GermanStringColumnTest, test_byte_size) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Empty column
    ASSERT_EQ(0, gc->byte_size());

    gc->append(Slice(kShort));  // inline, 5 bytes
    gc->append(Slice(kLong));   // long, 35 bytes in arena

    // Total byte_size = num_elements * sizeof(GermanString) + arena_allocated
    size_t expected = 2 * sizeof(GermanString) + gc->arena_memory_usage();
    ASSERT_EQ(expected, gc->byte_size());

    // Per-element byte_size
    ASSERT_EQ(sizeof(GermanString), gc->byte_size(static_cast<size_t>(0)));  // inline: no extra
    ASSERT_EQ(sizeof(GermanString) + kLong.size(), gc->byte_size(static_cast<size_t>(1)));  // long: extra data
}

// ---- test_byte_size_range ----
PARALLEL_TEST(GermanStringColumnTest, test_byte_size_range) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("a"));     // inline
    gc->append(Slice(kLong));   // long
    gc->append(Slice("b"));     // inline

    // byte_size(from, size) with 2 args
    size_t range_size = gc->byte_size(0, 2);
    // 2 * sizeof(GermanString) + kLong.size() (the long one at index 1)
    ASSERT_EQ(2 * sizeof(GermanString) + kLong.size(), range_size);
}

// ---- test_debug_string ----
PARALLEL_TEST(GermanStringColumnTest, test_debug_string) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("abc"));
    gc->append(Slice("def"));

    std::string dbg = gc->debug_string();
    ASSERT_NE(std::string::npos, dbg.find("abc"));
    ASSERT_NE(std::string::npos, dbg.find("def"));
}

// ---- test_get_datum ----
PARALLEL_TEST(GermanStringColumnTest, test_get_datum) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    Datum d0 = gc->get(0);
    ASSERT_EQ(kShort, std::string(d0.get_german_string().get_data(), d0.get_german_string().len));

    Datum d1 = gc->get(1);
    ASSERT_EQ(kLong, std::string(d1.get_german_string().get_data(), d1.get_german_string().len));
}

// ---- test_update_rows ----
PARALLEL_TEST(GermanStringColumnTest, test_update_rows) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("aaa"));
    gc->append(Slice("bbb"));
    gc->append(Slice("ccc"));
    gc->append(Slice("ddd"));

    auto src = GermanStringColumn::create();
    auto* gc_src = down_cast<GermanStringColumn*>(src.get());
    gc_src->append(Slice(kLong));
    gc_src->append(Slice("zzz"));

    std::vector<uint32_t> indexes = {1, 3};
    gc->update_rows(*gc_src, indexes.data());

    ASSERT_EQ(4, gc->size());
    ASSERT_EQ("aaa", gc->get_slice(0).to_string());
    ASSERT_EQ(kLong, gc->get_slice(1).to_string());
    ASSERT_EQ("ccc", gc->get_slice(2).to_string());
    ASSERT_EQ("zzz", gc->get_slice(3).to_string());
}

// ---- test_fill_default ----
PARALLEL_TEST(GermanStringColumnTest, test_fill_default) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("aaa"));
    gc->append(Slice(kLong));
    gc->append(Slice("ccc"));

    Filter filter = {0, 1, 0};  // fill index 1 with default
    gc->fill_default(filter);

    ASSERT_EQ(3, gc->size());
    ASSERT_EQ("aaa", gc->get_slice(0).to_string());
    ASSERT_EQ("", gc->get_slice(1).to_string());  // filled with default
    ASSERT_EQ("ccc", gc->get_slice(2).to_string());
}

// ---- test_container_memory_usage ----
PARALLEL_TEST(GermanStringColumnTest, test_container_memory_usage) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kLong));
    gc->append(Slice(kLong2));

    size_t usage = gc->container_memory_usage();
    ASSERT_GT(usage, 0);
    // container_memory_usage = capacity * sizeof(GermanString) + arena
    ASSERT_GE(usage, gc->capacity() * sizeof(GermanString) + gc->arena_memory_usage());
}

// ---- test_capacity_limit_reached ----
PARALLEL_TEST(GermanStringColumnTest, test_capacity_limit_reached) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Small column: no limit reached
    gc->append(Slice("test"));
    ASSERT_TRUE(gc->capacity_limit_reached().ok());
}

// ---- test_immutable_container ----
PARALLEL_TEST(GermanStringColumnTest, test_immutable_container) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    auto imm = gc->immutable_data();
    ASSERT_EQ(2, imm.size());

    GermanString gs0 = imm[0];
    ASSERT_EQ(kShort, std::string(gs0.get_data(), gs0.len));

    GermanString gs1 = imm[1];
    ASSERT_EQ(kLong, std::string(gs1.get_data(), gs1.len));
}

// ---- test_immutable_container_empty ----
PARALLEL_TEST(GermanStringColumnTest, test_immutable_container_empty) {
    GermanStringImmContainer empty_imm;
    ASSERT_EQ(0, empty_imm.size());
}

// ---- test_serialize_batch ----
PARALLEL_TEST(GermanStringColumnTest, test_serialize_batch) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("abc"));
    gc->append(Slice(kLong));

    uint32_t max_ser = gc->max_one_element_serialize_size();
    size_t chunk_size = gc->size();

    // Allocate buffer: chunk_size * max_ser
    std::vector<uint8_t> dst(chunk_size * max_ser, 0);
    Buffer<uint32_t> slice_sizes(chunk_size, 0);

    gc->serialize_batch(dst.data(), slice_sizes, chunk_size, max_ser);

    // Deserialize and verify
    auto col2 = GermanStringColumn::create();
    auto* gc2 = down_cast<GermanStringColumn*>(col2.get());

    for (size_t i = 0; i < chunk_size; ++i) {
        gc2->deserialize_and_append(dst.data() + i * max_ser);
    }

    ASSERT_EQ(gc->size(), gc2->size());
    for (size_t i = 0; i < gc->size(); ++i) {
        ASSERT_EQ(gc->get_slice(i).to_string(), gc2->get_slice(i).to_string());
    }
}

// ---- test_check_or_die ----
PARALLEL_TEST(GermanStringColumnTest, test_check_or_die) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice(kShort));
    gc->append(Slice(kLong));
    gc->append(Slice(""));

    // Should not crash
    gc->check_or_die();
}

// ---- test_live_arena_bytes ----
PARALLEL_TEST(GermanStringColumnTest, test_live_arena_bytes) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Only inline strings: live_arena_bytes should be 0
    gc->append(Slice(kShort));
    gc->append(Slice(""));
    ASSERT_EQ(0, gc->live_arena_bytes());

    // Add a long string
    gc->append(Slice(kLong));
    ASSERT_EQ(kLong.size(), gc->live_arena_bytes());

    gc->append(Slice(kLong2));
    ASSERT_EQ(kLong.size() + kLong2.size(), gc->live_arena_bytes());
}

// ---- test_move_constructor ----
PARALLEL_TEST(GermanStringColumnTest, test_move_constructor) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());
    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    GermanStringColumn moved(std::move(*gc));

    ASSERT_EQ(2, moved.size());
    ASSERT_EQ(kShort, moved.get_slice(0).to_string());
    ASSERT_EQ(kLong, moved.get_slice(1).to_string());
}

// ---- test_move_assignment ----
PARALLEL_TEST(GermanStringColumnTest, test_move_assignment) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());
    gc->append(Slice(kShort));
    gc->append(Slice(kLong));

    GermanStringColumn moved;
    moved = std::move(*gc);

    ASSERT_EQ(2, moved.size());
    ASSERT_EQ(kShort, moved.get_slice(0).to_string());
    ASSERT_EQ(kLong, moved.get_slice(1).to_string());
}

// ---- test_filter_range_partial ----
PARALLEL_TEST(GermanStringColumnTest, test_filter_range_partial) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Append: "a", "b", "c", "d", "e"
    gc->append(Slice("a"));
    gc->append(Slice("b"));
    gc->append(Slice("c"));
    gc->append(Slice("d"));
    gc->append(Slice("e"));

    // Filter range [1, 4): keep "b" and "d" (indices 1 and 3)
    Filter filter = {0, 1, 0, 1, 0};
    gc->filter_range(filter, 1, 4);

    // Expected: "a" (untouched, before range), "b", "d" (kept), "e" (appended from after range)
    ASSERT_EQ(4, gc->size());
    ASSERT_EQ("a", gc->get_slice(0).to_string());
    ASSERT_EQ("b", gc->get_slice(1).to_string());
    ASSERT_EQ("d", gc->get_slice(2).to_string());
    ASSERT_EQ("e", gc->get_slice(3).to_string());
}

// ---- test_append_german_string_object ----
PARALLEL_TEST(GermanStringColumnTest, test_append_german_string_object) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    // Append a short GermanString
    GermanString gs_short(kShort.data(), kShort.size());
    gc->append(gs_short);
    ASSERT_EQ(kShort, gc->get_slice(0).to_string());

    // Append a long GermanString that has external pointer (from another column's arena)
    auto tmp = GermanStringColumn::create();
    auto* tmp_gc = down_cast<GermanStringColumn*>(tmp.get());
    tmp_gc->append(Slice(kLong));

    const GermanString& gs_long = tmp_gc->get_german_string(0);
    gc->append(gs_long);

    ASSERT_EQ(2, gc->size());
    ASSERT_EQ(kLong, gc->get_slice(1).to_string());

    // Data should be independent: modifying tmp should not affect gc
    tmp_gc->reset_column();
    ASSERT_EQ(kLong, gc->get_slice(1).to_string());
}

// ---- Serde round-trip tests (ColumnArraySerde) ----

// Basic round-trip: mixed short/long/empty strings.
PARALLEL_TEST(GermanStringColumnTest, test_serde_roundtrip_basic) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("short"));
    gc->append(Slice(""));
    gc->append(Slice("this is a long string exceeding 12 bytes"));
    gc->append(Slice("x"));
    gc->append(Slice(kShortMax));
    gc->append(Slice(kLongExact13));

    // Serialize
    auto size = serde::ColumnArraySerde::max_serialized_size(*gc);
    ASSERT_GT(size, 0);
    std::vector<uint8_t> buffer(size);
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, serde::ColumnArraySerde::serialize(*gc, buffer.data()));
    ASSERT_EQ(end, p1);

    // Deserialize into new GermanStringColumn
    auto col2 = GermanStringColumn::create();
    ASSIGN_OR_ABORT(auto p2, serde::ColumnArraySerde::deserialize(buffer.data(), end, col2.get()));
    ASSERT_EQ(end, p2);

    // Verify
    ASSERT_EQ(gc->size(), col2->size());
    for (size_t i = 0; i < gc->size(); i++) {
        ASSERT_EQ(gc->get_slice(i).to_string(), col2->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// Wire format should be identical to BinaryColumn.
PARALLEL_TEST(GermanStringColumnTest, test_serde_wire_format_compatible) {
    auto gs_col = GermanStringColumn::create();
    auto bin_col = BinaryColumn::create();

    std::vector<Slice> strings = {Slice("abc"), Slice(""), Slice("long string here!!!"),
                                  Slice(kShortMax), Slice(kLong)};
    for (auto& s : strings) {
        down_cast<GermanStringColumn*>(gs_col.get())->append(s);
        bin_col->append(s);
    }

    // Max serialized size should be the same
    auto gs_size = serde::ColumnArraySerde::max_serialized_size(*gs_col);
    auto bin_size = serde::ColumnArraySerde::max_serialized_size(*bin_col);
    ASSERT_EQ(gs_size, bin_size);

    // Serialize both
    std::vector<uint8_t> gs_buf(gs_size), bin_buf(bin_size);
    ASSIGN_OR_ABORT(auto gs_end, serde::ColumnArraySerde::serialize(*gs_col, gs_buf.data()));
    ASSIGN_OR_ABORT(auto bin_end, serde::ColumnArraySerde::serialize(*bin_col, bin_buf.data()));

    // Both should consume the full buffer
    ASSERT_EQ(gs_end, gs_buf.data() + gs_buf.size());
    ASSERT_EQ(bin_end, bin_buf.data() + bin_buf.size());

    // Wire format should be byte-identical
    ASSERT_EQ(gs_buf, bin_buf);
}

// Cross-deserialization: BinaryColumn wire bytes into GermanStringColumn and vice versa.
PARALLEL_TEST(GermanStringColumnTest, test_serde_cross_deserialize) {
    std::vector<Slice> strings = {Slice("hi"), Slice(kLong), Slice(""), Slice(kShortMax)};

    // Serialize from BinaryColumn
    auto bin_col = BinaryColumn::create();
    for (auto& s : strings) {
        bin_col->append(s);
    }
    auto buf_size = serde::ColumnArraySerde::max_serialized_size(*bin_col);
    std::vector<uint8_t> buffer(buf_size);
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, serde::ColumnArraySerde::serialize(*bin_col, buffer.data()));
    ASSERT_EQ(end, p1);

    // Deserialize into GermanStringColumn
    auto gs_col = GermanStringColumn::create();
    ASSIGN_OR_ABORT(auto p2, serde::ColumnArraySerde::deserialize(buffer.data(), end, gs_col.get()));
    ASSERT_EQ(end, p2);

    ASSERT_EQ(strings.size(), gs_col->size());
    for (size_t i = 0; i < strings.size(); i++) {
        ASSERT_EQ(strings[i].to_string(), gs_col->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// Round-trip an empty column.
PARALLEL_TEST(GermanStringColumnTest, test_serde_empty_column) {
    auto col = GermanStringColumn::create();

    auto size = serde::ColumnArraySerde::max_serialized_size(*col);
    ASSERT_GT(size, 0);  // Even empty columns have some header overhead
    std::vector<uint8_t> buffer(size);
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, serde::ColumnArraySerde::serialize(*col, buffer.data()));
    ASSERT_EQ(end, p1);

    auto col2 = GermanStringColumn::create();
    ASSIGN_OR_ABORT(auto p2, serde::ColumnArraySerde::deserialize(buffer.data(), end, col2.get()));
    ASSERT_EQ(end, p2);
    ASSERT_EQ(0, col2->size());
}

// Round-trip with 10K+ rows of mixed strings.
PARALLEL_TEST(GermanStringColumnTest, test_serde_large_column) {
    const size_t N = 10000;
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    std::vector<std::string> expected;
    expected.reserve(N);
    for (size_t i = 0; i < N; i++) {
        std::string s;
        if (i % 4 == 0) {
            s = "";  // empty
        } else if (i % 4 == 1) {
            s = std::to_string(i);  // short
        } else if (i % 4 == 2) {
            s = "long_prefix_for_serde_test_" + std::to_string(i) + "_suffix_data";  // long
        } else {
            s = "123456789012";  // exactly 12 bytes (inline boundary)
        }
        expected.push_back(s);
        gc->append(Slice(s));
    }
    ASSERT_EQ(N, gc->size());

    // Serialize
    auto size = serde::ColumnArraySerde::max_serialized_size(*gc);
    std::vector<uint8_t> buffer(size);
    const auto* end = buffer.data() + buffer.size();
    ASSIGN_OR_ABORT(auto p1, serde::ColumnArraySerde::serialize(*gc, buffer.data()));
    ASSERT_EQ(end, p1);

    // Deserialize
    auto col2 = GermanStringColumn::create();
    ASSIGN_OR_ABORT(auto p2, serde::ColumnArraySerde::deserialize(buffer.data(), end, col2.get()));
    ASSERT_EQ(end, p2);

    ASSERT_EQ(N, col2->size());
    for (size_t i = 0; i < N; i++) {
        ASSERT_EQ(expected[i], col2->get_slice(i).to_string()) << "mismatch at " << i;
    }
}

// Round-trip with encode levels (compression).
PARALLEL_TEST(GermanStringColumnTest, test_serde_encode_levels) {
    auto col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(col.get());

    gc->append(Slice("abc"));
    gc->append(Slice(kLong));
    gc->append(Slice(""));
    gc->append(Slice(kLong2));

    for (int level = -1; level < 8; ++level) {
        auto size = serde::ColumnArraySerde::max_serialized_size(*gc, level);
        std::vector<uint8_t> buffer(size);
        const auto* end = buffer.data() + buffer.size();
        ASSERT_OK(serde::ColumnArraySerde::serialize(*gc, buffer.data(), false, level));
        auto col2 = GermanStringColumn::create();
        ASSERT_OK(serde::ColumnArraySerde::deserialize(buffer.data(), end, col2.get(), false, level));

        ASSERT_EQ(gc->size(), col2->size()) << "encode_level=" << level;
        for (size_t i = 0; i < gc->size(); i++) {
            ASSERT_EQ(gc->get_slice(i).to_string(), col2->get_slice(i).to_string())
                    << "mismatch at " << i << " encode_level=" << level;
        }
    }
}

// ---- test_hash_consistency_with_binary ----
// Verifies that column hash visitor produces identical hash values for
// GermanStringColumn and BinaryColumn containing the same string data.
// This is critical for hash joins where one side is STRING_V2 and the other is STRING.
PARALLEL_TEST(GermanStringColumnTest, test_hash_consistency_with_binary) {
    auto gs_col = GermanStringColumn::create();
    auto* gc = down_cast<GermanStringColumn*>(gs_col.get());
    auto bin_col = BinaryColumn::create();

    std::vector<std::string> strings = {
            "",                                       // empty string
            "abc",                                    // short (3 bytes)
            "123456789012",                           // exactly 12 bytes (inline boundary)
            "this is longer than twelve bytes",        // long string (>12 bytes)
            "1234567890123",                           // exactly 13 bytes (first non-inline)
            "another fairly long test string here!",   // another long string
    };

    for (const auto& s : strings) {
        gc->append(Slice(s));
        bin_col->append(Slice(s));
    }

    const uint32_t n = strings.size();
    const uint32_t seed = 0x811C9DC5;  // typical FNV seed

    // Test CRC32 hash consistency
    {
        std::vector<uint32_t> gs_hashes(n, seed);
        std::vector<uint32_t> bin_hashes(n, seed);
        crc32_hash_column(*gs_col, gs_hashes.data(), 0, n);
        crc32_hash_column(*bin_col, bin_hashes.data(), 0, n);
        for (uint32_t i = 0; i < n; ++i) {
            ASSERT_EQ(gs_hashes[i], bin_hashes[i])
                    << "CRC32 hash mismatch at index " << i << " for string \"" << strings[i] << "\"";
        }
    }

    // Test FNV hash consistency
    {
        std::vector<uint32_t> gs_hashes(n, seed);
        std::vector<uint32_t> bin_hashes(n, seed);
        fnv_hash_column(*gs_col, gs_hashes.data(), 0, n);
        fnv_hash_column(*bin_col, bin_hashes.data(), 0, n);
        for (uint32_t i = 0; i < n; ++i) {
            ASSERT_EQ(gs_hashes[i], bin_hashes[i])
                    << "FNV hash mismatch at index " << i << " for string \"" << strings[i] << "\"";
        }
    }

    // Test XXHash3 consistency
    {
        std::vector<uint32_t> gs_hashes(n, seed);
        std::vector<uint32_t> bin_hashes(n, seed);
        xxh3_64_column(*gs_col, gs_hashes.data(), 0, n);
        xxh3_64_column(*bin_col, bin_hashes.data(), 0, n);
        for (uint32_t i = 0; i < n; ++i) {
            ASSERT_EQ(gs_hashes[i], bin_hashes[i])
                    << "XXHash3 hash mismatch at index " << i << " for string \"" << strings[i] << "\"";
        }
    }

    // Test with selective hash (subset of indices)
    {
        std::vector<uint16_t> sel = {0, 2, 4};  // empty, 12-byte, 13-byte
        std::vector<uint32_t> gs_hashes(n, seed);
        std::vector<uint32_t> bin_hashes(n, seed);
        crc32_hash_column_selective(*gs_col, gs_hashes.data(), sel.data(), sel.size());
        crc32_hash_column_selective(*bin_col, bin_hashes.data(), sel.data(), sel.size());
        for (auto idx : sel) {
            ASSERT_EQ(gs_hashes[idx], bin_hashes[idx])
                    << "CRC32 selective hash mismatch at index " << idx << " for string \"" << strings[idx] << "\"";
        }
    }

    // Test with selection bitmap
    {
        std::vector<uint8_t> selection = {1, 0, 1, 1, 0, 1};  // select indices 0,2,3,5
        std::vector<uint32_t> gs_hashes(n, seed);
        std::vector<uint32_t> bin_hashes(n, seed);
        crc32_hash_column_with_selection(*gs_col, gs_hashes.data(), selection.data(), 0, n);
        crc32_hash_column_with_selection(*bin_col, bin_hashes.data(), selection.data(), 0, n);
        for (uint32_t i = 0; i < n; ++i) {
            if (selection[i]) {
                ASSERT_EQ(gs_hashes[i], bin_hashes[i])
                        << "CRC32 selection hash mismatch at index " << i << " for string \"" << strings[i] << "\"";
            }
        }
    }
}

} // namespace starrocks
