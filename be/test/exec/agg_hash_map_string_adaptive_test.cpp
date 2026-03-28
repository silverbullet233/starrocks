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

#include "exec/aggregate/string_adaptive_hash_map.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "runtime/mem_pool.h"

namespace starrocks {

// ============================================================================
// Key conversion tests
// ============================================================================

TEST(SAHAKeyTest, SliceToS0Key) {
    // len=0
    Slice empty("", 0);
    EXPECT_EQ(slice_to_s0_key(empty), 0);

    // len=1
    Slice one("A", 1);
    EXPECT_EQ(slice_to_s0_key(one), 'A');

    // len=2
    Slice two("AB", 2);
    EXPECT_EQ(slice_to_s0_key(two), ('A' << 8) | 'B');

    // Different 2-byte strings produce different keys
    Slice two2("BA", 2);
    EXPECT_NE(slice_to_s0_key(two), slice_to_s0_key(two2));
}

TEST(SAHAKeyTest, SliceToKey8) {
    // len=3
    Slice s3("abc", 3);
    Key8 k3 = slice_to_key8(s3);
    EXPECT_NE(k3, 0);

    // len=8
    Slice s8("abcdefgh", 8);
    Key8 k8 = slice_to_key8(s8);
    EXPECT_NE(k8, 0);

    // Different strings produce different keys
    Slice s3b("abd", 3);
    EXPECT_NE(slice_to_key8(s3), slice_to_key8(s3b));

    // Same content different length produces different keys (length encoded in top byte)
    Slice s4("abc\0", 4);
    EXPECT_NE(slice_to_key8(s3), slice_to_key8(s4));
}

TEST(SAHAKeyTest, SliceToKey16) {
    Slice s9("abcdefghi", 9);
    Key16 k9 = slice_to_key16(s9);
    EXPECT_NE(k9, 0);

    Slice s16("abcdefghijklmnop", 16);
    Key16 k16 = slice_to_key16(s16);
    EXPECT_NE(k16, 0);

    // Different strings produce different keys
    Slice s9b("abcdefghj", 9);
    EXPECT_NE(slice_to_key16(s9), slice_to_key16(s9b));
}

TEST(SAHAKeyTest, SliceToKey24) {
    Slice s17("abcdefghijklmnopq", 17);
    Key24 k17 = slice_to_key24(s17);
    EXPECT_NE(k17, (Key24{}));

    Slice s24("abcdefghijklmnopqrstuvwx", 24);
    Key24 k24 = slice_to_key24(s24);
    EXPECT_NE(k24, (Key24{}));

    // Different strings produce different keys
    Slice s17b("abcdefghijklmnopR", 17);
    EXPECT_NE(slice_to_key24(s17), slice_to_key24(s17b));
}

TEST(SAHAKeyTest, BoundaryLengths) {
    // Test that keys at boundary lengths are properly dispatched
    // len=2 -> S0, len=3 -> S1
    Slice s2("ab", 2);
    Slice s3("abc", 3);
    EXPECT_EQ(slice_to_s0_key(s2), ('a' << 8) | 'b');
    Key8 k3 = slice_to_key8(s3);
    EXPECT_NE(k3, 0);

    // len=8 -> S1, len=9 -> S2
    Slice s8("12345678", 8);
    Slice s9("123456789", 9);
    Key8 k8 = slice_to_key8(s8);
    Key16 k9 = slice_to_key16(s9);
    EXPECT_NE(k8, 0);
    EXPECT_NE(k9, 0);

    // len=16 -> S2, len=17 -> S3
    Slice s16("1234567890123456", 16);
    Slice s17("12345678901234567", 17);
    Key16 k16 = slice_to_key16(s16);
    Key24 k17 = slice_to_key24(s17);
    EXPECT_NE(k16, 0);
    EXPECT_NE(k17, (Key24{}));
}

// ============================================================================
// Hash functor tests
// ============================================================================

TEST(SAHAKeyTest, Key24HashDeterministic) {
    Key24Hash<PhmapSeed1> hasher;
    Slice s("abcdefghijklmnopqrst", 20);
    Key24 k = slice_to_key24(s);
    size_t h1 = hasher(k);
    size_t h2 = hasher(k);
    EXPECT_EQ(h1, h2);
}

TEST(SAHAKeyTest, Key24HashDistribution) {
    Key24Hash<PhmapSeed1> hasher;
    std::unordered_set<size_t> hashes;
    for (int i = 0; i < 1000; i++) {
        std::string s = "abcdefghijklmnopqrst" + std::to_string(i);
        s.resize(24, 'x');
        Key24 k = slice_to_key24(Slice(s.data(), s.size()));
        hashes.insert(hasher(k));
    }
    // Expect reasonable distribution (at least 900 unique hashes out of 1000)
    EXPECT_GT(hashes.size(), 900);
}

// ============================================================================
// SAHAMultiMap standalone API tests
// ============================================================================

TEST(SAHAMultiMapTest, BasicSizeAndCapacity) {
    SAHAMultiMap<PhmapSeed1> map;
    EXPECT_EQ(map.size(), 0);
    EXPECT_EQ(map.bucket_count(), 0);
    map.clear();
    EXPECT_EQ(map.size(), 0);
}

TEST(SAHAMultiMapTest, EmplaceAndFind) {
    SAHAMultiMap<PhmapSeed1> map;
    AggDataPtr dummy1 = reinterpret_cast<AggDataPtr>(0x1);
    AggDataPtr dummy2 = reinterpret_cast<AggDataPtr>(0x2);

    // Emplace new key
    auto [vp1, ins1] = map.emplace(Slice("hello", 5));
    EXPECT_TRUE(ins1);
    *vp1 = dummy1;

    // Emplace same key - should not insert
    auto [vp2, ins2] = map.emplace(Slice("hello", 5));
    EXPECT_FALSE(ins2);
    EXPECT_EQ(*vp2, dummy1);

    // Find existing key
    auto* found = map.find(Slice("hello", 5));
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(*found, dummy1);

    // Find non-existing key
    EXPECT_EQ(map.find(Slice("world", 5)), nullptr);

    EXPECT_EQ(map.size(), 1);
}

TEST(SAHAMultiMapTest, LazyEmplace) {
    SAHAMultiMap<PhmapSeed1> map;
    AggDataPtr d1 = reinterpret_cast<AggDataPtr>(0x10);
    AggDataPtr d2 = reinterpret_cast<AggDataPtr>(0x20);

    int call_count = 0;
    auto* vp1 = map.lazy_emplace(Slice("abc", 3), [&](AggDataPtr& val) {
        call_count++;
        val = d1;
    });
    EXPECT_EQ(call_count, 1);
    EXPECT_EQ(*vp1, d1);

    // lazy_emplace again - callback should NOT be called
    auto* vp2 = map.lazy_emplace(Slice("abc", 3), [&](AggDataPtr& val) {
        call_count++;
        val = d2;
    });
    EXPECT_EQ(call_count, 1); // still 1
    EXPECT_EQ(*vp2, d1);     // still d1
}

TEST(SAHAMultiMapTest, AllSubTables) {
    SAHAMultiMap<PhmapSeed1> map;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    // S0: len 0-2
    map.lazy_emplace(Slice("", 0), [&](AggDataPtr& v) { v = dummy; });
    map.lazy_emplace(Slice("a", 1), [&](AggDataPtr& v) { v = dummy; });
    map.lazy_emplace(Slice("ab", 2), [&](AggDataPtr& v) { v = dummy; });

    // S1: len 3-8
    map.lazy_emplace(Slice("abc", 3), [&](AggDataPtr& v) { v = dummy; });
    map.lazy_emplace(Slice("abcdefgh", 8), [&](AggDataPtr& v) { v = dummy; });

    // S2: len 9-16
    map.lazy_emplace(Slice("123456789", 9), [&](AggDataPtr& v) { v = dummy; });
    std::string s16(16, 'x');
    map.lazy_emplace(Slice(s16), [&](AggDataPtr& v) { v = dummy; });

    // S3: len 17-24
    std::string s17(17, 'y');
    map.lazy_emplace(Slice(s17), [&](AggDataPtr& v) { v = dummy; });
    std::string s24(24, 'z');
    map.lazy_emplace(Slice(s24), [&](AggDataPtr& v) { v = dummy; });

    // L: len > 24
    std::string s30(30, 'w');
    map.lazy_emplace(Slice(s30), [&](AggDataPtr& v) { v = dummy; });

    EXPECT_EQ(map.size(), 10);

    // Verify all findable
    EXPECT_NE(map.find(Slice("", 0)), nullptr);
    EXPECT_NE(map.find(Slice("a", 1)), nullptr);
    EXPECT_NE(map.find(Slice("ab", 2)), nullptr);
    EXPECT_NE(map.find(Slice("abc", 3)), nullptr);
    EXPECT_NE(map.find(Slice("abcdefgh", 8)), nullptr);
    EXPECT_NE(map.find(Slice("123456789", 9)), nullptr);
    EXPECT_NE(map.find(Slice(s16)), nullptr);
    EXPECT_NE(map.find(Slice(s17)), nullptr);
    EXPECT_NE(map.find(Slice(s24)), nullptr);
    EXPECT_NE(map.find(Slice(s30)), nullptr);

    // Non-existing
    EXPECT_EQ(map.find(Slice("nope", 4)), nullptr);
}

TEST(SAHAMultiMapTest, ForEachValue) {
    SAHAMultiMap<PhmapSeed1> map;
    for (int i = 1; i <= 100; i++) {
        std::string s(i % 30 + 1, 'a' + (i % 26));
        map.lazy_emplace(Slice(s), [&](AggDataPtr& v) { v = reinterpret_cast<AggDataPtr>(i); });
    }

    size_t count = 0;
    map.for_each_value([&](AggDataPtr& v) {
        EXPECT_NE(v, nullptr);
        count++;
    });
    EXPECT_EQ(count, map.size());
}

TEST(SAHAMultiMapTest, HighCardinality) {
    SAHAMultiMap<PhmapSeed1> map;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    std::vector<std::string> keys;
    for (int i = 0; i < 50000; i++) {
        keys.push_back("key_" + std::to_string(i) + "_" + std::string(i % 30, 'x'));
    }
    for (auto& k : keys) {
        map.lazy_emplace(Slice(k), [&](AggDataPtr& v) { v = dummy; });
    }

    EXPECT_EQ(map.size(), 50000);

    // Verify all findable
    for (auto& k : keys) {
        EXPECT_NE(map.find(Slice(k)), nullptr);
    }
}

// ============================================================================
// Full aggregation hash map tests
// ============================================================================

class StringAdaptiveHashMapTest : public ::testing::Test {
protected:
    void SetUp() override { pool_ = std::make_unique<MemPool>(); }
    void TearDown() override { pool_.reset(); }

    std::unique_ptr<MemPool> pool_;

    // Simple allocator that just stores the Slice at the allocated location
    struct SimpleAllocator {
        MemPool* pool;
        std::vector<AggDataPtr> allocated;

        AggDataPtr operator()(const Slice& key) {
            auto* ptr = pool->allocate(sizeof(Slice) + sizeof(int64_t));
            *reinterpret_cast<Slice*>(ptr) = key;
            *reinterpret_cast<int64_t*>(ptr + sizeof(Slice)) = 1;
            allocated.push_back(ptr);
            return ptr;
        }

        AggDataPtr operator()(std::nullptr_t) {
            auto* ptr = pool->allocate(sizeof(Slice) + sizeof(int64_t));
            *reinterpret_cast<int64_t*>(ptr + sizeof(Slice)) = 1;
            allocated.push_back(ptr);
            return ptr;
        }
    };

    ColumnPtr make_string_column(const std::vector<std::string>& strings) {
        auto column = BinaryColumn::create();
        for (const auto& s : strings) {
            column->append(Slice(s));
        }
        return column;
    }

    ColumnPtr make_nullable_string_column(const std::vector<std::string>& strings,
                                          const std::vector<bool>& nulls) {
        auto data_column = BinaryColumn::create();
        auto null_column = NullColumn::create();
        for (size_t i = 0; i < strings.size(); i++) {
            data_column->append(Slice(strings[i]));
            null_column->append(nulls[i] ? 1 : 0);
        }
        return NullableColumn::create(std::move(data_column), std::move(null_column));
    }
};

TEST_F(StringAdaptiveHashMapTest, EmptyStrings) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    auto column = make_string_column({"", "", "a", ""});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    // "" appears 3 times, "a" appears 1 time -> 2 distinct keys
    EXPECT_EQ(hash_map.hash_map.size(), 2);
    // All empty strings should map to the same state
    EXPECT_EQ(agg_states[0], agg_states[1]);
    EXPECT_EQ(agg_states[0], agg_states[3]);
    EXPECT_NE(agg_states[0], agg_states[2]);
}

TEST_F(StringAdaptiveHashMapTest, ShortStrings_S0) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    auto column = make_string_column({"a", "b", "ab", "a", "b", "ab"});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 3); // "a", "b", "ab"
    EXPECT_EQ(agg_states[0], agg_states[3]); // "a" == "a"
    EXPECT_EQ(agg_states[1], agg_states[4]); // "b" == "b"
    EXPECT_EQ(agg_states[2], agg_states[5]); // "ab" == "ab"
}

TEST_F(StringAdaptiveHashMapTest, MediumStrings_S1) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    auto column = make_string_column({"abc", "defgh", "abcdefgh", "abc", "defgh"});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 3);
    EXPECT_EQ(agg_states[0], agg_states[3]); // "abc" == "abc"
    EXPECT_EQ(agg_states[1], agg_states[4]); // "defgh" == "defgh"
}

TEST_F(StringAdaptiveHashMapTest, MediumStrings_S2) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    auto column = make_string_column({"123456789", "1234567890123456", "123456789", "abcdefghij"});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 3);
    EXPECT_EQ(agg_states[0], agg_states[2]); // "123456789" == "123456789"
}

TEST_F(StringAdaptiveHashMapTest, MediumStrings_S3) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    std::string s17(17, 'x');
    std::string s24(24, 'y');
    auto column = make_string_column({s17, s24, s17, s24});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 2);
    EXPECT_EQ(agg_states[0], agg_states[2]);
    EXPECT_EQ(agg_states[1], agg_states[3]);
}

TEST_F(StringAdaptiveHashMapTest, LongStrings) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    std::string s30(30, 'a');
    std::string s50(50, 'b');
    auto column = make_string_column({s30, s50, s30});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 2);
    EXPECT_EQ(agg_states[0], agg_states[2]);
}

TEST_F(StringAdaptiveHashMapTest, MixedLengths) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    std::string s1("x");
    std::string s5("hello");
    std::string s12("hello world!");
    std::string s20("12345678901234567890");
    std::string s30(30, 'z');

    auto column = make_string_column({s1, s5, s12, s20, s30, s1, s5, s12, s20, s30});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 5);
    // Verify each duplicate maps to the same state
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(agg_states[i], agg_states[i + 5]);
    }
}

TEST_F(StringAdaptiveHashMapTest, NullableStrings) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, true>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    auto column = make_nullable_string_column({"abc", "", "def", "", "abc"}, {false, true, false, true, false});
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 2); // "abc", "def" (nulls share null_key_data)
    EXPECT_NE(hash_map.null_key_data, nullptr);
    EXPECT_EQ(agg_states[1], agg_states[3]); // Both nulls
    EXPECT_EQ(agg_states[0], agg_states[4]); // Both "abc"
}

TEST_F(StringAdaptiveHashMapTest, FindNotFound) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    // First, insert some keys
    auto column1 = make_string_column({"abc", "hello world!"});
    size_t chunk_size1 = column1->size();
    Buffer<AggDataPtr> agg_states1(chunk_size1);
    ExtraAggParam extra1;
    Columns key_columns1 = {column1};
    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size1, key_columns1, pool_.get(), allocator, &agg_states1, &extra1);

    // Now probe with some existing and non-existing keys
    auto column2 = make_string_column({"abc", "xyz", "hello world!", "not found"});
    size_t chunk_size2 = column2->size();
    Buffer<AggDataPtr> agg_states2(chunk_size2);
    Filter not_founds;
    ExtraAggParam extra2;
    extra2.not_founds = &not_founds;
    not_founds.assign(chunk_size2, 0);
    Columns key_columns2 = {column2};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<false, true, false>>(
            chunk_size2, key_columns2, pool_.get(), allocator, &agg_states2, &extra2);

    EXPECT_EQ(not_founds[0], 0); // "abc" found
    EXPECT_EQ(not_founds[1], 1); // "xyz" not found
    EXPECT_EQ(not_founds[2], 0); // "hello world!" found
    EXPECT_EQ(not_founds[3], 1); // "not found" not found
}

TEST_F(StringAdaptiveHashMapTest, HighCardinality) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    // Generate 10000 distinct strings of various lengths
    std::vector<std::string> strings;
    for (int i = 0; i < 10000; i++) {
        strings.push_back("key_" + std::to_string(i) + "_" + std::string(i % 30, 'x'));
    }

    auto column = make_string_column(strings);
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    EXPECT_EQ(hash_map.hash_map.size(), 10000);

    // Verify all states are distinct
    std::unordered_set<AggDataPtr> unique_states(agg_states.begin(), agg_states.end());
    EXPECT_EQ(unique_states.size(), 10000);
}

TEST_F(StringAdaptiveHashMapTest, InsertKeysToColumns) {
    using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
    HashMap hash_map(4096, nullptr);
    SimpleAllocator allocator{pool_.get(), {}};

    std::vector<std::string> input = {"a", "hello", "1234567890", "12345678901234567890", std::string(30, 'z')};
    auto column = make_string_column(input);
    size_t chunk_size = column->size();
    Buffer<AggDataPtr> agg_states(chunk_size);
    ExtraAggParam extra;
    Columns key_columns = {column};

    hash_map.template compute_agg_states<SimpleAllocator, HTBuildOp<true, false, false>>(
            chunk_size, key_columns, pool_.get(), allocator, &agg_states, &extra);

    // Now test insert_keys_to_columns
    // Populate results from agg_states (each state has the Slice at the beginning)
    hash_map.results.resize(chunk_size);
    for (size_t i = 0; i < chunk_size; i++) {
        hash_map.results[i] = *reinterpret_cast<Slice*>(agg_states[i]);
    }

    auto output_column = BinaryColumn::create();
    MutableColumns output_columns;
    output_columns.push_back(std::move(output_column));

    hash_map.insert_keys_to_columns(hash_map.results, output_columns, chunk_size);

    auto* result_col = down_cast<BinaryColumn*>(output_columns[0].get());
    EXPECT_EQ(result_col->size(), chunk_size);
    for (size_t i = 0; i < chunk_size; i++) {
        EXPECT_EQ(result_col->get_slice(i).to_string(), input[i]);
    }
}

} // namespace starrocks
