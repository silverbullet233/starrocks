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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/german_string.h"
#include "column/german_string_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "runtime/mem_pool.h"

namespace starrocks {
namespace {

// A trivial allocator that satisfies AggHashMapWithKey's AllocFunc concept:
// it must accept both the key type and std::nullptr_t and return an AggDataPtr.
struct TestAllocator {
    std::vector<uint64_t>* storage;
    size_t next = 0;
    AggDataPtr operator()(const GermanString& /*key*/) {
        return reinterpret_cast<AggDataPtr>(&(*storage)[next++]);
    }
    AggDataPtr operator()(std::nullptr_t) {
        return reinterpret_cast<AggDataPtr>(&(*storage)[next++]);
    }
};

// Wrap a MutableColumnPtr into a `Columns` (vector<ColumnPtr>) via the helper
// used throughout the codebase.
inline Columns to_columns(MutableColumnPtr&& col) {
    MutableColumns m;
    m.emplace_back(std::move(col));
    return ColumnHelper::to_columns(std::move(m));
}

// Small helper to exercise a GermanString-keyed agg hash map built the same
// way the aggregator builds it: one GermanStringColumn, one key per row.
template <PhmapSeed seed>
void build_one(AggHashMapWithOneGermanStringKey<GermanStringAggHashMap<seed>>& op,
               const GermanStringColumn& column, MemPool* pool, std::vector<uint64_t>& storage) {
    const size_t n = column.size();
    Columns cols = to_columns(column.clone());
    Buffer<AggDataPtr> agg_states(n);
    storage.assign(n + 1, 0);
    TestAllocator alloc{&storage, 0};
    op.build_hash_map(n, cols, pool, alloc, &agg_states);
}

} // namespace

TEST(AggHashMapGermanStringTest, DistinctInlineAndLongAndPrefixCollisions) {
    using HashMap = GermanStringAggHashMap<PhmapSeed1>;
    using OpType = AggHashMapWithOneGermanStringKey<HashMap>;

    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    OpType op(/*chunk_size=*/256, &stat);

    auto col = GermanStringColumn::create();
    // Inline duplicates (len <= 12)
    col->append(Slice("a"));
    col->append(Slice("a"));
    col->append(Slice("abc"));
    // Long duplicates (len > 12)
    col->append(Slice("this is a long string"));
    col->append(Slice("this is a long string"));
    // Two distinct long strings that share a 4-byte prefix ("prefix_same") but
    // differ past the prefix — exercises the comparator's
    // prefix-equal-but-full-compare path.
    col->append(Slice("prefix_same_but_different_tail_A"));
    col->append(Slice("prefix_same_but_different_tail_B"));
    // An inline string that shares the first 4 chars with a long one.
    col->append(Slice("prefA"));                          // inline
    col->append(Slice("prefA_long_string_with_tail"));    // long
    // Mixed with empty string
    col->append(Slice(""));
    col->append(Slice(""));

    MemPool pool;
    std::vector<uint64_t> storage;
    build_one<PhmapSeed1>(op, *col, &pool, storage);

    // Distinct keys: {"a", "abc", "this is a long string",
    //                 "prefix_same_but_different_tail_A",
    //                 "prefix_same_but_different_tail_B",
    //                 "prefA", "prefA_long_string_with_tail", ""}
    EXPECT_EQ(8u, op.hash_map.size());
}

TEST(AggHashMapGermanStringTest, LongKeyStaysValidAfterSourceColumnFree) {
    using HashMap = GermanStringAggHashMap<PhmapSeed1>;
    using OpType = AggHashMapWithOneGermanStringKey<HashMap>;

    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    OpType op(/*chunk_size=*/32, &stat);

    const std::string long1 = "alpha_long_payload_one_xyz";
    const std::string long2 = "beta_long_payload_two_qrs";

    MemPool pool;
    std::vector<uint64_t> storage;
    {
        auto col = GermanStringColumn::create();
        col->append(Slice(long1));
        col->append(Slice(long2));
        col->append(Slice(long1));
        build_one<PhmapSeed1>(op, *col, &pool, storage);
        // col goes out of scope -> arena is destroyed. The hash map must have
        // copied long-rep bytes into |pool|; if it did not, the reads below
        // would be UAF.
    }

    EXPECT_EQ(2u, op.hash_map.size());

    // Look up by building fresh (pool-independent) GermanStrings. Any stored
    // key that still points at the freed arena would either fail to match or
    // trigger ASan.
    GermanString k1(long1.data(), long1.size());
    GermanString k2(long2.data(), long2.size());
    GermanString k3(std::string("nonexistent_but_long_enough").data(), 28);
    EXPECT_EQ(1u, op.hash_map.count(k1));
    EXPECT_EQ(1u, op.hash_map.count(k2));
    EXPECT_EQ(0u, op.hash_map.count(k3));
}

TEST(AggHashSetGermanStringTest, DistinctSemantics) {
    using HashSet = GermanStringAggHashSet<PhmapSeed1>;
    using OpType = AggHashSetOfOneGermanStringKey<HashSet>;

    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    OpType op(/*chunk_size=*/64, &stat);

    auto col = GermanStringColumn::create();
    col->append(Slice("x"));
    col->append(Slice("x"));
    col->append(Slice("yy"));
    col->append(Slice("zzzzzzzzzzzzzzzzzzz"));       // long
    col->append(Slice("zzzzzzzzzzzzzzzzzzz"));       // long dup
    col->append(Slice("zzzzzzzzzzzzzzzzzzz_tail")); // long, different

    MemPool pool;
    Columns cols = to_columns(col->clone());
    op.build_hash_set(col->size(), cols, &pool);

    // Distinct values: {"x", "yy", "zz...", "zz..._tail"}
    EXPECT_EQ(4u, op.hash_set.size());
}

TEST(AggHashSetGermanStringTest, LongKeySurvivesSourceColumnFree) {
    using HashSet = GermanStringAggHashSet<PhmapSeed1>;
    using OpType = AggHashSetOfOneGermanStringKey<HashSet>;

    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    OpType op(/*chunk_size=*/64, &stat);

    const std::string long_key = "this_payload_is_long_enough_to_be_outlined";

    MemPool pool;
    {
        auto col = GermanStringColumn::create();
        col->append(Slice(long_key));
        Columns cols = to_columns(col->clone());
        op.build_hash_set(1, cols, &pool);
        // col drops here; its arena is freed.
    }

    EXPECT_EQ(1u, op.hash_set.size());
    GermanString probe(long_key.data(), long_key.size());
    EXPECT_EQ(1u, op.hash_set.count(probe));
}

TEST(AggHashMapVariantGermanStringTest, VariantRoutesToGermanArm) {
    // Verify the HashVariantResolver selects the GermanString arm for
    // TYPE_GERMAN_STRING (both phases, nullable and non-nullable).
    using MapResolver = HashVariantResolver<AggHashMapVariant>;
    auto& r = MapResolver::instance();
    EXPECT_EQ(AggHashMapVariant::Type::phase1_german_string,
              r.get_unary_type(AggrPhase1, TYPE_GERMAN_STRING, /*nullable=*/false));
    EXPECT_EQ(AggHashMapVariant::Type::phase1_null_german_string,
              r.get_unary_type(AggrPhase1, TYPE_GERMAN_STRING, /*nullable=*/true));
    EXPECT_EQ(AggHashMapVariant::Type::phase2_german_string,
              r.get_unary_type(AggrPhase2, TYPE_GERMAN_STRING, /*nullable=*/false));
    EXPECT_EQ(AggHashMapVariant::Type::phase2_null_german_string,
              r.get_unary_type(AggrPhase2, TYPE_GERMAN_STRING, /*nullable=*/true));

    using SetResolver = HashVariantResolver<AggHashSetVariant>;
    auto& rs = SetResolver::instance();
    EXPECT_EQ(AggHashSetVariant::Type::phase1_german_string,
              rs.get_unary_type(AggrPhase1, TYPE_GERMAN_STRING, /*nullable=*/false));
    EXPECT_EQ(AggHashSetVariant::Type::phase1_null_german_string,
              rs.get_unary_type(AggrPhase1, TYPE_GERMAN_STRING, /*nullable=*/true));
    EXPECT_EQ(AggHashSetVariant::Type::phase2_german_string,
              rs.get_unary_type(AggrPhase2, TYPE_GERMAN_STRING, /*nullable=*/false));
    EXPECT_EQ(AggHashSetVariant::Type::phase2_null_german_string,
              rs.get_unary_type(AggrPhase2, TYPE_GERMAN_STRING, /*nullable=*/true));
}

} // namespace starrocks
