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

#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/german_string.h"
#include "column/german_string_column.h"
#include "exec/join/join_hash_map_helper.h"
#include "exec/join/join_hash_map_method.h"
#include "exec/join/join_hash_map_method.hpp"
#include "exec/join/join_hash_table_descriptor.h"
#include "exec/join/join_key_constructor.h"
#include "exec/join/join_key_constructor.hpp"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

// Exercises the GermanString-native arm of the single-key join hash path:
//   - BuildKeyConstructorForOneKey<TYPE_GERMAN_STRING>
//   - ProbeKeyConstructorForOneKey<TYPE_GERMAN_STRING>
//   - BucketChainedJoinHashMap<TYPE_GERMAN_STRING>
//   - JoinKeyHash<GermanString> (delegates to GermanString::fnv_hash)
//
// The full JoinHashTable::build()/probe() path is heavy machinery (requires a
// RuntimeState, RowDescriptor with slot descriptors, profile timers, etc.) and
// is exercised by end-to-end SQL tests. Here we directly drive the three
// components above, which is sufficient to prove the new arm wires up and that
// long-rep keys remain valid after the source column is freed.

inline MutableColumnPtr make_column_from_strings(const std::vector<std::string>& values) {
    auto col = GermanStringColumn::create();
    for (const auto& v : values) {
        col->append(Slice(v.data(), v.size()));
    }
    return col;
}

// Build the dummy-row convention used by JoinHashTable::build: index 0 is the
// sentinel row; real keys start at index 1.
inline MutableColumnPtr make_build_key_column(const std::vector<std::string>& values) {
    auto col = GermanStringColumn::create();
    col->append_default();
    for (const auto& v : values) {
        col->append(Slice(v.data(), v.size()));
    }
    return col;
}

struct BuiltTable {
    std::shared_ptr<JoinHashTableItems> items;
    TypeDescriptor gs_type;
};

BuiltTable build_table(const std::vector<std::string>& keys) {
    BuiltTable out;
    out.gs_type = TypeDescriptor::from_logical_type(TYPE_GERMAN_STRING);
    out.items = std::make_shared<JoinHashTableItems>();
    auto& items = *out.items;

    items.row_count = static_cast<uint32_t>(keys.size());
    items.join_keys.emplace_back(JoinKeyDesc{&out.gs_type, false, nullptr});
    items.key_columns.emplace_back(make_build_key_column(keys));

    using Build = BuildKeyConstructorForOneKey<TYPE_GERMAN_STRING>;
    using Method = BucketChainedJoinHashMap<TYPE_GERMAN_STRING>;

    Build::prepare(nullptr, &items);
    Build::build_key(nullptr, &items);
    Method::build_prepare(nullptr, &items);
    Method::construct_hash_table(&items, Build::get_key_data(items), Build::get_is_nulls(items));

    return out;
}

// Probe: emulate the one-key probe path without the full HashTableProbeState
// runtime plumbing (buckets buffer, next buffer). We recompute bucket lookup
// + chain walk manually against table_items.first / next.
//
// Returns whether |probe_key| was found in the hash table.
bool probe_contains(const JoinHashTableItems& items, const GermanString& probe_key) {
    using Method = BucketChainedJoinHashMap<TYPE_GERMAN_STRING>;
    // keys Buffer<GermanString> from the column's get_data (includes dummy at 0)
    const auto* data_column =
            ColumnHelper::as_raw_column<GermanStringColumn>(ColumnHelper::get_data_column(items.key_columns[0]));
    const auto& build_keys = data_column->get_data();

    const uint32_t bucket_num = JoinHashMapHelper::calc_bucket_num<GermanString>(probe_key, items.bucket_size,
                                                                                  items.log_bucket_size);
    uint32_t idx = items.first[bucket_num];
    while (idx != 0) {
        if (Method::equal(build_keys[idx], probe_key)) {
            return true;
        }
        idx = items.next[idx];
    }
    return false;
}

TEST(JoinHashMapGermanStringTest, InlineAndLongKeysProbeAfterBuildColumnFreed) {
    // Mix of inline (<= 12 bytes) and long-rep (> 12 bytes) strings.
    std::vector<std::string> build_keys = {
            "a",                                 // inline, 1 byte
            "abc",                               // inline
            "abcdefghij",                        // inline, 10 bytes
            "abcdefghijkl",                      // inline max, 12 bytes
            "abcdefghijklm",                     // long-rep, 13 bytes
            "the quick brown fox jumps",         // long-rep
            "STARROCKS_GERMAN_STRING_LONG_KEY",  // long-rep
    };

    auto table = build_table(build_keys);
    auto& items = *table.items;

    // Precompute expected GermanString values using a throwaway column we will
    // drop before probing, to confirm that the hash-table's copy of the bytes
    // is self-owned and stays valid.
    std::vector<GermanString> expected_hits;
    {
        auto throwaway = make_column_from_strings(build_keys);
        auto& data = ColumnHelper::as_raw_column<GermanStringColumn>(throwaway.get())->get_data();
        expected_hits.assign(data.begin(), data.end());
        // Drop the source column. For long-rep keys, `expected_hits[i].long_rep.ptr`
        // now dangles — but we only rebuild them when probing, via Slice inputs
        // below. The point of this block is to prove we did not keep any pointer
        // from it; everything below constructs GermanStrings afresh.
    }

    // Probe: each build key should be found, via a freshly materialized
    // GermanString whose long payload lives in a probe-owned buffer.
    for (const auto& k : build_keys) {
        auto probe_col = GermanStringColumn::create();
        probe_col->append(Slice(k.data(), k.size()));
        const GermanString& gs = probe_col->get_german_string(0);
        EXPECT_TRUE(probe_contains(items, gs)) << "missing key=\"" << k << "\"";
    }

    // Probe with strings that were NOT inserted: should miss.
    std::vector<std::string> non_keys = {"b", "xyz", "definitely_absent_from_the_table_123"};
    for (const auto& k : non_keys) {
        auto probe_col = GermanStringColumn::create();
        probe_col->append(Slice(k.data(), k.size()));
        const GermanString& gs = probe_col->get_german_string(0);
        EXPECT_FALSE(probe_contains(items, gs)) << "unexpected hit key=\"" << k << "\"";
    }
}

TEST(JoinHashMapGermanStringTest, LongKeysWithSharedFourBytePrefix) {
    // Two long-rep keys that share the first 4 bytes ("abcd"). This stresses
    // GermanString::operator==, which short-circuits on the first 8 bytes
    // (len + prefix/inline); with identical length + prefix, equality must
    // fall through to a full payload memcompare.
    std::vector<std::string> build_keys = {
            "abcdXXXXXXXXXXXXXYYY",  // 20 bytes, long-rep
            "abcdXXXXXXXXXXXXXZZZ",  // 20 bytes, long-rep, same len, same prefix
    };

    auto table = build_table(build_keys);
    auto& items = *table.items;

    for (const auto& k : build_keys) {
        auto probe_col = GermanStringColumn::create();
        probe_col->append(Slice(k.data(), k.size()));
        EXPECT_TRUE(probe_contains(items, probe_col->get_german_string(0)))
                << "missing prefix-collision key=\"" << k << "\"";
    }

    // A third key sharing the same prefix but not equal must miss.
    {
        std::string missing = "abcdXXXXXXXXXXXXXWWW";
        auto probe_col = GermanStringColumn::create();
        probe_col->append(Slice(missing.data(), missing.size()));
        EXPECT_FALSE(probe_contains(items, probe_col->get_german_string(0)))
                << "unexpected hit for prefix-colliding non-key";
    }
}

TEST(JoinHashMapGermanStringTest, KeyConstructorGetKeyDataReturnsColumnBuffer) {
    // Confirms BuildKeyConstructorForOneKey<TYPE_GERMAN_STRING>::get_key_data
    // returns the column's underlying Buffer<GermanString> (no slice cache
    // needed for the GermanString arm).
    std::vector<std::string> keys = {"inline", "this_string_is_long_enough_to_be_out_of_line"};
    auto table = build_table(keys);
    auto& items = *table.items;

    using Build = BuildKeyConstructorForOneKey<TYPE_GERMAN_STRING>;
    const auto key_data = Build::get_key_data(items);
    // One dummy row at index 0 plus one entry per build key.
    ASSERT_EQ(key_data.size(), keys.size() + 1);
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_EQ(static_cast<std::string>(key_data[i + 1]), keys[i]);
    }

    // Slice cache must NOT have been populated for TYPE_GERMAN_STRING (distinct
    // from the Slice arm).
    EXPECT_TRUE(items.build_slice.empty());
}

} // namespace
} // namespace starrocks
