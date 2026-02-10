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
#include <vector>

namespace starrocks {

using TestMap = StringAdaptiveHashMap<int64_t, PhmapSeed1>;

static std::string make_key(char ch, size_t len) {
    return std::string(len, ch);
}

TEST(StringAdaptiveHashMapTest, BoundaryLengthRouting) {
    TestMap map;
    std::vector<std::string> keys = {
            make_key('a', 2),  make_key('b', 3),  make_key('c', 8),  make_key('d', 9),
            make_key('e', 16), make_key('f', 17), make_key('g', 24), make_key('h', 25),
    };

    for (int i = 0; i < keys.size(); ++i) {
        Slice key(keys[i]);
        map.lazy_emplace(key, [&](const auto& ctor) { ctor(key, i + 1); });
    }

    ASSERT_EQ(keys.size(), map.size());
    for (int i = 0; i < keys.size(); ++i) {
        Slice key(keys[i]);
        auto it = map.find(key);
        ASSERT_NE(it, map.end());
        EXPECT_EQ(i + 1, it->second);
    }
}

TEST(StringAdaptiveHashMapTest, LazyEmplaceRunsCtorOnlyOnce) {
    TestMap map;
    std::string key_str = "same-key";
    Slice key(key_str);
    int64_t ctor_called = 0;

    for (int i = 0; i < 5; ++i) {
        auto it = map.lazy_emplace(key, [&](const auto& ctor) {
            ++ctor_called;
            ctor(key, 100);
        });
        ASSERT_NE(it, map.end());
        EXPECT_EQ(100, it->second);
    }

    EXPECT_EQ(1, ctor_called);
    EXPECT_EQ(1, map.size());
}

TEST(StringAdaptiveHashMapTest, FindWithAndWithoutHashIsConsistent) {
    TestMap map;
    std::vector<std::string> keys = {
            make_key('a', 1),  make_key('b', 2),  make_key('c', 3),  make_key('d', 8),  make_key('e', 9),
            make_key('f', 16), make_key('g', 17), make_key('h', 24), make_key('i', 25), make_key('j', 64),
    };

    auto hash_fn = map.hash_function();
    for (int i = 0; i < keys.size(); ++i) {
        Slice key(keys[i]);
        map.lazy_emplace_with_hash(key, hash_fn(key), [&](const auto& ctor) { ctor(key, i); });
    }

    for (int i = 0; i < keys.size(); ++i) {
        Slice key(keys[i]);
        auto with_hash = map.find(key, hash_fn(key));
        auto without_hash = map.find(key);
        ASSERT_NE(with_hash, map.end());
        ASSERT_NE(without_hash, map.end());
        EXPECT_EQ(with_hash->second, without_hash->second);
    }
}

TEST(StringAdaptiveHashMapTest, ClearReserveAndSize) {
    TestMap map;
    map.reserve(1024);

    std::string k1 = "abc";
    std::string k2 = "0123456789012345678901234567";
    map.lazy_emplace(Slice(k1), [&](const auto& ctor) { ctor(Slice(k1), 1); });
    map.lazy_emplace(Slice(k2), [&](const auto& ctor) { ctor(Slice(k2), 2); });
    ASSERT_EQ(2, map.size());

    map.clear();
    EXPECT_EQ(0, map.size());
    EXPECT_TRUE(map.empty());
}

TEST(StringAdaptiveHashMapTest, SupportsEmbeddedNullBytes) {
    TestMap map;
    std::string key_with_null("ab\0cd", 5);
    Slice key(key_with_null.data(), key_with_null.size());

    map.lazy_emplace(key, [&](const auto& ctor) { ctor(key, 123); });
    auto it = map.find(key);
    ASSERT_NE(it, map.end());
    EXPECT_EQ(123, it->second);

    std::string other("ab\0ce", 5);
    auto miss = map.find(Slice(other.data(), other.size()));
    EXPECT_EQ(miss, map.end());
}

TEST(StringAdaptiveHashMapTest, TrailingZeroKeyDoesNotConflictWithShortInlineKey) {
    TestMap map;
    std::string k1 = "A";
    std::string k2z("A\0", 2);
    std::string k3 = "abc";
    std::string k4z("abc\0", 4);
    std::string k9 = "12345678X";
    std::string k10z("12345678X\0", 10);

    map.lazy_emplace(Slice(k1), [&](const auto& ctor) { ctor(Slice(k1), 0); });
    map.lazy_emplace(Slice(k2z.data(), k2z.size()),
                     [&](const auto& ctor) { ctor(Slice(k2z.data(), k2z.size()), 5); });
    map.lazy_emplace(Slice(k3), [&](const auto& ctor) { ctor(Slice(k3), 1); });
    map.lazy_emplace(Slice(k4z.data(), k4z.size()),
                     [&](const auto& ctor) { ctor(Slice(k4z.data(), k4z.size()), 2); });
    map.lazy_emplace(Slice(k9), [&](const auto& ctor) { ctor(Slice(k9), 3); });
    map.lazy_emplace(Slice(k10z.data(), k10z.size()),
                     [&](const auto& ctor) { ctor(Slice(k10z.data(), k10z.size()), 4); });

    auto i1 = map.find(Slice(k1));
    auto i2z = map.find(Slice(k2z.data(), k2z.size()));
    auto i3 = map.find(Slice(k3));
    auto i4z = map.find(Slice(k4z.data(), k4z.size()));
    auto i9 = map.find(Slice(k9));
    auto i10z = map.find(Slice(k10z.data(), k10z.size()));
    ASSERT_NE(i1, map.end());
    ASSERT_NE(i2z, map.end());
    ASSERT_NE(i3, map.end());
    ASSERT_NE(i4z, map.end());
    ASSERT_NE(i9, map.end());
    ASSERT_NE(i10z, map.end());
    EXPECT_EQ(0, i1->second);
    EXPECT_EQ(5, i2z->second);
    EXPECT_EQ(1, i3->second);
    EXPECT_EQ(2, i4z->second);
    EXPECT_EQ(3, i9->second);
    EXPECT_EQ(4, i10z->second);
}

} // namespace starrocks
