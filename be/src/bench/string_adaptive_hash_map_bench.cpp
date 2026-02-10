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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/string_adaptive_hash_map.h"

namespace starrocks {

using BaselineMap = SliceAggHashMap<PhmapSeed1>;
using SahaMap = StringAdaptiveHashMap<uint64_t, PhmapSeed1>;

namespace {

constexpr int kRows = 200000;
constexpr int kMaxHighCardinality = 200000;
constexpr int kMaxKeySpaceCapacity = 1000000;

// Representative lengths for SAHA buckets: <=2, <=8, <=16, <=24, >24.
static const std::vector<int> kKeyLengths = {2, 8, 16, 24, 64};

struct DataSet {
    int key_len = 0;
    bool high_cardinality = false;
    size_t ndv = 0;

    std::vector<std::string> dictionary;
    std::vector<size_t> dictionary_hashes;

    std::vector<std::string> miss_dictionary;

    std::vector<Slice> rows;
    std::vector<size_t> row_hashes;
    std::vector<Slice> hit_queries;
    std::vector<size_t> hit_hashes;
    std::vector<Slice> mixed_queries;
    std::vector<size_t> mixed_hashes;
};

size_t compute_key_space_capacity(int key_len) {
    size_t capacity = 1;
    for (int i = 0; i < key_len; ++i) {
        if (capacity > static_cast<size_t>(kMaxKeySpaceCapacity / 62)) {
            return kMaxKeySpaceCapacity;
        }
        capacity *= 62;
    }
    return std::min<size_t>(capacity, kMaxKeySpaceCapacity);
}

size_t compute_high_cardinality(int key_len) {
    const size_t key_space = compute_key_space_capacity(key_len);
    return std::min<size_t>(kMaxHighCardinality, key_space / 2);
}

size_t compute_low_cardinality(int key_len) {
    const size_t high = compute_high_cardinality(key_len);
    const size_t low = std::max<size_t>(8, high / 8);
    return std::min<size_t>(1024, low);
}

std::string encode_key(size_t id, int key_len) {
    static constexpr char kAlphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string key(key_len, '0');
    size_t value = id;
    for (int i = key_len - 1; i >= 0; --i) {
        key[i] = kAlphabet[value % 62];
        value /= 62;
    }
    return key;
}

DataSet build_dataset(int key_len, bool high_cardinality) {
    DataSet data;
    data.key_len = key_len;
    data.high_cardinality = high_cardinality;
    data.ndv = high_cardinality ? compute_high_cardinality(key_len) : compute_low_cardinality(key_len);

    const size_t key_space_capacity = compute_key_space_capacity(key_len);
    const size_t miss_start = key_space_capacity - data.ndv;

    data.dictionary.reserve(data.ndv);
    data.dictionary_hashes.reserve(data.ndv);
    data.miss_dictionary.reserve(data.ndv);
    for (size_t i = 0; i < data.ndv; ++i) {
        data.dictionary.emplace_back(encode_key(i, key_len));
        data.miss_dictionary.emplace_back(encode_key(miss_start + i, key_len));
    }

    SliceHashWithSeed<PhmapSeed1> hash_fn;
    for (const auto& key : data.dictionary) {
        data.dictionary_hashes.emplace_back(hash_fn(Slice(key)));
    }

    std::vector<uint32_t> idx(kRows);
    std::iota(idx.begin(), idx.end(), 0);
    for (auto& i : idx) {
        i %= data.ndv;
    }
    std::mt19937 rng(static_cast<uint32_t>(key_len * 97 + (high_cardinality ? 11 : 7)));
    std::shuffle(idx.begin(), idx.end(), rng);

    data.rows.reserve(kRows);
    data.row_hashes.reserve(kRows);
    data.hit_queries.reserve(kRows);
    data.hit_hashes.reserve(kRows);
    data.mixed_queries.reserve(kRows);
    data.mixed_hashes.reserve(kRows);
    for (int i = 0; i < kRows; ++i) {
        size_t dict_idx = idx[i];
        Slice hit(data.dictionary[dict_idx]);
        data.rows.emplace_back(hit);
        data.row_hashes.emplace_back(data.dictionary_hashes[dict_idx]);
        data.hit_queries.emplace_back(hit);
        data.hit_hashes.emplace_back(data.dictionary_hashes[dict_idx]);

        if ((i & 1) == 0) {
            data.mixed_queries.emplace_back(hit);
            data.mixed_hashes.emplace_back(data.dictionary_hashes[dict_idx]);
        } else {
            Slice miss(data.miss_dictionary[dict_idx]);
            data.mixed_queries.emplace_back(miss);
            data.mixed_hashes.emplace_back(hash_fn(miss));
        }
    }

    return data;
}

template <typename MapType>
typename MapType::mapped_type make_mapped(size_t value) {
    if constexpr (std::is_pointer_v<typename MapType::mapped_type>) {
        return reinterpret_cast<typename MapType::mapped_type>(value + 1);
    } else {
        return static_cast<typename MapType::mapped_type>(value + 1);
    }
}

template <typename MapType>
uint64_t mapped_to_u64(typename MapType::mapped_type value) {
    if constexpr (std::is_pointer_v<typename MapType::mapped_type>) {
        return reinterpret_cast<uintptr_t>(value);
    } else {
        return static_cast<uint64_t>(value);
    }
}

template <typename MapType>
void fill_map(MapType& map, const DataSet& data) {
    map.reserve(data.ndv);
    for (size_t i = 0; i < data.dictionary.size(); ++i) {
        Slice key(data.dictionary[i]);
        map.lazy_emplace_with_hash(key, data.dictionary_hashes[i],
                                   [&](const auto& ctor) { ctor(key, make_mapped<MapType>(i)); });
    }
}

template <typename MapType>
void run_insert_stream(benchmark::State& state, const DataSet& data) {
    for (auto _ : state) {
        MapType map;
        map.reserve(data.ndv);
        for (size_t i = 0; i < data.rows.size(); ++i) {
            map.lazy_emplace_with_hash(data.rows[i], data.row_hashes[i],
                                       [&](const auto& ctor) { ctor(data.rows[i], make_mapped<MapType>(i)); });
        }
        benchmark::DoNotOptimize(map.size());
    }
    state.SetItemsProcessed(state.iterations() * data.rows.size());
}

template <typename MapType>
void run_find_hit(benchmark::State& state, const DataSet& data) {
    MapType map;
    fill_map(map, data);

    for (auto _ : state) {
        uint64_t sum = 0;
        for (size_t i = 0; i < data.hit_queries.size(); ++i) {
            auto it = map.find(data.hit_queries[i], data.hit_hashes[i]);
            if (it != map.end()) {
                sum += mapped_to_u64<MapType>(it->second);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * data.hit_queries.size());
}

template <typename MapType>
void run_find_mixed50(benchmark::State& state, const DataSet& data) {
    MapType map;
    fill_map(map, data);

    for (auto _ : state) {
        uint64_t sum = 0;
        for (size_t i = 0; i < data.mixed_queries.size(); ++i) {
            auto it = map.find(data.mixed_queries[i], data.mixed_hashes[i]);
            if (it != map.end()) {
                sum += mapped_to_u64<MapType>(it->second);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * data.mixed_queries.size());
}

template <typename Workload>
void run_case(benchmark::State& state, Workload workload, bool use_saha) {
    const int key_len = static_cast<int>(state.range(0));
    const bool high_cardinality = state.range(1) == 1;
    const DataSet data = build_dataset(key_len, high_cardinality);

    if (use_saha) {
        workload.template operator()<SahaMap>(state, data);
    } else {
        workload.template operator()<BaselineMap>(state, data);
    }
}

struct InsertStreamWorkload {
    template <typename MapType>
    void operator()(benchmark::State& state, const DataSet& data) const {
        run_insert_stream<MapType>(state, data);
    }
};

struct FindHitWorkload {
    template <typename MapType>
    void operator()(benchmark::State& state, const DataSet& data) const {
        run_find_hit<MapType>(state, data);
    }
};

struct FindMixed50Workload {
    template <typename MapType>
    void operator()(benchmark::State& state, const DataSet& data) const {
        run_find_mixed50<MapType>(state, data);
    }
};

void add_args(benchmark::internal::Benchmark* b) {
    for (int key_len : kKeyLengths) {
        b->Args({key_len, 1}); // high-cardinality
        b->Args({key_len, 0}); // low-cardinality
    }
}

} // namespace

static void BM_InsertStream(benchmark::State& state, bool use_saha) {
    run_case(state, InsertStreamWorkload{}, use_saha);
}

static void BM_FindHit(benchmark::State& state, bool use_saha) {
    run_case(state, FindHitWorkload{}, use_saha);
}

static void BM_FindMixed50(benchmark::State& state, bool use_saha) {
    run_case(state, FindMixed50Workload{}, use_saha);
}

BENCHMARK_CAPTURE(BM_InsertStream, baseline, false)->Apply(add_args)->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_InsertStream, saha, true)->Apply(add_args)->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_FindHit, baseline, false)->Apply(add_args)->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_FindHit, saha, true)->Apply(add_args)->Unit(benchmark::kMicrosecond);

BENCHMARK_CAPTURE(BM_FindMixed50, baseline, false)->Apply(add_args)->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_FindMixed50, saha, true)->Apply(add_args)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();

} // namespace starrocks
