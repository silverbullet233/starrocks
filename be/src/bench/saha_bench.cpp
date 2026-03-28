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

// Benchmark: SAHAMultiMap vs production baseline for single-string GROUP BY.
//
// Production path for single VARCHAR/CHAR GROUP BY:
//   OneStringAggHashMap = phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed, SliceEqual>
//
// This is confirmed by the variant selection logic in aggregator.cpp:
// _get_hash_table_type() -> HashVariantResolver::get_unary_type(phase, TYPE_VARCHAR, nullable)
//   -> phase1_string / phase1_null_string
// No fixed-size optimization (SliceKey4/8/16) applies to VARCHAR because
// get_size_of_fixed_length_type(TYPE_VARCHAR) returns 0.
//
// For fair comparison, the baseline uses the exact same hash + equality functions
// as production, and test strings are allocated with SIMD padding to support
// memequal_padded (SliceEqual).

#include <benchmark/benchmark.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_hash.h"
#include "exec/aggregate/string_adaptive_hash_map.h"

namespace starrocks {

// ============================================================================
// Test data: strings allocated with SIMD padding (mimics MemPool behavior)
// ============================================================================

struct TestData {
    // Owns the raw memory; each string has SLICE_MEMEQUAL_OVERFLOW_PADDING bytes after it
    std::vector<std::unique_ptr<char[]>> buffers;
    std::vector<Slice> slices;

    void generate(int num_rows, int min_len, int max_len, int cardinality) {
        static const std::string alphanum =
                "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> dist_len(min_len, max_len);
        std::uniform_int_distribution<int> dist_char(0, alphanum.size() - 1);

        // Build dictionary with padding
        std::vector<Slice> dict_slices;
        std::vector<std::unique_ptr<char[]>> dict_bufs;
        for (int i = 0; i < cardinality; i++) {
            int len = dist_len(rng);
            auto buf = std::make_unique<char[]>(len + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            for (int j = 0; j < len; j++) buf[j] = alphanum[dist_char(rng)];
            memset(buf.get() + len, 0, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            dict_slices.emplace_back(buf.get(), len);
            dict_bufs.push_back(std::move(buf));
        }

        // Sample from dictionary, copy with padding
        std::uniform_int_distribution<int> dist_idx(0, cardinality - 1);
        buffers.clear();
        slices.clear();
        buffers.reserve(num_rows);
        slices.reserve(num_rows);
        for (int i = 0; i < num_rows; i++) {
            auto& src = dict_slices[dist_idx(rng)];
            auto buf = std::make_unique<char[]>(src.size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(buf.get(), src.data, src.size);
            memset(buf.get() + src.size, 0, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            slices.emplace_back(buf.get(), src.size);
            buffers.push_back(std::move(buf));
        }
    }
};

// Global test data for each scenario.
// 0=Short_1_2/1K  1=Short_3_8/10K  2=Med_9_16/10K  3=Med_17_24/10K
// 4=Long_25_64/10K  5=Mix_1_32/10K  6=Mix_1_64/100K  7=Short_3_8/100K
static constexpr int kNumRows = 200000;
static TestData g_data[8];

static void init_data() {
    static bool initialized = false;
    if (initialized) return;
    initialized = true;

    g_data[0].generate(kNumRows, 1, 2, 1000);
    g_data[1].generate(kNumRows, 3, 8, 10000);
    g_data[2].generate(kNumRows, 9, 16, 10000);
    g_data[3].generate(kNumRows, 17, 24, 10000);
    g_data[4].generate(kNumRows, 25, 64, 10000);
    g_data[5].generate(kNumRows, 1, 32, 10000);
    g_data[6].generate(500000, 1, 64, 100000);
    g_data[7].generate(500000, 3, 8, 100000);
}

// ============================================================================
// Baseline: exact production hash map for single VARCHAR GROUP BY
//   phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed, SliceEqual>
// SliceEqual uses SIMD-accelerated memequal_padded (SSE2)
// ============================================================================

using BaselineMap = phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;

static void BM_Baseline_Emplace(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& slices = g_data[idx].slices;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    for (auto _ : state) {
        BaselineMap map;
        for (auto& s : slices) {
            map.lazy_emplace(s, [&](const auto& ctor) { ctor(s, dummy); });
        }
        benchmark::DoNotOptimize(map.size());
    }
    state.SetItemsProcessed(state.iterations() * slices.size());
}

static void BM_Baseline_Find(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& slices = g_data[idx].slices;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    BaselineMap map;
    for (auto& s : slices) {
        map.lazy_emplace(s, [&](const auto& ctor) { ctor(s, dummy); });
    }

    for (auto _ : state) {
        size_t found = 0;
        for (auto& s : slices) {
            found += (map.find(s) != map.end());
        }
        benchmark::DoNotOptimize(found);
    }
    state.SetItemsProcessed(state.iterations() * slices.size());
}

// ============================================================================
// SAHA: SAHAMultiMap
// ============================================================================

static void BM_SAHA_Emplace(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& slices = g_data[idx].slices;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    for (auto _ : state) {
        SAHAMultiMap<PhmapSeed1> map;
        for (auto& s : slices) {
            map.lazy_emplace(s, [&](AggDataPtr& val) { val = dummy; });
        }
        benchmark::DoNotOptimize(map.size());
    }
    state.SetItemsProcessed(state.iterations() * slices.size());
}

static void BM_SAHA_Find(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& slices = g_data[idx].slices;
    AggDataPtr dummy = reinterpret_cast<AggDataPtr>(0x1);

    SAHAMultiMap<PhmapSeed1> map;
    for (auto& s : slices) {
        map.lazy_emplace(s, [&](AggDataPtr& val) { val = dummy; });
    }

    for (auto _ : state) {
        size_t found = 0;
        for (auto& s : slices) {
            found += (map.find(s) != nullptr);
        }
        benchmark::DoNotOptimize(found);
    }
    state.SetItemsProcessed(state.iterations() * slices.size());
}


// ============================================================================
// Aggregation-level: tests compute_agg_states (batch dispatch path)
// ============================================================================

static void BM_AggLevel_Baseline(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& test = g_data[idx];

    for (auto _ : state) {
        state.PauseTiming();
        MemPool pool;
        // Build a BinaryColumn from test slices
        auto column = BinaryColumn::create();
        for (auto& s : test.slices) column->append(s);
        ColumnPtr col = std::move(column);

        using HashMap = AggHashMapWithOneStringKeyWithNullable<
                phmap::flat_hash_map<Slice, AggDataPtr, SliceHashWithSeed<PhmapSeed1>, SliceEqual>, false>;
        HashMap hash_map(4096, nullptr);
        size_t chunk_size = col->size();
        Buffer<AggDataPtr> agg_states(chunk_size);
        auto alloc = [&](auto&& key) -> AggDataPtr {
            auto* p = pool.allocate(sizeof(Slice));
            if constexpr (!std::is_null_pointer_v<std::decay_t<decltype(key)>>) {
                *reinterpret_cast<Slice*>(p) = key;
            }
            return p;
        };
        state.ResumeTiming();

        ExtraAggParam extra;
        Columns key_columns = {col};
        hash_map.template compute_agg_states<decltype(alloc), HTBuildOp<true, false, false>>(
                chunk_size, key_columns, &pool, std::move(alloc), &agg_states, &extra);
        benchmark::DoNotOptimize(hash_map.hash_map.size());
    }
    state.SetItemsProcessed(state.iterations() * test.slices.size());
}

static void BM_AggLevel_SAHA(benchmark::State& state) {
    init_data();
    int idx = state.range(0);
    auto& test = g_data[idx];

    for (auto _ : state) {
        state.PauseTiming();
        MemPool pool;
        auto column = BinaryColumn::create();
        for (auto& s : test.slices) column->append(s);
        ColumnPtr col = std::move(column);

        using HashMap = AggHashMapWithOneStringKeyAdaptive<PhmapSeed1, false>;
        HashMap hash_map(4096, nullptr);
        size_t chunk_size = col->size();
        Buffer<AggDataPtr> agg_states(chunk_size);
        auto alloc = [&](auto&& key) -> AggDataPtr {
            auto* p = pool.allocate(sizeof(Slice));
            if constexpr (!std::is_null_pointer_v<std::decay_t<decltype(key)>>) {
                *reinterpret_cast<Slice*>(p) = key;
            }
            return p;
        };
        state.ResumeTiming();

        ExtraAggParam extra;
        Columns key_columns = {col};
        hash_map.template compute_agg_states<decltype(alloc), HTBuildOp<true, false, false>>(
                chunk_size, key_columns, &pool, std::move(alloc), &agg_states, &extra);
        benchmark::DoNotOptimize(hash_map.hash_map.size());
    }
    state.SetItemsProcessed(state.iterations() * test.slices.size());
}

// ============================================================================
// Registration
// ============================================================================

static void BenchArgs(benchmark::internal::Benchmark* b) {
    for (int i = 0; i < 8; i++) b->Arg(i);
}

// Data scenario names (for reference):
// 0=Short_1_2/1K  1=Short_3_8/10K  2=Med_9_16/10K  3=Med_17_24/10K
// 4=Long_25_64/10K  5=Mix_1_32/10K  6=Mix_1_64/100K  7=Short_3_8/100K

BENCHMARK(BM_Baseline_Emplace)->Apply(BenchArgs)->Iterations(10);
BENCHMARK(BM_SAHA_Emplace)->Apply(BenchArgs)->Iterations(10);
BENCHMARK(BM_Baseline_Find)->Apply(BenchArgs)->Iterations(10);
BENCHMARK(BM_SAHA_Find)->Apply(BenchArgs)->Iterations(10);
// Aggregation-level (tests batch dispatch in compute_agg_states)
BENCHMARK(BM_AggLevel_Baseline)->Apply(BenchArgs)->Iterations(5);
BENCHMARK(BM_AggLevel_SAHA)->Apply(BenchArgs)->Iterations(5);

} // namespace starrocks

BENCHMARK_MAIN();
