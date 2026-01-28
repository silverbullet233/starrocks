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

#include <cstdint>
#include <vector>

#include "column/fixed_length_column.h"
#include "runtime/current_thread.h"
#include "runtime/memory/memory_allocator.h"

namespace starrocks {
namespace {

using ColumnType = FixedLengthColumn<int32_t>;
using ValueType = int32_t;

ColumnType::MutablePtr make_src_column(size_t count) {
    auto column = ColumnType::create(memory::get_default_allocator());
    std::vector<ValueType> values(count);
    for (size_t i = 0; i < count; ++i) {
        values[i] = static_cast<ValueType>(i);
    }
    column->append_numbers(values.data(), values.size() * sizeof(ValueType));
    return column;
}

static void bench_append(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const size_t count = static_cast<size_t>(state.range(0));
    auto src = make_src_column(count);

    for (auto _ : state) {
        state.PauseTiming();
        auto dst = ColumnType::create(memory::get_default_allocator());
        state.ResumeTiming();

        dst->append(*src, 0, count);
        benchmark::DoNotOptimize(dst);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(ValueType));
}

static void bench_append_reserved(benchmark::State& state) {
    SCOPED_SET_CATCHED(true);
    const size_t count = static_cast<size_t>(state.range(0));
    auto src = make_src_column(count);

    for (auto _ : state) {
        state.PauseTiming();
        auto dst = ColumnType::create(memory::get_default_allocator());
        dst->reserve(count);
        state.ResumeTiming();

        dst->append(*src, 0, count);
        benchmark::DoNotOptimize(dst);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(ValueType));
}

static void process_args(benchmark::internal::Benchmark* bench) {
    bench->Arg(4096)->Iterations(200);
    bench->Arg(40960)->Iterations(50);
    bench->Arg(409600)->Iterations(10);
}

BENCHMARK(bench_append)->Apply(process_args);
BENCHMARK(bench_append_reserved)->Apply(process_args);

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();
