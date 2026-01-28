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
#include <glog/logging.h>

#include <cstdio>
#include <cstring>

#include "gutil/strings/fastmem.h"
#include <jemalloc/jemalloc.h>

namespace starrocks {

namespace {

// Reset jemalloc arena state by purging unused memory
static void reset_arena_state() {
#ifdef __APPLE__
#define JEMALLOC_CTL mallctl
#else
#define JEMALLOC_CTL je_mallctl
#endif
    char buffer[100];
    // MALLCTL_ARENAS_ALL is defined as 4096 in jemalloc.h, representing all arenas
    // Use the value directly if the macro is not available
#ifndef MALLCTL_ARENAS_ALL
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = 4096;
#else
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = MALLCTL_ARENAS_ALL;
#endif
    int res = snprintf(buffer, sizeof(buffer), "arena.%u.purge", MALLCTL_ARENAS_ALL_VALUE);
    if (res > 0 && res < static_cast<int>(sizeof(buffer))) {
        buffer[res] = '\0';
        JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
    }
#undef JEMALLOC_CTL
}

// Benchmark: je_realloc vs je_xallocx + je_malloc + memcpy + je_free
void BM_jemalloc_realloc_je_realloc(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        void* old_ptr = ptr;
        ptr = je_realloc(ptr, new_size);
        CHECK(ptr != nullptr);
        
        // Check if realloc was in-place (pointer didn't change)
        if (ptr == old_ptr) {
            in_place_count++;
        }
        
        benchmark::DoNotOptimize(ptr);
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;
    
    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void BM_jemalloc_realloc_xallocx_malloc_memcpy_free(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        // Try to extend in-place first
        if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
            in_place_count++;
            benchmark::DoNotOptimize(ptr);
        } else {
            // Fall back to malloc + memcpy + free
            void* new_ptr = je_malloc(new_size);
            CHECK(new_ptr != nullptr);
            std::memcpy(new_ptr, ptr, old_size);
            je_free(ptr);
            ptr = new_ptr;
            benchmark::DoNotOptimize(ptr);
        }
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;
    
    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void BM_jemalloc_realloc_xallocx_malloc_memcpy_inlined_free(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        // Try to extend in-place first
        if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
            in_place_count++;
            benchmark::DoNotOptimize(ptr);
        } else {
            // Fall back to malloc + memcpy_inlined + free
            void* new_ptr = je_malloc(new_size);
            CHECK(new_ptr != nullptr);
            strings::memcpy_inlined(new_ptr, ptr, old_size);
            je_free(ptr);
            ptr = new_ptr;
            benchmark::DoNotOptimize(ptr);
        }
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;
    
    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void process_realloc_args(benchmark::internal::Benchmark* b) {
    b->ArgNames({"old_size", "new_size"});
    int64_t old_size = 8;
    constexpr int64_t kMaxOldSize = 2048LL * 1024 * 1024;
    while (old_size <= kMaxOldSize) {
        b->Args({old_size, old_size * 2});
        old_size *= 2;
    }
}

} // namespace

BENCHMARK(BM_jemalloc_realloc_je_realloc)->Name("realloc")->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_xallocx_malloc_memcpy_free)->Name("xallocx")->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_xallocx_malloc_memcpy_inlined_free)->Name("xallocx_memcpy_inlined")->Apply(process_realloc_args);

} // namespace starrocks

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
