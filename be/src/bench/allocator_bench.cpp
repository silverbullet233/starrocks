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

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <jemalloc/jemalloc.h>

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/memory_allocator.h"

namespace starrocks {

constexpr int64_t kBytesPerOp = 1024;
constexpr int kOpsPerIteration = 1;
constexpr size_t kReallocTargetBytes = 32U * 1024 * 1024;

void ensure_global_env() {
    if (!GlobalEnv::is_init()) {
        auto tracker = std::make_shared<MemTracker>(-1, "allocator_bench_root");
        auto status = GlobalEnv::GetInstance()->init_for_bench(std::move(tracker));
        CHECK(status.ok()) << status.to_string();
    }
}

namespace {

template <typename Alloc>
void BM_allocator_alloc_free(benchmark::State& state) {
    MemTracker* tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    // Prepare TLS and bind to process tracker.
    CurrentThread& current = CurrentThread::current();
    CurrentThreadMemTrackerSetter mem_tracker_setter(tracker);
    CHECK(current.mem_tracker() == tracker);

    Alloc allocator;
    std::vector<void*> ptrs(kOpsPerIteration);

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            void* ptr = allocator.alloc(kBytesPerOp);
            CHECK(ptr != nullptr);
            benchmark::DoNotOptimize(ptr);
            ptrs[i] = ptr;
        }
        benchmark::ClobberMemory();
        for (void* ptr : ptrs) {
            allocator.free(ptr, kBytesPerOp);
        }
    }

    // Flush cached counters before reporting.
    current.mem_tracker_ctx_shift();

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * kBytesPerOp));
    state.counters["s_per_op"] =
            benchmark::Counter(kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate |
                                                      benchmark::Counter::kInvert);
}

template <typename Alloc>
void BM_allocator_realloc_grow(benchmark::State& state) {
    MemTracker* tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    // Prepare TLS and bind to process tracker.
    CurrentThread& current = CurrentThread::current();
    CurrentThreadMemTrackerSetter mem_tracker_setter(tracker);
    CHECK(current.mem_tracker() == tracker);

    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Limit total live bytes per iteration to keep large realloc sizes manageable.
    const size_t base_size = std::max(old_size, new_size);
    const size_t max_ops_by_bytes = kReallocTargetBytes / base_size;
    const size_t ops_per_iteration = std::max<size_t>(
            1, std::min<size_t>(static_cast<size_t>(kOpsPerIteration), max_ops_by_bytes));

    Alloc allocator;
    std::vector<void*> ptrs(ops_per_iteration);

    auto init_ptrs = [&]() {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = allocator.alloc(old_size);
            CHECK(ptr != nullptr);
            ptrs[i] = ptr;
        }
    };

    state.PauseTiming();
    init_ptrs();
    state.ResumeTiming();

    for (auto _ : state) {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = allocator.realloc(ptrs[i], old_size, new_size);
            CHECK(ptr != nullptr);
            benchmark::DoNotOptimize(ptr);
            ptrs[i] = ptr;
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = allocator.realloc(ptrs[i], new_size, old_size);
            CHECK(ptr != nullptr);
            ptrs[i] = ptr;
        }
        state.ResumeTiming();
    }

    state.PauseTiming();
    for (void* ptr : ptrs) {
        allocator.free(ptr, old_size);
    }
    state.ResumeTiming();

    // Flush cached counters before reporting.
    current.mem_tracker_ctx_shift();

    const double ops = static_cast<double>(state.iterations()) * ops_per_iteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * new_size));
    state.counters["s_per_op"] =
            benchmark::Counter(ops_per_iteration, benchmark::Counter::kIsIterationInvariantRate |
                                                          benchmark::Counter::kInvert);
}

template <typename Alloc>
void BM_allocator_alloc_memcpy_free_grow(benchmark::State& state) {
    MemTracker* tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    // Prepare TLS and bind to process tracker.
    CurrentThread& current = CurrentThread::current();
    CurrentThreadMemTrackerSetter mem_tracker_setter(tracker);
    CHECK(current.mem_tracker() == tracker);

    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Limit total live bytes per iteration to keep large realloc sizes manageable.
    const size_t base_size = std::max(old_size, new_size);
    const size_t max_ops_by_bytes = kReallocTargetBytes / base_size;
    const size_t ops_per_iteration = std::max<size_t>(
            1, std::min<size_t>(static_cast<size_t>(kOpsPerIteration), max_ops_by_bytes));

    Alloc allocator;
    std::vector<void*> ptrs(ops_per_iteration);

    auto init_ptrs = [&]() {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = allocator.alloc(old_size);
            CHECK(ptr != nullptr);
            ptrs[i] = ptr;
        }
    };

    state.PauseTiming();
    init_ptrs();
    state.ResumeTiming();

    for (auto _ : state) {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* old_ptr = ptrs[i];
            void* new_ptr = allocator.alloc(new_size);
            CHECK(new_ptr != nullptr);
            std::memcpy(new_ptr, old_ptr, old_size);
            allocator.free(old_ptr, old_size);
            benchmark::DoNotOptimize(new_ptr);
            ptrs[i] = new_ptr;
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* old_ptr = ptrs[i];
            void* new_ptr = allocator.alloc(old_size);
            CHECK(new_ptr != nullptr);
            std::memcpy(new_ptr, old_ptr, old_size);
            allocator.free(old_ptr, new_size);
            ptrs[i] = new_ptr;
        }
        state.ResumeTiming();
    }

    state.PauseTiming();
    for (void* ptr : ptrs) {
        allocator.free(ptr, old_size);
    }
    state.ResumeTiming();

    // Flush cached counters before reporting.
    current.mem_tracker_ctx_shift();

    const double ops = static_cast<double>(state.iterations()) * ops_per_iteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * new_size));
    state.counters["s_per_op"] =
            benchmark::Counter(ops_per_iteration, benchmark::Counter::kIsIterationInvariantRate |
                                                          benchmark::Counter::kInvert);
}

void* jemalloc_realloc_xallocx_or_malloc(void* ptr, size_t old_size, size_t new_size) {
    if (old_size == new_size) {
        return ptr;
    }
    if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
        return ptr;
    }
    void* new_ptr = je_malloc(new_size);
    if (new_ptr == nullptr) {
        return nullptr;
    }
    std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
    je_free(ptr);
    return new_ptr;
}

void BM_jemalloc_realloc_xallocx_grow(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Limit total live bytes per iteration to keep large realloc sizes manageable.
    const size_t base_size = std::max(old_size, new_size);
    const size_t max_ops_by_bytes = kReallocTargetBytes / base_size;
    const size_t ops_per_iteration = std::max<size_t>(
            1, std::min<size_t>(static_cast<size_t>(kOpsPerIteration), max_ops_by_bytes));

    std::vector<void*> ptrs(ops_per_iteration);

    auto init_ptrs = [&]() {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = je_malloc(old_size);
            CHECK(ptr != nullptr);
            ptrs[i] = ptr;
        }
    };

    state.PauseTiming();
    init_ptrs();
    state.ResumeTiming();

    for (auto _ : state) {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = jemalloc_realloc_xallocx_or_malloc(ptrs[i], old_size, new_size);
            CHECK(ptr != nullptr);
            benchmark::DoNotOptimize(ptr);
            ptrs[i] = ptr;
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        for (void* ptr : ptrs) {
            je_free(ptr);
        }
        init_ptrs();
        state.ResumeTiming();
    }

    state.PauseTiming();
    for (void* ptr : ptrs) {
        je_free(ptr);
    }
    state.ResumeTiming();

    const double ops = static_cast<double>(state.iterations()) * ops_per_iteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * new_size));
    state.counters["s_per_op"] =
            benchmark::Counter(ops_per_iteration, benchmark::Counter::kIsIterationInvariantRate |
                                                          benchmark::Counter::kInvert);
}

void BM_jemalloc_realloc_je_realloc_grow(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Limit total live bytes per iteration to keep large realloc sizes manageable.
    const size_t base_size = std::max(old_size, new_size);
    const size_t max_ops_by_bytes = kReallocTargetBytes / base_size;
    const size_t ops_per_iteration = std::max<size_t>(
            1, std::min<size_t>(static_cast<size_t>(kOpsPerIteration), max_ops_by_bytes));

    std::vector<void*> ptrs(ops_per_iteration);

    auto init_ptrs = [&]() {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = je_malloc(old_size);
            CHECK(ptr != nullptr);
            ptrs[i] = ptr;
        }
    };

    state.PauseTiming();
    init_ptrs();
    state.ResumeTiming();

    for (auto _ : state) {
        for (size_t i = 0; i < ops_per_iteration; ++i) {
            void* ptr = je_realloc(ptrs[i], new_size);
            CHECK(ptr != nullptr);
            benchmark::DoNotOptimize(ptr);
            ptrs[i] = ptr;
        }
        benchmark::ClobberMemory();

        state.PauseTiming();
        for (void* ptr : ptrs) {
            je_free(ptr);
        }
        init_ptrs();
        state.ResumeTiming();
    }

    state.PauseTiming();
    for (void* ptr : ptrs) {
        je_free(ptr);
    }
    state.ResumeTiming();

    const double ops = static_cast<double>(state.iterations()) * ops_per_iteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * new_size));
    state.counters["s_per_op"] =
            benchmark::Counter(ops_per_iteration, benchmark::Counter::kIsIterationInvariantRate |
                                                          benchmark::Counter::kInvert);
}

void process_args(benchmark::internal::Benchmark* b) {
    std::vector<int> threads_list = {1, 2, 4, 8, 16, 32, 64};
    for (auto threads : threads_list) {
        b->Threads(threads);
    }
}

void process_realloc_args(benchmark::internal::Benchmark* b) {
    b->ArgNames({"old_size", "new_size"});
    int64_t old_size = 8;
    constexpr int64_t kMaxOldSize = 128LL * 1024 * 1024;
    while (old_size <= kMaxOldSize) {
        b->Args({old_size, old_size * 2});
        old_size *= 8;
    }
}

} // namespace

using DefaultTrackedAllocator = memory::TrackedAllocator<memory::JemallocAllocator<false>>;
using JemallocAllocator = memory::JemallocAllocator<false>;
using MallocAllocator = memory::MallocAllocator<false>;

BENCHMARK_TEMPLATE(BM_allocator_alloc_free, DefaultTrackedAllocator)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, JemallocAllocator)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, MallocAllocator)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_realloc_grow, DefaultTrackedAllocator)->Apply(process_realloc_args);
BENCHMARK_TEMPLATE(BM_allocator_realloc_grow, JemallocAllocator)->Apply(process_realloc_args);
BENCHMARK_TEMPLATE(BM_allocator_realloc_grow, MallocAllocator)->Apply(process_realloc_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_memcpy_free_grow, DefaultTrackedAllocator)->Apply(process_realloc_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_memcpy_free_grow, JemallocAllocator)->Apply(process_realloc_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_memcpy_free_grow, MallocAllocator)->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_xallocx_grow)->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_je_realloc_grow)->Apply(process_realloc_args);

} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::ensure_global_env();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
