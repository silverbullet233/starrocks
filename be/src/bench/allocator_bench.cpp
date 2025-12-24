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

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/allocator_v2.h"

namespace starrocks {

namespace {

constexpr int64_t kBytesPerOp = 1024;
constexpr int kOpsPerIteration = 100000;

void ensure_global_env() {
    if (!GlobalEnv::is_init()) {
        auto tracker = std::make_shared<MemTracker>(-1, "allocator_bench_root");
        auto status = GlobalEnv::GetInstance()->init_for_bench(std::move(tracker));
        CHECK(status.ok()) << status.to_string();
    }
}

class MallocAllocator : public memory::AllocatorFactory<memory::Allocator, MallocAllocator> {
public:
    ~MallocAllocator() override = default;

    void* alloc(size_t size, size_t alignment = 0) override {
        if (UNLIKELY(size == 0)) {
            return nullptr;
        }
        void* ret = nullptr;
        if (alignment <= memory::MALLOC_MIN_ALIGNMENT) {
            ret = ::malloc(size);
        } else {
            if (posix_memalign(&ret, alignment, size) != 0) {
                ret = nullptr;
            }
        }
        return ret;
    }

    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override {
        if (alignment <= memory::MALLOC_MIN_ALIGNMENT) {
            return ::realloc(ptr, new_size);
        }
        void* ret = alloc(new_size, alignment);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }
        if (ptr != nullptr && old_size > 0) {
            std::memcpy(ret, ptr, std::min(old_size, new_size));
            ::free(ptr);
        }
        return ret;
    }

    void free(void* ptr, size_t /*size*/) override {
        if (UNLIKELY(ptr == nullptr)) {
            return;
        }
        ::free(ptr);
    }

    int64_t nallox(size_t size, int /*flags*/ = 0) const override { return static_cast<int64_t>(size); }
};

template <typename Alloc>
void BM_allocator_alloc_free(benchmark::State& state) {
    ensure_global_env();
    MemTracker* tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    // Prepare TLS and bind to process tracker.
    CurrentThread& current = CurrentThread::current();
    CurrentThreadMemTrackerSetter mem_tracker_setter(tracker);
    CHECK(current.mem_tracker() == tracker);

    Alloc allocator;
    std::vector<void*> ptrs;
    ptrs.reserve(kOpsPerIteration);

    for (auto _ : state) {
        ptrs.clear();
        for (int i = 0; i < kOpsPerIteration; ++i) {
            void* ptr = allocator.alloc(kBytesPerOp);
            CHECK(ptr != nullptr);
            benchmark::DoNotOptimize(ptr);
            ptrs.push_back(ptr);
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

void process_args(benchmark::internal::Benchmark* b) {
    std::vector<int> threads_list = {1, 2, 4, 8, 16, 32, 64};
    for (auto threads : threads_list) {
        b->Threads(threads)->Iterations(10);
    }
}

} // namespace

using DefaultTrackedAllocator = memory::TrackedAllocator<memory::JemallocAllocator<false>>;
using JemallocAllocator = memory::JemallocAllocator<false>;

BENCHMARK_TEMPLATE(BM_allocator_alloc_free, DefaultTrackedAllocator)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, JemallocAllocator)->Apply(process_args);
BENCHMARK_TEMPLATE(BM_allocator_alloc_free, MallocAllocator)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();

