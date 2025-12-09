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

#include "runtime/mem_tracker.h"

namespace starrocks {

// Shared tracker tree for all threads in the benchmark.
static MemTracker* shared_tracker() {
    // Root has no limit to avoid accidental limit hits.
    static MemTracker root_tracker(1024 * 1024 * 1024, "mem_tracker_bench_root");
    // Child tracker is the one the benchmark exercises.
    // static MemTracker bench_tracker(1024 * 1024 * 1024, "mem_tracker_bench_child", &root_tracker);
    // return &bench_tracker;
    return &root_tracker;
}

static void BM_memtracker_try_consume(benchmark::State& state) {
    constexpr int64_t kBytesPerOp = 256;
    constexpr int kOpsPerIteration = 10000;
    MemTracker* tracker = shared_tracker();

    // Reset consumption before each run to keep iterations comparable.
    if (state.thread_index == 0) {
        tracker->set(0);
        if (tracker->parent() != nullptr) {
            tracker->parent()->set(0);
        }
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            auto* failed = tracker->try_consume(kBytesPerOp);
            // try_consume should always succeed because limits are disabled.
            DCHECK(failed == nullptr);
            tracker->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * kBytesPerOp));
    // seconds per try_consume+release
    state.counters["s_per_op"] =
            benchmark::Counter(kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate |
                                                      benchmark::Counter::kInvert);
}

static void BM_memtracker_try_consume_without_update(benchmark::State& state) {
    constexpr int64_t kBytesPerOp = 256;
    constexpr int kOpsPerIteration = 10000;
    MemTracker* tracker = shared_tracker();

    if (state.thread_index == 0) {
        tracker->set(0);
        if (tracker->parent() != nullptr) {
            tracker->parent()->set(0);
        }
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            auto* failed = tracker->try_consume_without_update(kBytesPerOp);
            DCHECK(failed == nullptr);
            tracker->release_without_update(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.SetBytesProcessed(static_cast<int64_t>(ops * kBytesPerOp));
    state.counters["s_per_op"] =
            benchmark::Counter(kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate |
                                                      benchmark::Counter::kInvert);
}

static void process_args(benchmark::internal::Benchmark* b) {
    std::vector<int> threads_list = {1, 2, 4, 8, 16, 32, 64};

    for (auto threads : threads_list) {
        b->Threads(threads)->Iterations(10);
    }
}

BENCHMARK(BM_memtracker_try_consume)->Apply(process_args);
BENCHMARK(BM_memtracker_try_consume_without_update)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();

