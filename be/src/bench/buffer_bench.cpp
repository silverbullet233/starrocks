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
#include <string>
#include <vector>

#include "runtime/memory/allocator_v2.h"
#include "runtime/memory/column_allocator.h"
#include "util/buffer.h"
#include "util/raw_container.h"
#include "runtime/current_thread.h"

namespace starrocks {

namespace {

struct SmallNonPod {
    SmallNonPod() = default;
    SmallNonPod(int64_t id, int payload_len) : value(id), payload(payload_len, 'x') {}
    int64_t value{0};
    std::string payload;
};

template <typename T>
using ColumnVector = std::vector<T, ColumnAllocator<T>>;
template <typename T, size_t padding = 0>
using Buffer = util::Buffer<T, padding>;

template <typename MakeContainer>
void run_ctor_bench(benchmark::State& state, MakeContainer make_container) {
    SCOPED_SET_CATCHED(true);
    for (auto _ : state) {
        auto container = make_container();
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(state.iterations());
}

template <typename MakeContainer>
void run_ctor_count_bench(benchmark::State& state, MakeContainer make_container) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = make_container(n);
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename ReserveFn>
void run_reserve_bench(benchmark::State& state, MakeContainer make_container, ReserveFn reserve_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = make_container();
        reserve_fn(container, n);
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename ReserveFn, typename PushFn>
void run_push_bench(benchmark::State& state, MakeContainer make_container, ReserveFn reserve_fn, PushFn push_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = make_container();
        reserve_fn(container, n);
        for (int64_t i = 0; i < n; ++i) {
            push_fn(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename PrepareFn, typename PushFn>
void run_push_bench_prepared(benchmark::State& state, MakeContainer make_container, PrepareFn prepare_fn,
                             PushFn push_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = make_container();
        prepare_fn(container, n);
        state.ResumeTiming();
        for (int64_t i = 0; i < n; ++i) {
            push_fn(container, i);
        }
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename ResizeFn>
void run_resize_bench(benchmark::State& state, MakeContainer make_container, ResizeFn resize_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = make_container();
        resize_fn(container, n);
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename AssignFn>
void run_assign_bench(benchmark::State& state, MakeContainer make_container, AssignFn assign_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        auto container = make_container();
        assign_fn(container, n);
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename ReserveFn, typename PushFn, typename ShrinkFn>
void run_shrink_to_fit_bench(benchmark::State& state, MakeContainer make_container, ReserveFn reserve_fn,
                             PushFn push_fn, ShrinkFn shrink_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    for (auto _ : state) {
        state.PauseTiming();
        auto container = make_container();
        reserve_fn(container, n);
        for (int64_t i = 0; i < n / 2; ++i) {
            push_fn(container, i);
        }
        state.ResumeTiming();
        shrink_fn(container);
        benchmark::DoNotOptimize(container);
    }
    state.SetItemsProcessed(n * state.iterations());
}

template <typename MakeContainer, typename AccessFn>
void run_iterate_sum_bench(benchmark::State& state, MakeContainer make_container, AccessFn access_fn) {
    SCOPED_SET_CATCHED(true);
    const int64_t n = state.range(0);
    auto container = make_container(n);
    for (auto _ : state) {
        int64_t sum = 0;
        for (int64_t i = 0; i < n; ++i) {
            sum += access_fn(container, i);
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(n * state.iterations());
}

void apply_counts(benchmark::internal::Benchmark* b) {
    for (int64_t n : {16, 128, 1024, 4096}) {
        b->Arg(n);
    }
}

// Constructors (Buffer 3 ctors: explicit, move ctor, move assign)
static void BM_buffer_default_ctor(benchmark::State& state) {
    run_ctor_bench(state, [] { return Buffer<int>(&memory::kDefaultAllocator); });
}

static void BM_vector_default_ctor(benchmark::State& state) {
    run_ctor_bench(state, [] { return ColumnVector<int>(); });
}

static void BM_rawvector_ctor(benchmark::State& state) {
    run_ctor_bench(state, [] { return raw::RawVector<int>(); });
}

static void BM_buffer_ctor_count(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return Buffer<int>(&memory::kDefaultAllocator, n); });
}

static void BM_vector_ctor_count(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return ColumnVector<int>(n); });
}

static void BM_rawvector_ctor_count(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return raw::RawVector<int>(n); });
}

static void BM_buffer_ctor_count_value(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return Buffer<int>(&memory::kDefaultAllocator, n, 1); });
}

static void BM_vector_ctor_count_value(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return ColumnVector<int>(n, 1); });
}

static void BM_rawvector_ctor_count_value(benchmark::State& state) {
    run_ctor_count_bench(state, [](int64_t n) { return raw::RawVector<int>(n, 1); });
}

static void BM_buffer_move_ctor(benchmark::State& state) {
    run_ctor_bench(state, [] {
        Buffer<int> src(&memory::kDefaultAllocator);
        src.reserve(8);
        src.push_back(1);
        return Buffer<int>(std::move(src));
    });
}

static void BM_buffer_move_assign(benchmark::State& state) {
    run_ctor_bench(state, [] {
        Buffer<int> src(&memory::kDefaultAllocator);
        src.reserve(8);
        src.push_back(1);
        Buffer<int> dst(&memory::kDefaultAllocator);
        dst = std::move(src);
        return dst;
    });
}

// Reserve
static void BM_buffer_reserve(benchmark::State& state) {
    run_reserve_bench(state, [&] { return Buffer<int>(&memory::kDefaultAllocator); },
                      [](auto& c, int64_t n) { c.reserve(n); });
}

static void BM_vector_reserve(benchmark::State& state) {
    run_reserve_bench(state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); });
}

static void BM_rawvector_reserve(benchmark::State& state) {
    run_reserve_bench(state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); });
}

static void BM_std_string_reserve(benchmark::State& state) {
    run_reserve_bench(state, [] { return std::string(); }, [](auto& c, int64_t n) { c.reserve(n); });
}

static void BM_rawstring_reserve(benchmark::State& state) {
    run_reserve_bench(state, [] { return raw::RawString(); }, [](auto& c, int64_t n) { c.reserve(n); });
}

// POD push_back
static void BM_buffer_int_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state,
                   [] { return Buffer<int>(&memory::kDefaultAllocator); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

static void BM_buffer_int_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state,
                            [] { return Buffer<int>(&memory::kDefaultAllocator); },
                            [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

static void BM_vector_int_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state, [] { return ColumnVector<int>(); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

static void BM_vector_int_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

static void BM_rawvector_int_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state, [] { return raw::RawVector<int>(); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

static void BM_rawvector_int_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); });
}

// Non-POD push_back
static void BM_buffer_nonpod_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state,
                   [] { return Buffer<SmallNonPod>(&memory::kDefaultAllocator); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, int64_t i) { c.push_back(SmallNonPod{i, 16}); });
}

static void BM_buffer_nonpod_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return Buffer<SmallNonPod>(&memory::kDefaultAllocator); },
                            [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, int64_t i) { c.push_back(SmallNonPod{i, 16}); });
}

static void BM_vector_nonpod_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state, [] { return ColumnVector<SmallNonPod>(); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, int64_t i) { c.push_back(SmallNonPod{i, 16}); });
}

static void BM_vector_nonpod_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return ColumnVector<SmallNonPod>(); }, [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, int64_t i) { c.push_back(SmallNonPod{i, 16}); });
}

// RawVector cannot hold non-trivial types (RawAllocator requires trivial destructor), so skipped.

// String push_back (character)
static void BM_std_string_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state, [] { return std::string(); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, [[maybe_unused]] int64_t i) { c.push_back('x'); });
}

static void BM_std_string_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return std::string(); }, [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, [[maybe_unused]] int64_t i) { c.push_back('x'); });
}

static void BM_rawstring_push_back_no_reserve(benchmark::State& state) {
    run_push_bench(state, [] { return raw::RawString(); },
                   []([[maybe_unused]] auto& c, [[maybe_unused]] int64_t n) {},
                   [](auto& c, [[maybe_unused]] int64_t i) { c.push_back('x'); });
}

static void BM_rawstring_push_back_reserve(benchmark::State& state) {
    run_push_bench_prepared(state, [] { return raw::RawString(); }, [](auto& c, int64_t n) { c.reserve(n); },
                            [](auto& c, [[maybe_unused]] int64_t i) { c.push_back('x'); });
}

// Resize
static void BM_buffer_resize(benchmark::State& state) {
    run_resize_bench(state, [] { return Buffer<int>(&memory::kDefaultAllocator); },
                     [](auto& c, int64_t n) { c.resize(n); });
}

static void BM_vector_resize(benchmark::State& state) {
    run_resize_bench(state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.resize(n); });
}

static void BM_rawvector_resize(benchmark::State& state) {
    run_resize_bench(state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.resize(n); });
}

static void BM_buffer_resize_value(benchmark::State& state) {
    run_resize_bench(state, [] { return Buffer<int>(&memory::kDefaultAllocator); },
                     [](auto& c, int64_t n) { c.resize(n, 1); });
}

static void BM_vector_resize_value(benchmark::State& state) {
    run_resize_bench(state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.resize(n, 1); });
}

static void BM_rawvector_resize_value(benchmark::State& state) {
    run_resize_bench(state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.resize(n, 1); });
}

// Assign
static void BM_buffer_assign(benchmark::State& state) {
    run_assign_bench(state, [] { return Buffer<int>(&memory::kDefaultAllocator); },
                     [](auto& c, int64_t n) { c.assign(n, 1); });
}

static void BM_vector_assign(benchmark::State& state) {
    run_assign_bench(state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.assign(n, 1); });
}

static void BM_rawvector_assign(benchmark::State& state) {
    run_assign_bench(state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.assign(n, 1); });
}

// Shrink to fit
static void BM_buffer_shrink_to_fit(benchmark::State& state) {
    run_shrink_to_fit_bench(
            state, [] { return Buffer<int>(&memory::kDefaultAllocator); },
            [](auto& c, int64_t n) { c.reserve(n); }, [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); },
            [](auto& c) { c.shrink_to_fit(); });
}

static void BM_vector_shrink_to_fit(benchmark::State& state) {
    run_shrink_to_fit_bench(
            state, [] { return ColumnVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); },
            [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); }, [](auto& c) { c.shrink_to_fit(); });
}

static void BM_rawvector_shrink_to_fit(benchmark::State& state) {
    run_shrink_to_fit_bench(
            state, [] { return raw::RawVector<int>(); }, [](auto& c, int64_t n) { c.reserve(n); },
            [](auto& c, int64_t i) { c.push_back(static_cast<int>(i)); }, [](auto& c) { c.shrink_to_fit(); });
}

// Iterate sum
static void BM_buffer_iterate_sum(benchmark::State& state) {
    run_iterate_sum_bench(
            state,
            [](int64_t n) {
                Buffer<int> c(&memory::kDefaultAllocator);
                c.reserve(n);
                for (int64_t i = 0; i < n; ++i) {
                    c.push_back(static_cast<int>(i));
                }
                return c;
            },
            [](auto& c, int64_t i) { return c[i]; });
}

static void BM_vector_iterate_sum(benchmark::State& state) {
    run_iterate_sum_bench(
            state,
            [](int64_t n) {
                ColumnVector<int> c;
                c.reserve(n);
                for (int64_t i = 0; i < n; ++i) {
                    c.push_back(static_cast<int>(i));
                }
                return c;
            },
            [](auto& c, int64_t i) { return c[i]; });
}

static void BM_rawvector_iterate_sum(benchmark::State& state) {
    run_iterate_sum_bench(
            state,
            [](int64_t n) {
                raw::RawVector<int> c;
                c.reserve(n);
                for (int64_t i = 0; i < n; ++i) {
                    c.push_back(static_cast<int>(i));
                }
                return c;
            },
            [](auto& c, int64_t i) { return c[i]; });
}

} // namespace

BENCHMARK(BM_buffer_default_ctor);
BENCHMARK(BM_vector_default_ctor);
BENCHMARK(BM_rawvector_ctor);
BENCHMARK(BM_buffer_ctor_count)->Apply(apply_counts);
BENCHMARK(BM_vector_ctor_count)->Apply(apply_counts);
BENCHMARK(BM_rawvector_ctor_count)->Apply(apply_counts);
BENCHMARK(BM_buffer_ctor_count_value)->Apply(apply_counts);
BENCHMARK(BM_vector_ctor_count_value)->Apply(apply_counts);
BENCHMARK(BM_rawvector_ctor_count_value)->Apply(apply_counts);
BENCHMARK(BM_buffer_move_ctor);
BENCHMARK(BM_buffer_move_assign);

BENCHMARK(BM_buffer_reserve)->Apply(apply_counts);
BENCHMARK(BM_vector_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawvector_reserve)->Apply(apply_counts);
BENCHMARK(BM_std_string_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawstring_reserve)->Apply(apply_counts);

BENCHMARK(BM_buffer_int_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_buffer_int_push_back_reserve)->Apply(apply_counts);
BENCHMARK(BM_vector_int_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_vector_int_push_back_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawvector_int_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawvector_int_push_back_reserve)->Apply(apply_counts);

BENCHMARK(BM_buffer_nonpod_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_buffer_nonpod_push_back_reserve)->Apply(apply_counts);
BENCHMARK(BM_vector_nonpod_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_vector_nonpod_push_back_reserve)->Apply(apply_counts);

BENCHMARK(BM_std_string_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_std_string_push_back_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawstring_push_back_no_reserve)->Apply(apply_counts);
BENCHMARK(BM_rawstring_push_back_reserve)->Apply(apply_counts);

BENCHMARK(BM_buffer_resize)->Apply(apply_counts);
BENCHMARK(BM_vector_resize)->Apply(apply_counts);
BENCHMARK(BM_rawvector_resize)->Apply(apply_counts);
BENCHMARK(BM_buffer_resize_value)->Apply(apply_counts);
BENCHMARK(BM_vector_resize_value)->Apply(apply_counts);
BENCHMARK(BM_rawvector_resize_value)->Apply(apply_counts);

BENCHMARK(BM_buffer_assign)->Apply(apply_counts);
BENCHMARK(BM_vector_assign)->Apply(apply_counts);
BENCHMARK(BM_rawvector_assign)->Apply(apply_counts);

BENCHMARK(BM_buffer_shrink_to_fit)->Apply(apply_counts);
BENCHMARK(BM_vector_shrink_to_fit)->Apply(apply_counts);
BENCHMARK(BM_rawvector_shrink_to_fit)->Apply(apply_counts);

BENCHMARK(BM_buffer_iterate_sum)->Apply(apply_counts);
BENCHMARK(BM_vector_iterate_sum)->Apply(apply_counts);
BENCHMARK(BM_rawvector_iterate_sum)->Apply(apply_counts);


int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}

} // namespace starrocks
