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
#include <testutil/assert.h>

#include <cassert>
#include <memory>
#include <string>

#include "column/datum_tuple.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "runtime/chunk_cursor.h"
#include "runtime/current_thread.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/mem_hook_allocator.h"
#include "runtime/memory/roaring_hook.h"
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"
#include "types/bitmap_value_detail.h"
#include "util/random.h"
#include "runtime/memory/roaring_hook.h"
#include "runtime/memory/counting_allocator.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {

class JemallocArenaAllocator final: public AllocatorFactory<Allocator, JemallocArenaAllocator> {
// @TODO use a sperate arena
public:
    JemallocArenaAllocator() {
        // allocate anrea
        size_t len = sizeof(_arena_idx);
        if (auto ret = je_mallctl("arenas.create", &_arena_idx, &len, nullptr, 0 )) {
            // LOG(INFO) << "create arena failed, " << ret;
        }

        _flags = MALLOCX_ARENA(_arena_idx) | MALLOCX_TCACHE_NONE;
    }

    ~JemallocArenaAllocator() override {
        std::string key = "arenas." + std::to_string(_arena_idx) + ".destroy";
        if (auto ret = je_mallctl(key.c_str(), nullptr, nullptr, nullptr, 0)) {
            // LOG(INFO) << "destroy arena failed, " << ret;
        }
    }

    void* alloc(size_t size) override {
        void* result = je_mallocx(size, _flags);
        return result;
    }
    void free(void* ptr) override {
        if (UNLIKELY(ptr == nullptr)) {
            return;
        }
        je_dallocx(ptr, _flags);
    }

    void* realloc(void* ptr, size_t size) override {
        if (LIKELY(ptr != nullptr && size != 0)) {
            return je_rallocx(ptr, size, _flags);
        } else if (ptr != nullptr && size == 0) {
            return nullptr;
        }
        return je_mallocx(size, _flags);
    }

    void* calloc(size_t n, size_t size) override {
        void* result = alloc(n * size);
        memset(result, 0, n * size);
        return result;
    }

    void cfree(void* ptr) override {
        CHECK(false);
    }

    void* memalign(size_t align, size_t size) override {
        CHECK(false);
        return nullptr;
    }

    void* aligned_alloc(size_t align, size_t size) override {
        CHECK(false) << "not implement";
        return nullptr;
    }

    void* valloc(size_t size) override {
        CHECK(false);
        return nullptr;
    }

    void* pvalloc(size_t size) override {
        CHECK(false);
        return nullptr;
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        CHECK(false);
        return 0;
    }
private:
    unsigned _arena_idx;
    int _flags = 0;
};

template<class Base>
class StackAllocator final: public AllocatorFactory<Base, StackAllocator<Base>> {
// batch alloc, batch commit
public:
    struct MemChunk {
        uint8_t* data = nullptr;
        size_t capacity = 0;
        size_t allocated_size = 0;
    };
    static const size_t kChunkSize = 1024 * 1024 * 2;
    StackAllocator() {
        MemChunk chunk;
        chunk.data = reinterpret_cast<uint8_t*>(Base::alloc(kChunkSize));
        chunk.capacity = kChunkSize;
        chunk.allocated_size = 0;
        _mem_chunks.push_back(chunk);
    }

    ~StackAllocator() override {
        for (auto& mem_chunk: _mem_chunks) {
            if (mem_chunk.data) {
                Base::free(mem_chunk.data);
            }
        }
        _mem_chunks.clear();
    }

    void* alloc(size_t size) override {
        // LOG(INFO) << "alloc " << size << ", chunks " << _mem_chunks.size();
        auto& chunk = _mem_chunks.back();
        if (LIKELY(chunk.allocated_size + size <= chunk.capacity)) {
            void* result = chunk.data + chunk.allocated_size;
            chunk.allocated_size += size;
            return result;
        }
        size_t new_chunk_size = std::max(size, kChunkSize);
        MemChunk new_chunk;
        new_chunk.data = reinterpret_cast<uint8_t*>(Base::alloc(kChunkSize));
        new_chunk.capacity = new_chunk_size;
        new_chunk.allocated_size = size;
        _mem_chunks.emplace_back(new_chunk);
        return new_chunk.data;
    }
    void free(void* ptr) override {
        // LOG(INFO) << "free";
        // do nothing
    }

    void* realloc(void* ptr, size_t size) override {
        return alloc(size);
    }

    void* calloc(size_t n, size_t size) override {
        void* result = alloc(n * size);
        memset(result, 0, n * size);
        return result;
    }

    void cfree(void* ptr) override {
        CHECK(false);
    }

    void* memalign(size_t align, size_t size) override {
        CHECK(false);
        return nullptr;
    }

    void* aligned_alloc(size_t align, size_t size) override {
        CHECK(false) << "not implement";
        return nullptr;
        // auto& mem_chunk = _mem_chunks.back();
        // void* result = std::align(align, size, mem_chunk.data + mem_chunk.allocated_size, mem_chunk.capacity - mem_chunk.allocated_size);
        // if (result == nullptr) {
        //     // @TODO
        // }
        // mem_chunk.allocated_size += size;
        // return result;
    }

    void* valloc(size_t size) override {
        CHECK(false);
        return nullptr;
    }

    void* pvalloc(size_t size) override {
        CHECK(false);
        return nullptr;
    }

    int posix_memalign(void** ptr, size_t align, size_t size) override {
        CHECK(false);
        return 0;
        // int result = Base::posix_memalign(ptr, align, size);
        // return result;
    }
private:
    std::vector<MemChunk> _mem_chunks;
};

using StackAllocatorWithHook = StackAllocator<MemHookAllocator>;

class RoaringBitmapAllocTest {
public:
    void SetUp() {}
    void TearDown() {}

    RoaringBitmapAllocTest(size_t start, size_t end, size_t shift_width):
        _start(start), _end(end), _shift_width(shift_width) {}
    ~RoaringBitmapAllocTest() {
        {
            ThreadLocalRoaringAllocatorSetter setter(_allocator.get());
            _bitmap.reset();
        }
        _allocator.reset();
    }
    template<class Alloc>
    void do_bench(benchmark::State& state);
private:
    std::unique_ptr<Allocator> _allocator;
    std::unique_ptr<detail::Roaring64Map> _bitmap;

    size_t _start = 0;
    size_t _end = 0;
    size_t _shift_width = 0;
};

//@TODO impelement a stack memory allocator?

template <class Alloc>
void RoaringBitmapAllocTest::do_bench(benchmark::State& state) {
    init_roaring_hook();
    _allocator = std::make_unique<Alloc>();
    ThreadLocalRoaringAllocatorSetter setter(_allocator.get());
    _bitmap = std::make_unique<detail::Roaring64Map>();
    state.ResumeTiming();
    for (size_t i = _start; i < _end; i++) {
        _bitmap->add(i << _shift_width);
        // LOG(INFO) << "add " << i;
    }
    state.PauseTiming();
}

static void BM_mem_hook_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&]() {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<MemHookAllocator>(state);
            });
        }
        for (auto& thread: threads) {
            thread.join();
        }
   }
}

static void BM_counting_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&] () {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<CountingAllocatorWithHook>(state);
            });
        }
        for(auto& thread: threads) {
            thread.join();
        }
   }
}

static void BM_stack_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&] () {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<StackAllocatorWithHook>(state);
            });
        }
        for(auto& thread: threads) {
            thread.join();
        }
   }
}
static void BM_arena_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&] () {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<JemallocArenaAllocator>(state);
            });
        }
        for(auto& thread: threads) {
            thread.join();
        }
   }
}

static void process_args(benchmark::internal::Benchmark* b) {
    int64_t start = 0;
    int64_t end = 1 << 20;
    int64_t iterations = 2;
    // std::vector<size_t> shift_width = {0, 1, 2, 4, 8, 16, 32};
    std::vector<size_t> shift_width = {32};
    for (auto i : shift_width) {
        b->Args({start, end, i})->Iterations(iterations);
    }
}

BENCHMARK(BM_mem_hook_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);
BENCHMARK(BM_counting_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);
BENCHMARK(BM_stack_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);
BENCHMARK(BM_arena_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);

} // namespace starrocks

BENCHMARK_MAIN();