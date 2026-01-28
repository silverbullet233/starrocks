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
#pragma once

#include <cstdlib>

#include "malloc.h"
#include <atomic>
#include "runtime/memory/allocator.h"
#include <iostream>

namespace starrocks {

static std::atomic<int64_t> g_alloc_count = 0;
static std::atomic<int64_t> g_alloc_size = 0;
static std::atomic<int64_t> g_free_count = 0;

class MemHookAllocator : public AllocatorFactory<Allocator, MemHookAllocator> {
public:
    void* alloc(size_t size) override {
        // std::cout << "alloc size: " << size << std::endl;
        g_alloc_count.fetch_add(size, std::memory_order_relaxed);
        g_alloc_size.fetch_add(size, std::memory_order_relaxed);
        return ::malloc(size);
    }

    void free(void* ptr) override {
        g_free_count.fetch_add(1, std::memory_order_relaxed);
        ::free(ptr);
    }

    void* realloc(void* ptr, size_t size) override { return ::realloc(ptr, size); }

    void* calloc(size_t n, size_t size) override { return ::calloc(n, size); }

    void cfree(void* ptr) override { ::free(ptr); }

    void* memalign(size_t align, size_t size) override { return ::memalign(align, size); }

    void* aligned_alloc(size_t align, size_t size) override { return ::aligned_alloc(align, size); }

    void* valloc(size_t size) override { return ::valloc(size); }

    void* pvalloc(size_t size) override { return ::pvalloc(size); }

    int posix_memalign(void** ptr, size_t align, size_t size) override { return ::posix_memalign(ptr, align, size); }
};
} // namespace starrocks