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

namespace starrocks::memory {

static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

class Allocator {
public:
    virtual ~Allocator() = default;
    virtual void* alloc(size_t size, size_t alignment = 0);
    virtual void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0);
    virtual void free(void* ptr, size_t size);
};

template <class Base, class Derived>
class AllocatorFactory: public Base {
public:
    void* alloc(size_t size, size_t alignment = 0) override {
        return static_cast<Derived*>(this)->alloc(size, alignment);
    }
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override {
        return static_cast<Derived*>(this)->realloc(ptr, old_size, new_size, alignment);
    }
    void free(void* ptr, size_t size) override {
        static_cast<Derived*>(this)->free(ptr, size);
    }
};

template <bool clear_memory, bool use_mmap, bool populate>
class BaseAllocator: public AllocatorFactory<Allocator, BaseAllocator<clear_memory, use_mmap, populate>> {
public:
    virtual ~BaseAllocator() = default;
    virtual void* alloc(size_t size, size_t alignment = 0);
    virtual void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0);
    virtual void free(void* ptr, size_t size);
};

}