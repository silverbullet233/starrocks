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

#include "runtime/memory/allocator_v2.h"

#include <cstring>

#include "common/config.h"
#include "runtime/current_thread.h"
#include <jemalloc/jemalloc.h>

namespace starrocks::memory {

void try_consume_memory(size_t size) {
    if (LIKELY(starrocks::tls_is_thread_status_init)) {
        if (UNLIKELY(!starrocks::tls_thread_status.try_mem_consume(size))) {
            throw std::bad_alloc();
        }
    } else {
        if (UNLIKELY(!starrocks::CurrentThread::try_mem_consume_without_cache(size))) {
            throw std::bad_alloc();
        }
    }
}

void release_memory(size_t size) {
    if (LIKELY(starrocks::tls_is_thread_status_init)) {
        starrocks::tls_thread_status.mem_release(size);
    } else {
        starrocks::CurrentThread::mem_release_without_cache(size);
    }
}

template <bool clear_memory, bool use_mmap, bool populate>
void* BaseAllocator<clear_memory, use_mmap, populate>::alloc(size_t size, size_t alignment) {
    int64_t alloc_size = je_nallocx(size, 0);
    try_consume_memory(alloc_size);

    void* ret = nullptr;
    if (use_mmap && size >= config::mmap_bytes_threshold) {
    } else {
        if (alignment <= MALLOC_MIN_ALIGNMENT) {
            if constexpr (clear_memory) {
                ret = je_calloc(size, 1);
            } else {
                ret = je_malloc(size);
            }
            if (UNLIKELY(ret == nullptr)) {
                release_memory(alloc_size);
                throw std::bad_alloc();
            }
        } else {
            int res = je_posix_memalign(&ret, alignment, size);
            if (res != 0) {
                release_memory(alloc_size);
                throw std::bad_alloc();
            }
            if constexpr (clear_memory) {
                memset(ret, 0, size);
            }
        }
    }
    return ret;
}

template <bool clear_memory, bool use_mmap, bool populate>
void* BaseAllocator<clear_memory, use_mmap, populate>::realloc(void* ptr, size_t old_size, size_t new_size,
                                                           size_t alignment) {
    if (old_size == new_size) {
        return ptr;
    }
    int64_t old_alloc_size = je_nallocx(old_size, 0);
    int64_t new_alloc_size = je_nallocx(new_size, 0);
    int64_t delta = new_alloc_size - old_alloc_size;
    if (delta > 0) {
        try_consume_memory(delta);
    }

    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        ret = je_realloc(ptr, new_size);
    } else {
        int res = je_posix_memalign(&ret, alignment, new_size);
        if (res == 0 && ptr != nullptr) {
            size_t copy_size = old_size < new_size ? old_size : new_size;
            memcpy(ret, ptr, copy_size);
            je_free(ptr);
        }
    }

    if (UNLIKELY(ret == nullptr)) {
        if (delta > 0) {
            release_memory(delta);
        }
        throw std::bad_alloc();
    }

    if constexpr (clear_memory) {
        if (new_size > old_size) {
            memset(static_cast<char*>(ret) + old_size, 0, new_size - old_size);
        }
    }
    if (delta < 0) {
        release_memory(-delta);
    }

    return ret;
}

template <bool clear_memory, bool use_mmap, bool populate>
void BaseAllocator<clear_memory, use_mmap, populate>::free(void* ptr, size_t size) {
    if (ptr == nullptr) {
        return;
    }
    size_t alloc_size = je_malloc_usable_size(ptr);
    je_free(ptr);
    release_memory(alloc_size);
}

template class BaseAllocator<false, false, false>;
template class BaseAllocator<true, false, false>;

} // namespace starrocks::memory
