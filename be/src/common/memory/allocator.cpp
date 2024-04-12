// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common/memory/allocator.h"
#include "common/compiler_util.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {

// void* NoMemHookAllocator::alloc(size_t size) {
//     return je_malloc(size);
// }

// void* NoMemHookAllocator::aligned_alloc(size_t align, size_t size) {
//     return je_aligned_alloc(align, size);
// }

// void NoMemHookAllocator::free(void* ptr) {
//     if (UNLIKELY(ptr == nullptr)) {
//         return;
//     }
//     je_free(ptr);
// }

// template<typename T>
// T* NoMemHookAllocator<T>::allocate(size_t n) {
//     return static_cast<T*>(je_malloc(n * sizeof(T)));
// }

// template<typename T>
// void NoMemHookAllocator<T>::deallocate(T* ptr, size_t n) {
//     je_free(ptr);
// }

}