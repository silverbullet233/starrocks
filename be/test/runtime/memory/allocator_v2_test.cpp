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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

namespace starrocks::memory {

TEST(BaseAllocatorTest, AllocAndFree) {
    BaseAllocator<false, false, false> allocator;
    void* ptr = allocator.alloc(64);
    ASSERT_NE(ptr, nullptr);
    std::memset(ptr, 0xAB, 64);
    allocator.free(ptr, 64);

    allocator.free(nullptr, 0);
}

TEST(BaseAllocatorTest, ReallocPreservesAndZeroes) {
    BaseAllocator<true, false, false> allocator;
    auto* ptr = static_cast<char*>(allocator.alloc(8));
    std::memset(ptr, 1, 8);

    auto* grown = static_cast<char*>(allocator.realloc(ptr, 8, 16));
    ASSERT_NE(grown, nullptr);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(grown[i], 1);
    }
    for (int i = 8; i < 16; ++i) {
        EXPECT_EQ(grown[i], 0);
    }

    auto* shrunk = static_cast<char*>(allocator.realloc(grown, 16, 4));
    ASSERT_NE(shrunk, nullptr);
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(shrunk[i], 1);
    }
    allocator.free(shrunk, 4);
}

TEST(BaseAllocatorTest, Alignment) {
    BaseAllocator<false, false, false> allocator;
    void* ptr = allocator.alloc(32, 64);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(ptr) % 64, 0);
    allocator.free(ptr, 32);
}

} // namespace starrocks::memory
