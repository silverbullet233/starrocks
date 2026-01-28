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

#include "util/buffer.h"

#include <gtest/gtest.h>
#include <vector>

#include "runtime/memory/memory_allocator.h"

namespace starrocks {

class BufferTest : public testing::Test {
public:
    BufferTest() = default;
    ~BufferTest() override = default;

protected:
    memory::JemallocAllocator<false> _allocator;
};
template <class T, size_t padding>
using RawBuffer = util::RawBuffer<T, padding>;
template <class T, size_t padding>
using Buffer = util::Buffer<T, padding>;



// Test RawBuffer basic operations
TEST_F(BufferTest, RawBufferBasic) {
    RawBuffer<int, 16> buf;
    
    // Initially empty and not initialized
    ASSERT_TRUE(buf.empty());
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(0, buf.capacity());
    ASSERT_FALSE(buf.is_initialized());
    
    // Reserve some capacity
    buf.reserve(&_allocator, 10);
    ASSERT_TRUE(buf.is_initialized());
    ASSERT_EQ(0, buf.size());
    ASSERT_GE(buf.capacity(), 10);
}

// Test RawBuffer push_back and pop_back
TEST_F(BufferTest, RawBufferPushPop) {
    RawBuffer<int, 16> buf;
    
    // Push back elements
    for (int i = 0; i < 10; ++i) {
        buf.push_back(&_allocator, i);
        ASSERT_EQ(i + 1, buf.size());
        ASSERT_EQ(i, buf[i]);
    }
    
    // Pop back elements
    for (int i = 9; i >= 0; --i) {
        ASSERT_EQ(i, buf[i]);
        buf.pop_back();
        ASSERT_EQ(i, buf.size());
    }
    
    ASSERT_TRUE(buf.empty());
}

// Test RawBuffer emplace_back
TEST_F(BufferTest, RawBufferEmplaceBack) {
    RawBuffer<std::pair<int, int>, 16> buf;
    
    for (int i = 0; i < 5; ++i) {
        auto& ref = buf.emplace_back(&_allocator, i, i * 2);
        ASSERT_EQ(i, ref.first);
        ASSERT_EQ(i * 2, ref.second);
        ASSERT_EQ(i + 1, buf.size());
    }
}

// Test RawBuffer reserve and resize
TEST_F(BufferTest, RawBufferReserveResize) {
    RawBuffer<int, 16> buf;
    
    // Reserve capacity
    buf.reserve(&_allocator, 20);
    size_t cap = buf.capacity();
    ASSERT_GE(cap, 20);
    
    // Resize to smaller
    buf.resize(&_allocator, 10);
    ASSERT_EQ(10, buf.size());
    ASSERT_GE(buf.capacity(), 10);
    
    // Resize to larger
    buf.resize(&_allocator, 15);
    ASSERT_EQ(15, buf.size());
}

// Test RawBuffer assign
TEST_F(BufferTest, RawBufferAssign) {
    RawBuffer<int, 16> buf;
    
    // Assign with count and value
    buf.assign(&_allocator, 5, 42);
    ASSERT_EQ(5, buf.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        ASSERT_EQ(42, buf[i]);
    }
    
    // Assign with iterator range
    std::vector<int> vec = {1, 2, 3, 4, 5};
    buf.assign(&_allocator, vec.begin(), vec.end());
    ASSERT_EQ(5, buf.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        ASSERT_EQ(static_cast<int>(i + 1), buf[i]);
    }
    
    // Assign with initializer_list
    buf.assign(&_allocator, {10, 20, 30});
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(10, buf[0]);
    ASSERT_EQ(20, buf[1]);
    ASSERT_EQ(30, buf[2]);
}

// Test RawBuffer insert
TEST_F(BufferTest, RawBufferInsert) {
    RawBuffer<int, 16> buf;
    
    // Insert at end
    buf.push_back(&_allocator, 1);
    buf.push_back(&_allocator, 3);
    auto it = buf.insert(&_allocator, 1, 0);
    ASSERT_EQ(0, *it);
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    
    // Insert at end again
    it = buf.insert(&_allocator, 1, 2);
    ASSERT_EQ(2, *it);
    ASSERT_EQ(4, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    ASSERT_EQ(2, buf[3]);
    
    // Insert with initializer_list at end
    buf.insert(&_allocator, {10, 11});
    ASSERT_EQ(6, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    ASSERT_EQ(2, buf[3]);
    ASSERT_EQ(10, buf[4]);
    ASSERT_EQ(11, buf[5]);
}

// Test RawBuffer swap
TEST_F(BufferTest, RawBufferSwap) {
    RawBuffer<int, 16> buf1, buf2;
    
    for (int i = 0; i < 5; ++i) {
        buf1.push_back(&_allocator, i);
    }
    
    size_t size1 = buf1.size();
    size_t cap1 = buf1.capacity();
    
    buf1.swap(buf2);
    
    ASSERT_EQ(0, buf1.size());
    ASSERT_EQ(size1, buf2.size());
    ASSERT_EQ(cap1, buf2.capacity());
    
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(i, buf2[i]);
    }
}

// Test RawBuffer clear
TEST_F(BufferTest, RawBufferClear) {
    RawBuffer<int, 16> buf;
    
    for (int i = 0; i < 10; ++i) {
        buf.push_back(&_allocator, i);
    }
    
    ASSERT_EQ(10, buf.size());
    size_t cap = buf.capacity();
    
    buf.clear();
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(cap, buf.capacity()); // Capacity should remain
}

// Test RawBuffer shrink_to_fit
TEST_F(BufferTest, RawBufferShrinkToFit) {
    RawBuffer<int, 16> buf;
    
    buf.reserve(&_allocator, 100);
    size_t large_cap = buf.capacity();
    
    for (int i = 0; i < 10; ++i) {
        buf.push_back(&_allocator, i);
    }
    
    buf.shrink_to_fit(&_allocator);
    ASSERT_EQ(10, buf.size());
    ASSERT_LE(buf.capacity(), large_cap);
    ASSERT_GE(buf.capacity(), 10);
}

// Test RawBuffer release
TEST_F(BufferTest, RawBufferRelease) {
    RawBuffer<int, 16> buf;
    
    buf.reserve(&_allocator, 10);
    ASSERT_TRUE(buf.is_initialized());
    
    buf.release(&_allocator);
    ASSERT_FALSE(buf.is_initialized());
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(0, buf.capacity());
}

// Test Buffer basic operations
TEST_F(BufferTest, BufferBasic) {
    Buffer<int, 16> buf(&_allocator);
    
    ASSERT_TRUE(buf.empty());
    ASSERT_EQ(0, buf.size());
    
    // Push back elements
    for (int i = 0; i < 10; ++i) {
        buf.push_back(i);
        ASSERT_EQ(i + 1, buf.size());
        ASSERT_EQ(i, buf[i]);
    }
}

// Test Buffer reserve and resize
TEST_F(BufferTest, BufferReserveResize) {
    Buffer<int, 16> buf(&_allocator);
    
    buf.reserve(20);
    ASSERT_GE(buf.capacity(), 20);
    
    buf.resize(10);
    ASSERT_EQ(10, buf.size());
    
    buf.resize(15);
    ASSERT_EQ(15, buf.size());
}

// Test Buffer assign
TEST_F(BufferTest, BufferAssign) {
    Buffer<int, 16> buf(&_allocator);
    
    buf.assign(5, 42);
    ASSERT_EQ(5, buf.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        ASSERT_EQ(42, buf[i]);
    }
    
    std::vector<int> vec = {1, 2, 3};
    buf.assign(vec.begin(), vec.end());
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(2, buf[1]);
    ASSERT_EQ(3, buf[2]);
    
    buf.assign({10, 20});
    ASSERT_EQ(2, buf.size());
    ASSERT_EQ(10, buf[0]);
    ASSERT_EQ(20, buf[1]);
}

// Test Buffer insert
TEST_F(BufferTest, BufferInsert) {
    Buffer<int, 16> buf(&_allocator);
    
    buf.push_back(1);
    buf.push_back(3);
    
    auto it = buf.insert(1, 0);
    ASSERT_EQ(0, *it);
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    
    it = buf.insert(1, 2);
    ASSERT_EQ(2, *it);
    ASSERT_EQ(4, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    ASSERT_EQ(2, buf[3]);
}

// Test Buffer swap
TEST_F(BufferTest, BufferSwap) {
    Buffer<int, 16> buf1(&_allocator);
    Buffer<int, 16> buf2(&_allocator);
    
    for (int i = 0; i < 5; ++i) {
        buf1.push_back(i);
    }
    
    size_t size1 = buf1.size();
    
    buf1.swap(buf2);
    
    ASSERT_EQ(0, buf1.size());
    ASSERT_EQ(size1, buf2.size());
    
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(i, buf2[i]);
    }
}

// Test Buffer move semantics
TEST_F(BufferTest, BufferMove) {
    Buffer<int, 16> buf1(&_allocator);
    
    for (int i = 0; i < 10; ++i) {
        buf1.push_back(i);
    }
    
    size_t size1 = buf1.size();
    size_t cap1 = buf1.capacity();
    
    Buffer<int, 16> buf2(std::move(buf1));
    
    ASSERT_EQ(0, buf1.size());
    ASSERT_EQ(size1, buf2.size());
    ASSERT_EQ(cap1, buf2.capacity());
    
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(i, buf2[i]);
    }
}

// Test Buffer release
TEST_F(BufferTest, BufferRelease) {
    Buffer<int, 16> buf(&_allocator);
    
    buf.reserve(10);
    ASSERT_GE(buf.capacity(), 10);
    
    buf.release();
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(0, buf.capacity());
}

// Test Buffer with different types
TEST_F(BufferTest, BufferDifferentTypes) {
    Buffer<double, 16> double_buf(&_allocator);
    for (int i = 0; i < 5; ++i) {
        double_buf.push_back(i * 1.5);
    }
    ASSERT_EQ(5, double_buf.size());
    ASSERT_DOUBLE_EQ(0.0, double_buf[0]);
    ASSERT_DOUBLE_EQ(6.0, double_buf[4]);
    
    Buffer<std::string, 16> str_buf(&_allocator);
    str_buf.push_back("hello");
    str_buf.push_back("world");
    ASSERT_EQ(2, str_buf.size());
    ASSERT_EQ("hello", str_buf[0]);
    ASSERT_EQ("world", str_buf[1]);
}

// Test padding parameter
TEST_F(BufferTest, BufferPadding) {
    constexpr size_t element_size = sizeof(int);
    constexpr int num_elements = 10;
    
    // Test different padding values
    // padding = 0: kPadding should be 0 (aligned to element_size)
    RawBuffer<int, 0> buf0;
    for (int i = 0; i < num_elements; ++i) {
        buf0.push_back(&_allocator, i);
    }
    size_t expected_padding0 = ((0 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated0 = buf0.capacity() * element_size + expected_padding0;
    ASSERT_EQ(expected_allocated0, buf0.allocated_bytes());
    size_t padding0 = RawBuffer<int, 0>::kPadding;
    ASSERT_EQ(expected_padding0, padding0);
    
    // padding = 1: kPadding should be aligned to element_size (4 for int)
    RawBuffer<int, 1> buf1;
    for (int i = 0; i < num_elements; ++i) {
        buf1.push_back(&_allocator, i);
    }
    size_t expected_padding1 = ((1 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated1 = buf1.capacity() * element_size + expected_padding1;
    ASSERT_EQ(expected_allocated1, buf1.allocated_bytes());
    size_t padding1 = RawBuffer<int, 1>::kPadding;
    ASSERT_EQ(expected_padding1, padding1);
    ASSERT_EQ(element_size, padding1); // Should be 4 for int
    
    // padding = 8: kPadding should be 8
    RawBuffer<int, 8> buf8;
    for (int i = 0; i < num_elements; ++i) {
        buf8.push_back(&_allocator, i);
    }
    size_t expected_padding8 = ((8 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated8 = buf8.capacity() * element_size + expected_padding8;
    ASSERT_EQ(expected_allocated8, buf8.allocated_bytes());
    size_t padding8 = RawBuffer<int, 8>::kPadding;
    ASSERT_EQ(expected_padding8, padding8);
    ASSERT_EQ(8, padding8);
    
    // padding = 16: kPadding should be 16
    RawBuffer<int, 16> buf16;
    for (int i = 0; i < num_elements; ++i) {
        buf16.push_back(&_allocator, i);
    }
    size_t expected_padding16 = ((16 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated16 = buf16.capacity() * element_size + expected_padding16;
    ASSERT_EQ(expected_allocated16, buf16.allocated_bytes());
    size_t padding16 = RawBuffer<int, 16>::kPadding;
    ASSERT_EQ(expected_padding16, padding16);
    ASSERT_EQ(16, padding16);
    
    // padding = 32: kPadding should be 32
    RawBuffer<int, 32> buf32;
    for (int i = 0; i < num_elements; ++i) {
        buf32.push_back(&_allocator, i);
    }
    size_t expected_padding32 = ((32 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated32 = buf32.capacity() * element_size + expected_padding32;
    ASSERT_EQ(expected_allocated32, buf32.allocated_bytes());
    size_t padding32 = RawBuffer<int, 32>::kPadding;
    ASSERT_EQ(expected_padding32, padding32);
    ASSERT_EQ(32, padding32);
    
    // Verify all buffers have the same size and data
    ASSERT_EQ(num_elements, buf0.size());
    ASSERT_EQ(num_elements, buf1.size());
    ASSERT_EQ(num_elements, buf8.size());
    ASSERT_EQ(num_elements, buf16.size());
    ASSERT_EQ(num_elements, buf32.size());
    
    for (int i = 0; i < num_elements; ++i) {
        ASSERT_EQ(i, buf0[i]);
        ASSERT_EQ(i, buf1[i]);
        ASSERT_EQ(i, buf8[i]);
        ASSERT_EQ(i, buf16[i]);
        ASSERT_EQ(i, buf32[i]);
    }
    
    // Verify allocated_bytes increases with padding
    ASSERT_LE(buf0.allocated_bytes(), buf1.allocated_bytes());
    ASSERT_LE(buf1.allocated_bytes(), buf8.allocated_bytes());
    ASSERT_LE(buf8.allocated_bytes(), buf16.allocated_bytes());
    ASSERT_LE(buf16.allocated_bytes(), buf32.allocated_bytes());
}

} // namespace starrocks

