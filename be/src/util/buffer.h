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
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include "runtime/memory/allocator_v2.h"

namespace starrocks {

#if defined(__AVX512F__)
    constexpr size_t SIMD_PADDING_BYTES = 64;
#elif defined(__AVX2__)
    constexpr size_t SIMD_PADDING_BYTES = 32;
#else
    constexpr size_t SIMD_PADDING_BYTES = 16;
#endif

template <class T, size_t padding>
class RawBuffer {
public:
    static constexpr size_t kElementSize = sizeof(T);
    static constexpr size_t kPadding = ((padding + kElementSize - 1) / kElementSize) * kElementSize;

    using iterator = T*;
    using const_iterator = const T*;

    RawBuffer() = default;
    RawBuffer(RawBuffer&& rhs) noexcept;
    RawBuffer& operator=(RawBuffer&&) noexcept;
    RawBuffer(const RawBuffer&) = delete;
    RawBuffer& operator=(const RawBuffer&) = delete;
    ~RawBuffer() = default;

    T* data() { return reinterpret_cast<T*>(_start); }
    const T* data() const { return reinterpret_cast<const T*>(_start); }
    iterator begin() { return reinterpret_cast<T*>(_start); }
    const_iterator begin() const { return reinterpret_cast<const T*>(_start); }
    iterator end() { return reinterpret_cast<T*>(_end); }
    const_iterator end() const {
        return reinterpret_cast<const T*>(_end);
    }
    bool empty() const { return _start == _end; }
    size_t size() const { return (_end - _start) / kElementSize; }
    size_t capacity() const { return (_end_of_storage - _start) / kElementSize; }
    size_t allocated_bytes() const { return _end_of_storage - _start + kPadding; }
    size_t used_bytes() const { return _end - _start; }

    T& operator[](size_t idx) { return data()[idx]; }
    const T& operator[](size_t idx) const { return data()[idx]; }

    bool is_initialized() const {
        return _start != nullptr && _end != nullptr && _end_of_storage != nullptr;
    }

    void clear() { _end = _start; }
    void release(memory::Allocator* allocator);
    void reserve(size_t new_cap, memory::Allocator* allocator);
    void shrink_to_fit(memory::Allocator* allocator);
    void resize(size_t count, memory::Allocator* allocator);

    void assign(size_t count, const T& value, memory::Allocator* allocator);
    template <class InputIt>
    void assign(InputIt first, InputIt last, memory::Allocator* allocator);
    void assign(std::initializer_list<T> ilist, memory::Allocator* allocator);

    // @TODO should we support const T& ?
    void push_back(const T& value, memory::Allocator* allocator);
    void push_back(T&& value, memory::Allocator* allocator);
    template <class... Args>
    T& emplace_back(memory::Allocator* allocator, Args&&... args);
    void pop_back();

    iterator insert(const T* pos, const T& value, memory::Allocator* allocator);
    iterator insert(const T* pos, T&& value, memory::Allocator* allocator);
    template <class InputIt>
    iterator insert(const T* pos, InputIt first, InputIt last, memory::Allocator* allocator);
    iterator insert(const T* pos, std::initializer_list<T> ilist, memory::Allocator* allocator);

    void swap(RawBuffer& other) noexcept;

protected:

    uint8_t* _start = nullptr;
    uint8_t* _end = nullptr;
    uint8_t* _end_of_storage = nullptr;
};

template <class T, size_t padding = 16>
class Buffer : protected RawBuffer<T, padding> {
public:
    using iterator = typename RawBuffer<T, padding>::iterator;
    using const_iterator = typename RawBuffer<T, padding>::const_iterator;  

    explicit Buffer(memory::Allocator* allocator) : _allocator(allocator) {}
    Buffer(Buffer&&) noexcept;
    Buffer& operator=(Buffer&&) noexcept;
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;
    ~Buffer() = default;

    void release();
    void reserve(size_t new_cap);
    void shrink_to_fit();
    void resize(size_t count);
    void assign(size_t count, const T& value);
    template <class InputIt>
    void assign(InputIt first, InputIt last);
    void assign(std::initializer_list<T> ilist);

    void push_back(const T& value);
    void push_back(T&& value);
    template <class... Args>
    T& emplace_back(Args&&... args);
    void pop_back();

    iterator insert(const T* pos, const T& value);
    iterator insert(const T* pos, T&& value);
    template <class InputIt>
    iterator insert(const T* pos, InputIt first, InputIt last);
    iterator insert(const T* pos, std::initializer_list<T> ilist);

    void swap(Buffer& other) noexcept;

private:
    memory::Allocator* _allocator = nullptr;
};

}