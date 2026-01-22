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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "gutil/macros.h"
#include "util/stack_util.h"
#include "runtime/memory/allocator_v2.h"

namespace starrocks::util {

#if defined(__AVX512F__)
    constexpr size_t SIMD_PADDING_BYTES = 64;
#elif defined(__AVX2__)
    constexpr size_t SIMD_PADDING_BYTES = 32;
#else
    constexpr size_t SIMD_PADDING_BYTES = 16;
#endif

static constexpr size_t empty_raw_buffer_size = 1024;
alignas(std::max_align_t) extern uint8_t empty_raw_buffer[empty_raw_buffer_size];

// Helper functions for element size calculations
inline size_t element_bytes(size_t element_size, size_t element_num) {
    size_t bytes;
    if (__builtin_mul_overflow(element_size, element_num, &bytes)) {
        throw std::runtime_error(
                fmt::format("element_bytes overflow: element_size={}, element_num={}", element_size, element_num));
    }
    return bytes;
}

inline size_t element_bytes_with_padding(size_t element_num, size_t element_size, size_t padding) {
    size_t base_bytes = element_bytes(element_size, element_num);
    size_t bytes;
    if (__builtin_add_overflow(base_bytes, padding, &bytes)) {
        throw std::runtime_error(fmt::format("element_bytes_with_padding overflow: element_num={}, element_size={}, "
                                              "padding={}",
                                              element_num, element_size, padding));
    }
    return bytes;
}

template <class T, size_t padding>
class RawBuffer {
public:
    static constexpr size_t kElementSize = sizeof(T);
    static constexpr size_t kPadding = ((padding + kElementSize - 1) / kElementSize) * kElementSize;
    static constexpr uint8_t* null = empty_raw_buffer;

    using value_type = T;
    using iterator = T*;
    using const_iterator = const T*;

    RawBuffer() = default;
    RawBuffer(RawBuffer&& rhs) noexcept;
    RawBuffer& operator=(RawBuffer&&) noexcept;
    DISALLOW_COPY(RawBuffer);
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
    const T& front() const {
        DCHECK(!empty());
        return *begin();
    }
    T& front() {
        DCHECK(!empty());
        return *begin();
    }
    const T& back() const {
        DCHECK(!empty());
        return *(end() - 1);
    }
    T& back() {
        DCHECK(!empty());
        return *(end() - 1);
    }

    bool is_initialized() const {
        return _start != null;
    }

    void clear() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            for (T* ptr = begin(); ptr != end(); ptr++) {
                ptr->~T();
            }
        }
        _end = _start;
    }
    void release(memory::Allocator* allocator);
    void reserve(memory::Allocator* allocator, size_t new_cap);
    void shrink_to_fit(memory::Allocator* allocator);
    void resize(memory::Allocator* allocator, size_t count);
    void resize(memory::Allocator* allocator, size_t count, const T& value);

    void assign(memory::Allocator* allocator, size_t count, const T& value);
    template <class InputIt,
              std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool> = true>
    void assign(memory::Allocator* allocator, InputIt first, InputIt last);
    void assign(memory::Allocator* allocator, std::initializer_list<T> ilist);

    // @TODO should we support const T& ?
    void push_back(memory::Allocator* allocator, const T& value);
    void push_back(memory::Allocator* allocator, T&& value);
    template <class... Args>
    T& emplace_back(memory::Allocator* allocator, Args&&... args);
    void pop_back();

    template <class InputIt, std::enable_if_t<std::is_base_of_v<std::input_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool> = true>
    iterator insert(memory::Allocator* allocator, InputIt first, InputIt last);
    iterator insert(memory::Allocator* allocator, std::initializer_list<T> ilist);
    iterator insert(memory::Allocator* allocator, size_t count, const T& value);

    void swap(RawBuffer& other) noexcept;

private:
    void relocate(memory::Allocator* allocator, size_t new_size, size_t new_allocated_bytes);

protected:
    uint8_t* _start = null;
    uint8_t* _end = null;
    uint8_t* _end_of_storage = null;
};

template <class T, size_t padding = 0>
class Buffer: public RawBuffer<T, padding> {
public:
    using iterator = typename RawBuffer<T, padding>::iterator;
    using const_iterator = typename RawBuffer<T, padding>::const_iterator;
    using value_type = typename RawBuffer<T, padding>::value_type;

    Buffer() = delete;
    explicit Buffer(memory::Allocator* allocator) : _allocator(allocator) {}
    Buffer(memory::Allocator* allocator, size_t count) : _allocator(allocator) {
        this->resize(count);
    }
    Buffer(memory::Allocator* allocator, size_t count, const T& value) : _allocator(allocator) {
        this->assign(count, value);
    }
    Buffer(Buffer&&) noexcept;
    Buffer& operator=(Buffer&&) noexcept;
    DISALLOW_COPY(Buffer);
    ~Buffer();

    void release();
    void reserve(size_t new_cap);
    void shrink_to_fit();
    void resize(size_t count);
    void resize(size_t count, const T& value);
    void assign(size_t count, const T& value);
    template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool> = true>
    void assign(InputIt first, InputIt last);
    void assign(std::initializer_list<T> ilist);

    void push_back(const T& value);
    void push_back(T&& value);
    template <class... Args>
    T& emplace_back(Args&&... args);
    void pop_back();

    template <class InputIt, std::enable_if_t<std::is_base_of_v<std::input_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool> = true>
    iterator insert(InputIt first, InputIt last);
    iterator insert(std::initializer_list<T> ilist);
    iterator insert(size_t count, const T& value);

    void swap(Buffer& other) noexcept;

    memory::Allocator* allocator() const { return _allocator; }

private:
    memory::Allocator* _allocator = nullptr;
};

template <class T, size_t padding>
RawBuffer<T, padding>::RawBuffer(RawBuffer&& rhs) noexcept {
    std::swap(_start, rhs._start);
    std::swap(_end, rhs._end);
    std::swap(_end_of_storage, rhs._end_of_storage);
}

template <class T, size_t padding>
RawBuffer<T, padding>& RawBuffer<T, padding>::operator=(RawBuffer&& rhs) noexcept {
    if (this != &rhs) {
        std::swap(_start, rhs._start);
        std::swap(_end, rhs._end);
        std::swap(_end_of_storage, rhs._end_of_storage);
    }
    return *this;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::release(memory::Allocator* allocator) {
    if (_start == null) {
        return;
    }

    if constexpr (!std::is_trivially_destructible_v<T>) {
        for (T* ptr = begin(); ptr != end(); ptr++) {
            ptr->~T();
        }
    }
    allocator->free(reinterpret_cast<void*>(_start), allocated_bytes());

    _start = null;
    _end = null;
    _end_of_storage = null;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::relocate(memory::Allocator* allocator, size_t new_size, size_t new_allocated_bytes) {
    size_t old_allocated_bytes = allocated_bytes();
    size_t old_used_bytes = used_bytes();

    if constexpr (std::is_trivially_copyable_v<T>) {
        uint8_t* new_start = reinterpret_cast<uint8_t*>(allocator->realloc(reinterpret_cast<void*>(_start), old_allocated_bytes, new_allocated_bytes));
        // if (new_start == _start) {
        //     LOG(INFO) << "real realloc, new_size:" << new_allocated_bytes << ", old_size:" << old_allocated_bytes
        //         << ", stack: " << starrocks::get_stack_trace();
        // }
        _start = new_start;
        _end = _start + old_used_bytes;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
    } else {
        uint8_t* new_start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
        T* new_data = reinterpret_cast<T*>(new_start);
        T* old_data = reinterpret_cast<T*>(_start);
        size_t old_size = size();
        size_t i = 0;
        try {
            for (; i < old_size; ++i) {
                new (new_data + i) T(std::move_if_noexcept(old_data[i]));
            }
        } catch (...) {
            for (size_t j = 0; j < i; ++j) {
                new_data[j].~T();
            }
            allocator->free(reinterpret_cast<void*>(new_start), new_allocated_bytes);
            throw;
        }
        for (size_t j = 0; j < old_size; ++j) {
            old_data[j].~T();
        }
        allocator->free(reinterpret_cast<void*>(_start), old_allocated_bytes);
        _start = new_start;
        _end = _start + old_used_bytes;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
    }
}

template <class T, size_t padding>
void RawBuffer<T, padding>::reserve(memory::Allocator* allocator, size_t new_cap) {
    if (capacity() >= new_cap) {
        return;
    }

    size_t new_allocated_bytes;
    if constexpr (padding > 0) {
        new_allocated_bytes = element_bytes_with_padding(new_cap, kElementSize, kPadding);
    } else {
        new_allocated_bytes = element_bytes(kElementSize, new_cap);
    }
    if (_start == null) {
        _start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
        _end = _start;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
        return;
    }
    // @TODO
    relocate(allocator, new_cap, new_allocated_bytes);
}

template <class T, size_t padding>
void RawBuffer<T, padding>::shrink_to_fit(memory::Allocator* allocator) {
    if (_end == _end_of_storage) {
        return;
    }

    size_t new_size = size();
    size_t new_allocated_bytes;
    if constexpr (padding > 0) {
        new_allocated_bytes = element_bytes_with_padding(new_size, kElementSize, kPadding);
    } else {
        new_allocated_bytes = element_bytes(kElementSize, new_size);
    }
    relocate(allocator, new_size, new_allocated_bytes);
}

template <class T, size_t padding>
void RawBuffer<T, padding>::resize(memory::Allocator* allocator, size_t new_size) {
    size_t old_size = size();
    if (new_size > capacity()) {
        reserve(allocator, new_size);
    }

    if constexpr (!std::is_trivially_destructible_v<T>) {
        T* ptr = data();
        for (size_t i = new_size; i < old_size; ++i) {
            ptr[i].~T();
        }
    }
    if constexpr (!std::is_trivially_copyable_v<T>) {
        T* ptr = data();
        for (size_t i = old_size; i < new_size; ++i) {
            new (ptr + i) T();
        }
    }

    _end = _start + new_size * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::resize(memory::Allocator* allocator, size_t new_size, const T& value) {
    size_t old_size = size();
    if (new_size > capacity()) {
        reserve(allocator, new_size);
    }
    if constexpr (!std::is_trivially_destructible_v<T>) {
        T* ptr = data();
        for (size_t i = new_size; i < old_size; ++i) {
            ptr[i].~T();
        }
    }

    if (new_size > old_size) {
        size_t grow = new_size - old_size;
        if constexpr (std::is_trivially_copyable_v<T>) {
            std::fill(end(), end() + grow, value);
        } else {
            std::uninitialized_fill_n(end(), grow, value);
        }
    }
    _end = _start + new_size * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, size_t count, const T& value) {
    if (count > capacity()) {
        reserve(allocator, count);
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
        std::fill(begin(), begin() + count, value);
    } else {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            for (T* ptr = begin(); ptr != end(); ptr++) {
                ptr->~T();
            }
        }
        std::uninitialized_fill_n(begin(), count, value);
    }
    _end = _start + count * kElementSize;
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool>>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, InputIt first, InputIt last) {
    size_t count = std::distance(first, last);
    if (count > capacity()) {
        reserve(allocator, count);
    }

    if constexpr (!std::is_trivially_destructible_v<T>) {
        for (T* ptr = begin(); ptr != end(); ptr++) {
            ptr->~T();
        }
    }

    T* ptr = data();
    if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<InputIt>) {
        memcpy(ptr, reinterpret_cast<const void*>(first), count * kElementSize);
    } else {
        std::uninitialized_copy(first, last, ptr);
    }
    _end = _start + count * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(memory::Allocator* allocator, std::initializer_list<T> ilist) {
    assign(allocator, ilist.begin(), ilist.end());
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(memory::Allocator* allocator, const T& value) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        size_t new_cap = empty() ? 1 : capacity() * 2;
        reserve(allocator, new_cap);
    }
    new (end()) T(value);
    _end += kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(memory::Allocator* allocator, T&& value) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        size_t new_cap = empty() ? 1 : capacity() * 2;
        reserve(allocator, new_cap);
    }
    new (end()) T(std::move(value));
    _end += kElementSize;
}

template <class T, size_t padding>
template <class... Args>
T& RawBuffer<T, padding>::emplace_back(memory::Allocator* allocator, Args&&... args) {
    if (UNLIKELY(_end + kElementSize > _end_of_storage)) {
        size_t new_cap = empty() ? 1 : capacity() * 2;
        reserve(allocator, new_cap);
    }
    T* ptr = end();
    new (ptr) T(std::forward<Args>(args)...);
    _end += kElementSize;
    return *ptr;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::pop_back() {
    if (empty()) {
        return;
    }
    _end -= kElementSize;
    if constexpr (!std::is_trivially_destructible_v<T>) {
        end()->~T();
    }
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::input_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool>>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::insert(memory::Allocator* allocator, InputIt first, InputIt last) {
    size_t count = std::distance(first, last);
    if (UNLIKELY(count == 0)) {
        return end();
    }
    size_t old_size = size();
    if (old_size + count > capacity()) {
        size_t new_cap = empty() ? count : std::max(capacity() * 2, old_size + count);
        reserve(allocator, new_cap);
    }
    T* insert_pos = reinterpret_cast<T*>(_end);
    // Insert new elements at the end
    if constexpr (std::is_trivially_copyable_v<T> && std::is_pointer_v<InputIt>) {
        memcpy(insert_pos, reinterpret_cast<const void*>(first), count * kElementSize);
    } else {
        size_t idx = 0;
        for (InputIt it = first; it != last; ++it, ++idx) {
            new (insert_pos + idx) T(*it);
        }
    }
    _end += count * kElementSize;
    return insert_pos;
}

template <class T, size_t padding>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::insert(memory::Allocator* allocator, std::initializer_list<T> ilist) {
    return insert(allocator, ilist.begin(), ilist.end());
}

template <class T, size_t padding>
typename RawBuffer<T, padding>::iterator RawBuffer<T, padding>::insert(memory::Allocator* allocator, size_t count, const T& value) {
    if (UNLIKELY(count == 0)) {
        return end();
    }
    size_t old_size = size();
    if (old_size + count > capacity()) {
        size_t new_cap = (capacity() == 0) ? count : std::max(capacity() * 2, old_size + count);
        reserve(allocator, new_cap);
    }
    T* insert_pos = end();
    // Insert new elements at the end
    for (size_t i = 0; i < count; ++i) {
        new (insert_pos + i) T(value);
    }
    _end += count * kElementSize;
    return insert_pos;
}


template <class T, size_t padding>
void RawBuffer<T, padding>::swap(RawBuffer& other) noexcept {
    std::swap(_start, other._start);
    std::swap(_end, other._end);
    std::swap(_end_of_storage, other._end_of_storage);
}

template <class T, size_t padding>
Buffer<T, padding>::Buffer(Buffer&& other) noexcept {
    swap(other);
}

template <class T, size_t padding>
Buffer<T, padding>::~Buffer() {
    release();
    _allocator = nullptr;
}

template <class T, size_t padding>
Buffer<T, padding>& Buffer<T, padding>::operator=(Buffer&& other) noexcept {
    swap(other);
    return *this;
}

template <class T, size_t padding>
void Buffer<T, padding>::release() {
    RawBuffer<T, padding>::release(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::reserve(size_t new_cap) {
    RawBuffer<T, padding>::reserve(this->_allocator, new_cap);
}

template <class T, size_t padding>
void Buffer<T, padding>::shrink_to_fit() {
    RawBuffer<T, padding>::shrink_to_fit(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::resize(size_t count) {
    RawBuffer<T, padding>::resize(this->_allocator, count);
}

template <class T, size_t padding>
void Buffer<T, padding>::resize(size_t count, const T& value) {
    RawBuffer<T, padding>::resize(this->_allocator, count, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(size_t count, const T& value) {
    RawBuffer<T, padding>::assign(this->_allocator, count, value);
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool>>
void Buffer<T, padding>::assign(InputIt first, InputIt last) {
    RawBuffer<T, padding>::assign(this->_allocator, first, last);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(std::initializer_list<T> ilist) {
    RawBuffer<T, padding>::assign(this->_allocator, ilist);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(const T& value) {
    RawBuffer<T, padding>::push_back(this->_allocator, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(T&& value) {
    RawBuffer<T, padding>::push_back(this->_allocator, std::move(value));
}

template <class T, size_t padding>
template <class... Args>
T& Buffer<T, padding>::emplace_back(Args&&... args) {
    return RawBuffer<T, padding>::emplace_back(this->_allocator, std::forward<Args>(args)...);
}

template <class T, size_t padding>
void Buffer<T, padding>::pop_back() {
    RawBuffer<T, padding>::pop_back();
}

template <class T, size_t padding>
template <class InputIt, std::enable_if_t<std::is_base_of_v<std::input_iterator_tag, typename std::iterator_traits<InputIt>::iterator_category>, bool>>
typename Buffer<T, padding>::iterator Buffer<T, padding>::insert(InputIt first, InputIt last) {
    return RawBuffer<T, padding>::insert(this->_allocator, first, last);
}

template <class T, size_t padding>
typename Buffer<T, padding>::iterator Buffer<T, padding>::insert(std::initializer_list<T> ilist) {
    return RawBuffer<T, padding>::insert(this->_allocator, ilist);
}

template <class T, size_t padding>
typename Buffer<T, padding>::iterator Buffer<T, padding>::insert(size_t count, const T& value) {
    return RawBuffer<T, padding>::insert(this->_allocator, count, value);
}

template <class T, size_t padding>
void Buffer<T, padding>::swap(Buffer& other) noexcept {
    RawBuffer<T, padding>::swap(other);
    std::swap(this->_allocator, other._allocator);
}

} // namespace starrocks::util
