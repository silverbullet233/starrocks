#include "util/buffer.h"

#include "fmt/format.h"

namespace starrocks {


ALWAYS_INLINE size_t element_bytes(size_t element_size, size_t element_num) {
    size_t bytes;
    if(__builtin_mul_overflow(element_size, element_num, &bytes)) {
        throw std::runtime_error(fmt::format("element_bytes overflow: element_size={}, element_num={}", element_size, element_num));
    }
    return bytes;
}

ALWAYS_INLINE size_t element_bytes_with_padding(size_t element_num, size_t element_size, size_t padding) {
    size_t bytes;
    if(__builtin_mul_overflow(element_bytes(element_size, element_num), padding, &bytes)) {
        throw std::runtime_error(fmt::format("get_allocated_bytes overflow: element_num={}, element_size={}, padding={}", element_num, element_size, padding));
    }
    return bytes;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::release(memory::Allocator* allocator) {
    if (_start == nullptr) {
        return;
    }
    allocator->free(reinterpret_cast<void*>(_start), allocated_bytes());

    _start = nullptr;
    _end = nullptr;
    _end_of_storage = nullptr;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::reserve(size_t new_cap, memory::Allocator* allocator) {
    if (capacity() >= new_cap) {
        return;
    }

    size_t new_allocated_bytes = element_bytes_with_padding(new_cap, kElementSize, kPadding);
    if (_start == nullptr) {
        _start = reinterpret_cast<uint8_t*>(allocator->alloc(new_allocated_bytes));
        _end = _start;
        _end_of_storage = _start + new_allocated_bytes - kPadding;
        return;
    }
    size_t old_allocated_bytes = allocated_bytes();
    size_t old_used_bytes = used_bytes();
    _start = reinterpret_cast<uint8_t*>(allocator->realloc(reinterpret_cast<void*>(_start), old_allocated_bytes, new_allocated_bytes));
    _end = _start + old_used_bytes;
    _end_of_storage = _start + new_allocated_bytes - kPadding;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::shrink_to_fit(memory::Allocator* allocator) {
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::resize(size_t new_size, memory::Allocator* allocator) {
    if (new_size > capacity()) {
        reserve(new_size, allocator);
    }
    _end = _start + new_size * kElementSize;
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(size_t new_size, const T& value, memory::Allocator* allocator) {

}

template <class T, size_t padding>
template <class InputIt>
void RawBuffer<T, padding>::assign(InputIt, InputIt, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::assign(std::initializer_list<T>, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(const T& value, memory::Allocator* memory) {
    // @TODO what if is_initialized == false ?
    if (_start + kElementSize >= _end_of_storage) {
        reserve(capacity() + 1, memory);
    }
    *reinterpret_cast<T*>(_end) = value;
    _end += kElementSize;
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::push_back(T&&, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
template <class... Args>
T& RawBuffer<T, padding>::emplace_back(memory::Allocator*, Args&&...) {
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::pop_back() {
    __builtin_unreachable();
}

template <class T, size_t padding>
T* RawBuffer<T, padding>::insert(const T*, const T&, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
T* RawBuffer<T, padding>::insert(const T*, T&&, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
template <class InputIt>
T* RawBuffer<T, padding>::insert(const T*, InputIt, InputIt, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
T* RawBuffer<T, padding>::insert(const T*, std::initializer_list<T>, memory::Allocator*) {
    __builtin_unreachable();
}

template <class T, size_t padding>
void RawBuffer<T, padding>::swap(RawBuffer&) noexcept {
    __builtin_unreachable();
}

template <class T, size_t padding>
Buffer<T, padding>::Buffer(Buffer&&) noexcept = default;

template <class T, size_t padding>
Buffer<T, padding>& Buffer<T, padding>::operator=(Buffer&&) noexcept = default;

template <class T, size_t padding>
void Buffer<T, padding>::release() {
    RawBuffer<T, padding>::release(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::reserve(size_t new_cap) {
    RawBuffer<T, padding>::reserve(new_cap, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::shrink_to_fit() {
    RawBuffer<T, padding>::shrink_to_fit(this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::resize(size_t count) {
    RawBuffer<T, padding>::resize(count, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(size_t count, const T& value) {
    RawBuffer<T, padding>::assign(count, value, this->_allocator);
}

template <class T, size_t padding>
template <class InputIt>
void Buffer<T, padding>::assign(InputIt first, InputIt last) {
    RawBuffer<T, padding>::assign(first, last, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::assign(std::initializer_list<T> ilist) {
    RawBuffer<T, padding>::assign(ilist, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(const T& value) {
    RawBuffer<T, padding>::push_back(value, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::push_back(T&& value) {
    RawBuffer<T, padding>::push_back(std::move(value), this->_allocator);
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
T* Buffer<T, padding>::insert(const T* pos, const T& value) {
    return RawBuffer<T, padding>::insert(pos, value, this->_allocator);
}

template <class T, size_t padding>
T* Buffer<T, padding>::insert(const T* pos, T&& value) {
    return RawBuffer<T, padding>::insert(pos, std::move(value), this->_allocator);
}

template <class T, size_t padding>
template <class InputIt>
T* Buffer<T, padding>::insert(const T* pos, InputIt first, InputIt last) {
    return RawBuffer<T, padding>::insert(pos, first, last, this->_allocator);
}

template <class T, size_t padding>
T* Buffer<T, padding>::insert(const T* pos, std::initializer_list<T> ilist) {
    return RawBuffer<T, padding>::insert(pos, ilist, this->_allocator);
}

template <class T, size_t padding>
void Buffer<T, padding>::swap(Buffer& other) noexcept {
    RawBuffer<T, padding>::swap(other);
    std::swap(this->_allocator, other._allocator);
}

} // namespace starrocks
