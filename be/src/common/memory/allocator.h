#pragma once

#include <memory>
#include "runtime/mem_tracker.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {


template<typename T>
class NoMemHookAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;

    T* allocate(size_t n) {
        return static_cast<T*>(je_malloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        je_free(ptr);
    }

};

template<typename T, typename Alloc = std::allocator<T>>
class MemTrackerAllocator {
public:
    typedef typename Alloc::value_type value_type;
    typedef typename Alloc::size_type size_type;

    explicit MemTrackerAllocator(std::shared_ptr<MemTracker> mem_tracker, std::shared_ptr<Alloc> allocator)
        : _tracker(std::move(mem_tracker)), _allocator(allocator) {}

    explicit MemTrackerAllocator(std::shared_ptr<MemTracker> mem_tracker):
        _tracker(std::move(mem_tracker)), _allocator(new Alloc()) {}

    T* allocate(size_t n) {
        // try consume
        _tracker->consume(n * sizeof(T));
        T* ptr = _allocator->allocate(n);
        if (ptr == nullptr) {
            _tracker->release(n * sizeof(T));
        }
        return ptr;
    }

    void deallocate(T* p, size_t n) {
        _allocator->deallocate(p, n);
        _tracker->release(n * sizeof(T));
    }


private:
    std::shared_ptr<MemTracker> _tracker;
    std::shared_ptr<Alloc> _allocator;
};

// use dedicated jemalloc arena
template<typename T>
class JemallocArenaAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;

    JemallocArenaAllocator() {
        init();
    }

    ~JemallocArenaAllocator() {
        std::ostringstream key;
        key << "arena." << _arena_index << ".destroy";
        unsigned tmp;
        size_t len = sizeof(tmp);
        if (auto ret = je_mallctl(key.str().c_str(), &tmp, &len, nullptr, 0); ret != 0) {
            std::cout << key.str() << " failed, ret: " << ret << std::endl;
        }
    }

    T* allocate(size_t n) {
        return static_cast<T*>(je_mallocx(sizeof(T) * n, _flags));
    }
    void deallocate(T* ptr, size_t n) {
        je_dallocx(ptr, _flags);
    }

private:
    bool init() {
        size_t len = sizeof(_arena_index);
        if (auto ret = je_mallctl("arenas.create", &_arena_index, &len, nullptr, 0); ret != 0) {
            std::cout << "create arena failed, ret: " << ret << std::endl;
            return false;
        }
        std::cout << "arena index: " << _arena_index << std::endl;
        _flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;
        return true;
    }

    unsigned _arena_index = 0;
    int _flags = 0;

};

}