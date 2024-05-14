#pragma once

#include <memory>
#include <memory_resource>
#include <type_traits>
#include "runtime/mem_tracker.h"
#include "jemalloc/jemalloc.h"
#include "fmt/format.h"

namespace starrocks {

struct AllocState {
    AllocState() = default;

    std::string to_string() const {
        return fmt::format("[alloc count[{}], dealloc count[{}], size[{}]]", _alloc_count, _dealloc_count, _size);
    }
    size_t _alloc_count = 0;
    size_t _dealloc_count = 0;
    size_t _size = 0;
};

class Allocator {
public:
    virtual ~Allocator() = default;
    virtual void* malloc(size_t bytes) {
        return je_malloc(bytes);
    }
    virtual void free(void* p, size_t bytes) {
        return je_free(p);
    }
    // ... other c api
};

class TrackingAllocator: public Allocator {
public:
    TrackingAllocator(std::string label, std::shared_ptr<MemTracker> tracker): _label(std::move(label)), _tracker(std::move(tracker)) {}
    ~TrackingAllocator() override = default;

    void* malloc(size_t bytes) override {
        void* ptr = Allocator::malloc(bytes);
        if (ptr) {
            _state._alloc_count += 1;
            _state._size += bytes;
            _tracker->consume(bytes);
        }
        std::cout << "consume " << bytes << " bytes, ptr=" << reinterpret_cast<uintptr_t>(ptr) << std::endl;
        return ptr;
    }
    void free(void* p, size_t bytes) override {
        _state._dealloc_count += 1;
        _state._size -= bytes;
        _tracker->release(bytes);
        std::cout << "release " << bytes << " bytes, ptr=" << reinterpret_cast<uintptr_t>(p) << std::endl;
        Allocator::free(p, bytes);

    }

    void set_tracker(std::shared_ptr<MemTracker> tracker) {
        _tracker = tracker;
    }

    std::string debug_string() const {
        return fmt::format("TrackingAllocator[label={}, state={}]", _label, _state.to_string());
    }

private:
    std::string _label;
    AllocState _state;
    std::shared_ptr<MemTracker> _tracker;
};


template<typename T>
class TrackingStlAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
	using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
	using propagate_on_container_swap = std::true_type; // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = TrackingStlAllocator<U>;
    };

    TrackingStlAllocator() = default;
    TrackingStlAllocator(std::shared_ptr<TrackingAllocator> allocator): _allocator(std::move(allocator)) {}
    TrackingStlAllocator(const TrackingStlAllocator& rhs): _allocator(rhs._allocator) {}
    template<class U>
    TrackingStlAllocator(const TrackingStlAllocator<U>& other): _allocator(other._allocator) {}

    ~TrackingStlAllocator() = default;

    TrackingStlAllocator(TrackingStlAllocator&& rhs) noexcept {
        _allocator.swap(rhs._allocator);
    }

    TrackingStlAllocator& operator=(TrackingStlAllocator&& rhs) noexcept {
        if (this != &rhs) {
            _allocator.swap(rhs._allocator);
        }
        return *this;
    }

    T* allocate(size_t n) {
        std::cout << "allocate " << n * sizeof(T) << std::endl;
        return static_cast<T*>(_allocator->malloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        std::cout << "deallocate " << n * sizeof(T) << std::endl;
        _allocator->free(ptr, n * sizeof(T));
    }

    TrackingStlAllocator& operator=(const TrackingStlAllocator& rhs) {
        _allocator = rhs._allocator;
        return *this;
    }

    template<class U>
    TrackingStlAllocator& operator=(const TrackingStlAllocator<U>& rhs) {
        _allocator = rhs._allocator;
        return *this;
    }


    bool operator==(const TrackingStlAllocator& rhs) const {
        return _allocator.get() == rhs._allocator.get();
    }

    bool operator!=(const TrackingStlAllocator& rhs) const {
        return !(*this == rhs);
    }

    std::string debug_string() const {
        return _allocator->debug_string();
    }

private:
    std::shared_ptr<TrackingAllocator> _allocator;
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

    size_t memory_usage() const {
        return _tracker->consumption();
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