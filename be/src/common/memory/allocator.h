#pragma once

#include <memory>
#include <type_traits>
#include "runtime/mem_tracker.h"
#include "jemalloc/jemalloc.h"
#include "fmt/format.h"

namespace starrocks {

struct AllocState {
    std::string to_string() {
        return fmt::format("[alloc count[{}], dealloc count[{}], size[{}]]", _alloc_count, _dealloc_count, _size);
    }
    size_t _alloc_count = 0;
    size_t _dealloc_count = 0;
    size_t _size = 0;
};

template<typename T>
class NoMemHookAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
	using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
	using propagate_on_container_swap = std::true_type; // to avoid the undefined behavior
    // using is_always_equal = std::true_type;
    // @TODO rebind
    template <typename U>
    struct rebind {
        using other = NoMemHookAllocator<U>;
    };

    NoMemHookAllocator() {
        std::cout << "default construct" << std::endl;
    }
    NoMemHookAllocator(std::string label): _label(label) {
        std::cout << "what ??" << std::endl;
    }
    NoMemHookAllocator(const NoMemHookAllocator& rhs): _label(rhs._label), _state(rhs._state) {
        // std::cout << "copy construct, " << rhs.debug_string() << std::endl; 
    }
    NoMemHookAllocator(NoMemHookAllocator& rhs): _label(rhs._label),_state(rhs._state) {
        // std::cout << "copy construct1, " << rhs.debug_string() << std::endl; 
    }

    ~NoMemHookAllocator() {
        // std::cout << "destruct " << std::to_string(reinterpret_cast<uintptr_t>(this)) << std::endl;
    }

    template<class U>
    NoMemHookAllocator(const NoMemHookAllocator<U>& other) {
        // std::cout << "convert construct " << other.debug_string() << ", this " << debug_string() << std::endl;
        _label = other.label();
        _state = other.state();
    }

    NoMemHookAllocator(NoMemHookAllocator&& rhs) noexcept {
        // std::cout << "move construct " <<  rhs.debug_string() << ", this " << debug_string() << std::endl;
        std::swap(_label, rhs._label);
        _state.swap(rhs._state);
    }

    NoMemHookAllocator& operator=(NoMemHookAllocator&& rhs) noexcept {
        if (this != &rhs) {
            std::cout << "move operator" << std::endl;
            _label = rhs._label;
            _state = rhs._state;
        }
        std::cout << "move operator end " << rhs.debug_string() << std::endl;
        return *this;
    }

    T* allocate(size_t n) {
        // size_t old = _state->_size;
        _state->_alloc_count ++;
        _state->_size += n * sizeof(T);
        // std::cout << "allocate " << n << ", old " << old << ", size: " << debug_string() << std::endl;
        return static_cast<T*>(je_malloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        // size_t old = _state->_size;
        je_free(ptr);
        _state->_size -= n*sizeof(T);
        _state->_dealloc_count++;
        // std::cout << "deallocate " << n << ", old " << old <<  ", size: " << debug_string() << std::endl;
    }


    std::string label() const {
        return _label;
    }
    std::shared_ptr<AllocState> state() const {
        return _state;
    }

    NoMemHookAllocator& operator=(const NoMemHookAllocator& rhs) {
        // std::cout << "assign operator " << rhs.debug_string() << std::endl;
        _label = rhs._label;
        _state = rhs._state;
        return *this;
    }

    template<class U>
    NoMemHookAllocator& operator=(const NoMemHookAllocator<U>& rhs) {
        // std::cout << "assign convert operator " << rhs.debug_string() << ", this " << debug_string() <<std::endl;
        _label = rhs._label;
        _state = rhs._state;
        return *this;
    }



    bool operator==(const NoMemHookAllocator& rhs) const {
        std::cout << "invoke ==" << std::endl;
        return this == &rhs;
        // return true;
    }

    bool operator!=(const NoMemHookAllocator& rhs) const {
        return !(*this == rhs);
    }

    std::string debug_string() const {
        return "NoMemHookAllocator(label=" + _label + ", "+ ", state=" + _state->to_string()+ ", addr=" + std::to_string(reinterpret_cast<uintptr_t>(this));
    }

    NoMemHookAllocator select_on_container_copy_construction() {
        std::cout << "select_on_container_copy_construction, " << debug_string() << std::endl;
        return *this;
    }

private:
    std::string _label;
    std::shared_ptr<AllocState> _state = std::make_shared<AllocState>();
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

// @TODO test pmr


}