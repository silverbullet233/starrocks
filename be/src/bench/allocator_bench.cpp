
#include "column/schema.h"
#include "common/memory/allocator.h"
#include "runtime/exec_env.h"

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <iostream>
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "common/status.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "datasketches/hll.hpp"

using namespace starrocks;
// 1. init mem tracker

std::shared_ptr<MemTracker> global_mem_tracker;

void test1() {
    global_mem_tracker = std::make_shared<MemTracker>(MemTracker::PROCESS, -1, "global");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(global_mem_tracker.get());
    std::shared_ptr<MemTracker> local_mem_tracker = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "local");

    std::cout << "before all, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    {
        std::vector<int> v1;
        std::cout << "v1 size: " << sizeof(v1) << std::endl;
        std::cout << "before init v1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
        for (size_t i = 0;i < 10;i ++) {
            v1.push_back(i);
            // std::cout << "tls: " << tls_mem_tracker->consumption() << std::endl;
            // std::cout << "local: " << local_mem_tracker->consumption() << std::endl;
        }
        std::cout << "after init v1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    }

    std::cout << "after scope1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;

    {
        MemTrackerAllocator<int, NoMemHookAllocator<int>> allocator(local_mem_tracker, std::make_shared<NoMemHookAllocator<int>>());
        std::vector<int, MemTrackerAllocator<int, NoMemHookAllocator<int>>> v2(allocator);
        std::cout << "v2 size: " << sizeof(v2) << std::endl;
        std::cout << "before init v2, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
        for (size_t i = 0;i < 10;i ++) {
            v2.push_back(i);
            // std::cout << "tls: " << tls_mem_tracker->consumption() << std::endl;
            // std::cout << "local: " << local_mem_tracker->consumption() << std::endl;
        }
        std::cout << "after init v2, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    }
    std::cout << "after all, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
}

void test2() {
    JemallocArenaAllocator<int> allocator;
    std::vector<int, JemallocArenaAllocator<int>> v(allocator);

    for (size_t i = 0; i < 10; i++) {
        v.push_back(i);
    }
    std::for_each(v.begin(), v.end(), [](int i) {
        std::cout << i << std::endl;
    });
}

// move between different allocator
void test3() {
    auto alloc1 = NoMemHookAllocator<int>("alloc1");
    std::vector<int, NoMemHookAllocator<int>> v1(alloc1);
    v1.push_back(1);
    v1.push_back(2);
    auto alloc2 = NoMemHookAllocator<int>("alloc2");
    std::vector<int, NoMemHookAllocator<int>> v2(alloc2);
    v2.push_back(3);
    std::cout << "v1 size: " << v1.get_allocator().debug_string() << ", v2 size: " << v2.get_allocator().debug_string() << std::endl;
    v2.swap(v1);
    std::cout << "v1 size: " << v1.get_allocator().debug_string() << ", v2 size: " << v2.get_allocator().debug_string() << std::endl;
}

template<class T>
using VectorWithAlloc = std::vector<T, NoMemHookAllocator<T>>;
// move vector vector
void test4() {
    std::cout << "test4" << std::endl;
    auto alloc1 = NoMemHookAllocator<int>("alloc1");
    auto alloc2 = NoMemHookAllocator<VectorWithAlloc<int>>("alloc2");
    VectorWithAlloc<VectorWithAlloc<int>> v1(alloc2);

    for (size_t i = 0;i < 3;i ++) {
        std::cout << "round " << i << "====" << std::endl;
        VectorWithAlloc<int> v(alloc1);
        v.push_back(i);
        std::cout << "v1 " << v.get_allocator().debug_string() << ", v " << v1.get_allocator().debug_string() << std::endl;
        v1.emplace_back(std::move(v));
        std::cout << "v1 " << v.get_allocator().debug_string() << ", v " << v1.get_allocator().debug_string() << std::endl;
    }
    for (size_t i = 0;i < 3;i++) {
        std::cout << "v[" << i << "] = " << v1[i].get_allocator().debug_string() << std::endl;
    }
    std::cout << "v " << v1.get_allocator().debug_string() << std::endl;
}

void test5() {
    std::cout << "test5 ===" << std::endl;
    using Alloc = NoMemHookAllocator<phmap::priv::Pair<const int, int>>;
    auto alloc = Alloc("test5");
    phmap::flat_hash_map<int, int, phmap::priv::hash_default_hash<int>, phmap::priv::hash_default_eq<int>, Alloc> m(alloc);

    for (size_t i = 0;i < 100000;i ++) {
        m.insert({i, i + 1});
        // std::cout << "alloc size: " << m.get_allocator().debug_string() << std::endl;
    }
    std::cout << "alloc size: " << std::endl << m.get_allocator().debug_string() << std::endl;
}

void test6() {
    std::cout << "test6 ===" << std::endl;
    // using Alloc = NoMemHookAllocator<phmap::priv::Pair<const int, int>>;
    using Alloc = NoMemHookAllocator<std::pair<const int, int>>;
    auto alloc = Alloc("test6");
    // phmap::flat_hash_map<int, int, phmap::priv::hash_default_hash<int>, phmap::priv::hash_default_eq<int>, Alloc> m(alloc);
    std::unordered_map<int, int, std::hash<int>, std::equal_to<int>, Alloc> m(alloc);

    for (size_t i = 0;i < 100000;i ++) {
        m.insert({i, i + 1});
        // std::cout << "alloc size: " << m.get_allocator().debug_string() << std::endl;
    }
    std::cout << "alloc size: " << std::endl << m.get_allocator().debug_string() << std::endl;
}

// @TODO, test transfer chunk, test schetch, test hll


void test7() {
    using Alloc = NoMemHookAllocator<uint8_t>;
    Alloc alloc("test7");
    datasketches::hll_sketch_alloc<Alloc> sketch(17, datasketches::HLL_6,false, alloc);
    for (size_t i = 0;i < 10000;i++){
        sketch.update(i);
    }
    std::cout << "alloc " << alloc.debug_string() << std::endl;
}


// how to transfer memory between different allocator

int main(int argc, char** argv) {
    // init global env
    // @TODO use CurrentThread
    test1();
    // test2();
    test3();
    test4();
    test5();
    test6();
    test7();
    return 0;
}