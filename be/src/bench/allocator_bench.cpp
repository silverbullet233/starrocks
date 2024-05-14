
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

// TrackingAllocator and mem_hook can isolate
void test_isolation() {
    global_mem_tracker = std::make_shared<MemTracker>(MemTracker::PROCESS, -1, "global");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(global_mem_tracker.get());
    std::shared_ptr<MemTracker> local_mem_tracker = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "local");

    std::cout << "before all, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    {
        std::vector<int> v1;
        std::cout << "v1 size: " << sizeof(v1) << std::endl;
        std::cout << "before init v1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
        for (int i = 0;i < 10;i ++) {
            v1.push_back(i);
            // std::cout << "tls: " << tls_mem_tracker->consumption() << std::endl;
            // std::cout << "local: " << local_mem_tracker->consumption() << std::endl;
        }
        std::cout << "after init v1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    }

    std::cout << "after scope1, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;

    {
        using StlAlloc = TrackingStlAllocator<int>;
        auto alloc = std::make_shared<TrackingAllocator>("label", local_mem_tracker);
        auto allocator = StlAlloc(alloc);
        std::vector<int, StlAlloc> v2(allocator);
        std::cout << "v2 size: " << sizeof(v2) << std::endl;
        std::cout << "before init v2, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
        for (int i = 0;i < 10;i ++) {
            v2.push_back(i);
            // std::cout << "allocator: " << allocator.debug_string() << std::endl;
            // std::cout << "tls: " << tls_mem_tracker->consumption() << std::endl;
            // std::cout << "local: " << local_mem_tracker->consumption() << std::endl;
        }
        std::cout << "after init v2, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
    }
    std::cout << "after all, local: " << local_mem_tracker->consumption() << ", tls: " << tls_mem_tracker->consumption() << std::endl;
}

// void test2() {
//     JemallocArenaAllocator<int> allocator;
//     std::vector<int, JemallocArenaAllocator<int>> v(allocator);

//     for (size_t i = 0; i < 10; i++) {
//         v.push_back(i);
//     }
//     std::for_each(v.begin(), v.end(), [](int i) {
//         std::cout << i << std::endl;
//     });
// }

// // move between different allocator
void test3() {

    std::shared_ptr<MemTracker> mem_tracker_1 = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "mem1");
    std::shared_ptr<MemTracker> mem_tracker_2 = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "mem2");
    using StlAlloc = TrackingStlAllocator<int>;
    auto alloc = std::make_shared<TrackingAllocator>("label1", mem_tracker_1);
    auto alloc1 = StlAlloc(alloc);

    auto alloc2 = StlAlloc(std::make_shared<TrackingAllocator>("label2", mem_tracker_2));
    {
        std::vector<int, StlAlloc> v1(alloc1);
        {
            v1.push_back(1);
            v1.push_back(2);

            std::vector<int, StlAlloc> v2(alloc2);
            v2.push_back(3);

            std::cout << "before swap, v1 size: " << alloc1.debug_string() << std::endl << ", v2 size: " << alloc2.debug_string() << std::endl;
            v2.swap(v1);
            std::cout << "after swap, v1 size: " << alloc1.debug_string() << std::endl<< ", v2 size: " << alloc2.debug_string() << std::endl;
        }
        std::cout << "after release v2, v1 size: " << alloc1.debug_string() << std::endl<<", v2 size: " << alloc2.debug_string() << std::endl;
    }

    std::cout << "after release v1, v1 size: " << alloc1.debug_string() << std::endl<<", v2 size: " << alloc2.debug_string() << std::endl;
}

// template<class T>
// using VectorWithAlloc = std::vector<T, NoMemHookAllocator<T>>;
// // move vector vector
// void test4() {
//     std::cout << "test4" << std::endl;
//     auto alloc1 = NoMemHookAllocator<int>("alloc1");
//     auto alloc2 = NoMemHookAllocator<VectorWithAlloc<int>>("alloc2");
//     VectorWithAlloc<VectorWithAlloc<int>> v1(alloc2);

//     for (size_t i = 0;i < 3;i ++) {
//         std::cout << "round " << i << "====" << std::endl;
//         VectorWithAlloc<int> v(alloc1);
//         v.push_back(i);
//         std::cout << "v1 " << v.get_allocator().debug_string() << ", v " << v1.get_allocator().debug_string() << std::endl;
//         v1.emplace_back(std::move(v));
//         std::cout << "v1 " << v.get_allocator().debug_string() << ", v " << v1.get_allocator().debug_string() << std::endl;
//     }
//     for (size_t i = 0;i < 3;i++) {
//         std::cout << "v[" << i << "] = " << v1[i].get_allocator().debug_string() << std::endl;
//     }
//     std::cout << "v " << v1.get_allocator().debug_string() << std::endl;
// }

// void test5() {
//     std::cout << "test5 ===" << std::endl;
//     using Alloc = NoMemHookAllocator<phmap::priv::Pair<const int, int>>;
//     auto alloc = Alloc("test5");
//     phmap::flat_hash_map<int, int, phmap::priv::hash_default_hash<int>, phmap::priv::hash_default_eq<int>, Alloc> m(alloc);

//     for (size_t i = 0;i < 100000;i ++) {
//         m.insert({i, i + 1});
//         // std::cout << "alloc size: " << m.get_allocator().debug_string() << std::endl;
//     }
//     std::cout << "alloc size: " << std::endl << m.get_allocator().debug_string() << std::endl;
// }

// void test6() {
//     std::cout << "test6 ===" << std::endl;
//     // using Alloc = NoMemHookAllocator<phmap::priv::Pair<const int, int>>;
//     using Alloc = NoMemHookAllocator<std::pair<const int, int>>;
//     auto alloc = Alloc("test6");
//     // phmap::flat_hash_map<int, int, phmap::priv::hash_default_hash<int>, phmap::priv::hash_default_eq<int>, Alloc> m(alloc);
//     std::unordered_map<int, int, std::hash<int>, std::equal_to<int>, Alloc> m(alloc);

//     for (size_t i = 0;i < 100000;i ++) {
//         m.insert({i, i + 1});
//         // std::cout << "alloc size: " << m.get_allocator().debug_string() << std::endl;
//     }
//     std::cout << "alloc size: " << std::endl << m.get_allocator().debug_string() << std::endl;
// }

// // @TODO, test transfer chunk, test schetch, test hll

// void test7() {
//     using Alloc = NoMemHookAllocator<uint8_t>;
//     Alloc alloc("test7");
//     datasketches::hll_sketch_alloc<Alloc> sketch(17, datasketches::HLL_6,false, alloc);
//     for (size_t i = 0;i < 10000;i++){
//         sketch.update(i);
//     }
//     std::cout << "alloc " << alloc.debug_string() << std::endl;
// }

// void test8() {
//     std::cout << "test 8===" << std::endl;
//     // allocate shared
//     using Alloc = NoMemHookAllocator<int>;
//     using Alloc = TrackingAllocator<int>;
//     Alloc alloc("test8");
//     auto ptr = std::allocate_shared<int, Alloc>(alloc, 10);

//     std::cout << "alloc " << alloc.debug_string() << ", sizeof(Alloc):" << sizeof(Alloc) << std::endl;
// }

// void test9() {
//     // transfer allocator, NOT WORK
//     using Alloc = TrackingAllocator<int>;
//     std::shared_ptr<MemTracker> mem_tracker_1 = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "tracker1");
//     Alloc alloc1("alloc1", mem_tracker_1);
//     // 先在mem_tracker_1上分配内存
//     std::vector<int, Alloc> v(alloc1);
//     for (size_t i = 0;i < 1000;i++){
//         v.push_back(i);
//     }

//     std::shared_ptr<MemTracker> mem_tracker_2 = std::make_shared<MemTracker>(MemTracker::NO_SET, -1, "tracker2");
//     Alloc alloc2("alloc2", mem_tracker_2);
//     {
//         mem_tracker_1->release(alloc1.allocate_bytes);
//         mem_tracker_2->consume(alloc1.allocate_bytes);
//         // 内存转移
//         // 运行中换allocator，释放的时候在mem_tracker_2上
//         v.get_allocator().set_tracker(mem_tracker_2); // 因为get_allocator返回的是值而不是引用，实际上这段代码不work
//         // transfer usage, deallocate
//         v.shrink_to_fit(); // 内存在mem_tracker_2上释放
//     }
// }

// // transfer allocator demo
// // define a new type
// void test10() {

// }

int main(int argc, char** argv) {
    // init global env
    // @TODO use CurrentThread
    // test_isolation();
    // test2();
    test3();
    // test4();
    // test5();
    // test6();
    // test7();
    // test8();
    // test9();
    return 0;
}