
#include "common/memory/allocator.h"
#include "runtime/exec_env.h"

#include <algorithm>
#include <memory>
#include <vector>
#include <iostream>
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "common/status.h"

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

int main(int argc, char** argv) {
    // init global env
    // @TODO use CurrentThread
    test1();
    test2();
    return 0;
}