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

#include "exec/spill/block.h"
#include "exec/spill/block_manager.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

namespace starrocks {
namespace spill {

class LogBlockContainer;
using LogBlockContainerPtr = std::shared_ptr<LogBlockContainer>;
// dir => containers
class LogBlockManager: public BlockManager {
public:
    LogBlockManager() = default;
    ~LogBlockManager() = default;

    virtual Status open() override;
    // 1. pick dir
    // 2. pick container
    virtual StatusOr<BlockPtr> acquire_block(const AcquireBlockOptions& opts) override;

    // after flush, use this method to tell log block manager that related-containter can be release
    virtual Status release_block(const BlockId& block_id) override;

    virtual Status release_block(const BlockPtr& block) override;

private:
    std::string get_storage_path();
    StatusOr<LogBlockContainerPtr> get_or_create_container(const std::string& path);

private:
    typedef std::unordered_map<uint64_t, LogBlockContainerPtr> ContainerMap;
    typedef std::queue<LogBlockContainerPtr> ContainerQueue;
    typedef std::shared_ptr<ContainerQueue> ContainerQueuePtr;

    std::atomic<uint64_t> _next_container_id = 0;
    // FileSystem* _fs;
    std::vector<std::string> _local_storage_paths;
    std::mutex _mutex;
    // path => container queue
    std::unordered_map<std::string, ContainerQueuePtr> _available_containers_by_path;
    // all containers
    ContainerMap _containers;
};
}
}