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

#include "exec/spill/log_block_manager.h"

#include <memory>
#include <mutex>
#include <unordered_map>

#include "fs/fs.h"
#include "storage/options.h"
#include "common/config.h"
#include "io/input_stream.h"
#include "util/uid_util.h"

namespace starrocks {
namespace spill {

class LogBlockContainer {
public:
    LogBlockContainer(const std::string& path, uint64_t id, std::shared_ptr<FileSystem> fs):
        _path(path), _id(id), _fs(std::move(fs)) {}

    ~LogBlockContainer() = default;

    Status open();

    Status close();

    size_t size() const {
        DCHECK(_writable_file!= nullptr);
        return _writable_file->size();
    }
    std::string path() const {
        return _path;
    }
    uint64_t id() const {
        return _id;
    }

    Status ensure_preallocate(size_t length);
    // write data from offset
    Status write_data(size_t offset, const std::vector<Slice>& data);

    Status flush_data(size_t offset, size_t length);

    // Status read_data(size_t offset, size_t length, std::string* output);
    // @TODO new readable
    // @TODO read with readable

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable_file();

    Status read_data(io::InputStreamWrapper* readable, int64_t count, void* data);

    static StatusOr<LogBlockContainerPtr> create(const std::string& path, uint64_t id);

private:
    const std::string _path;
    const uint64_t _id;
    size_t _size = 0;
    std::unique_ptr<WritableFile> _writable_file;
    std::shared_ptr<FileSystem> _fs;
};


Status LogBlockContainer::open() {
    std::string file_path = _path + "/" + std::to_string(_id);
    WritableFileOptions opt;
    opt.mode = FileSystem::CREATE_OR_OPEN;
    ASSIGN_OR_RETURN(_writable_file, _fs->new_writable_file(opt, file_path));
    LOG(INFO) << "create new file: " << file_path;
    return Status::OK();
}

Status LogBlockContainer::close() {
    _writable_file.reset();
    return Status::OK();
}

Status LogBlockContainer::ensure_preallocate(size_t length) {
    return _writable_file->pre_allocate(length);
}

Status LogBlockContainer::write_data(size_t offset, const std::vector<Slice>& data) {
    return _writable_file->appendv(data.data(), data.size());
}

Status LogBlockContainer::flush_data(size_t offset, size_t length) {
    // @TODO update size
    return _writable_file->flush(WritableFile::FLUSH_ASYNC);
}

StatusOr<std::unique_ptr<io::InputStreamWrapper>> LogBlockContainer::get_readable_file() {
    std::string file_path = _path + "/" + std::to_string(_id);
    ASSIGN_OR_RETURN(auto f, _fs->new_sequential_file(file_path));
    return std::make_unique<io::InputStreamWrapper>(std::move(f));
}

Status LogBlockContainer::read_data(io::InputStreamWrapper* readable, int64_t count, void* data) {
    // do we need this method??
    // ASSIGN_OR_RETURN(auto read_len, readable->read())
    return Status::OK();
}

StatusOr<LogBlockContainerPtr> LogBlockContainer::create(const std::string& path, uint64_t id) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    auto container =  std::make_shared<LogBlockContainer>(path, id, fs);
    // RETURN_IF_ERROR(fs->create_dir_if_missing(path));
    // @TODO pending fix
    RETURN_IF_ERROR(fs->create_dir_recursive(path));
    return container;
}

class LogBlock: public Block {
public: 
    LogBlock(LogBlockContainerPtr container, size_t offset):
        _container(container), _offset(offset) {
        }

    LogBlock(LogBlockContainerPtr container, size_t offset, size_t length):
        _container(container), _offset(offset), _length(length) {}

    virtual ~LogBlock() {
        if (_readable != nullptr) {
            _readable.reset();
        }
    }

    const BlockId id() const override {
        return _id;
    }
    size_t offset() const {
        return _offset;
    }
    size_t length() const {
        return _length;
    }

    LogBlockContainerPtr container() const {
        return _container;
    }


    Status append(const std::vector<Slice>& data) override {
        size_t total_size = 0;
        std::for_each(data.begin(), data.end(), [&] (const Slice& slice) {
            total_size += slice.size;
        });
        RETURN_IF_ERROR(_container->ensure_preallocate(total_size));
        RETURN_IF_ERROR(_container->write_data(_offset, data));
        _length += total_size;
        return Status::OK();
    }

    Status flush() override {
        // LOG(INFO) << "flush block, offset: " << _offset << ", len: " << _length;
        return _container->flush_data(_offset, _length);
    }

    Status read_fully(void* data, int64_t count) override {
        if (_readable == nullptr) {
            ASSIGN_OR_RETURN(_readable, _container->get_readable_file());
            RETURN_IF_ERROR(_readable->skip(_offset));
        }
        ASSIGN_OR_RETURN(auto read_len, _readable->read(data, count));
        if (read_len == 0) {
            return Status::EndOfFile("end of block");
        }
        if (read_len != count) {
            return Status::InternalError("read block error");
        }
        return Status::OK();
    }

    std::string debug_string() override {
        std::ostringstream oss;
        oss << "LogBlock[container=" << _container->path() << "/" << _container->id()
            << ", offset:" << _offset << ", length=" << _length <<"]";
        return oss.str();
    }

private:
    LogBlockContainerPtr _container;
    BlockId _id; // really need id?
    size_t _offset = 0;
    size_t _length = 0;
    std::unique_ptr<io::InputStreamWrapper> _readable;
};

Status LogBlockManager::open() {
    std::vector<starrocks::StorePath> storage_paths;
    RETURN_IF_ERROR(parse_conf_store_paths(config::storage_root_path, &storage_paths));
    if (storage_paths.empty()) {
        return Status::InvalidArgument("cannot found spill storage path");
    }
    for (const auto& path : storage_paths) {
        _local_storage_paths.emplace_back(path.path + "/" + config::spill_local_storage_dir);
    }
    // init storage path
    auto fs = FileSystem::Default();
    for (const auto& path: _local_storage_paths) {
        // LOG(INFO) << "create spill storage path: " << path;
        RETURN_IF_ERROR(fs->create_dir_if_missing(path));
    }
    return Status::OK();
}

StatusOr<BlockPtr> LogBlockManager::acquire_block(const AcquireBlockOptions& opts) {
    // 1. pick up storage path
    // @TODO we need a global component to pick up path by some strategies, e.g. res disk size
    std::string storage_path = get_storage_path();
    // storage_path + opts.query_id;

    // 2. get or create container from storage path
    // container name: dir/query_id/name-plan_node_id-id
    // @TODO pendign fix
    ASSIGN_OR_RETURN(auto block_container, get_or_create_container(
        storage_path + "/" + print_id(opts.query_id) + "/" + opts.name + "-" + std::to_string(opts.plan_node_id)));
    // @TODO need block id?
    // @TODO fix size
    auto block = std::make_shared<LogBlock>(block_container, block_container->size());
    return block;
}

Status LogBlockManager::release_block(const BlockId& block_id) {
    return Status::OK();
}

Status LogBlockManager::release_block(const BlockPtr& block) {
    auto log_block = dynamic_cast<LogBlock*>(block.get());
    auto container = log_block->container();
    auto path = container->path();
    std::string key = path + "/" + std::to_string(container->id());
    LOG(INFO) << "release block, path: " << key;
    std::lock_guard<std::mutex> l(_mutex);
    // put related contianer back to the avaiable container
    // @TODO if container is full, should not put
    auto iter = _available_containers_by_path.find(path);
    DCHECK(iter != _available_containers_by_path.end());
    iter->second->push(container);
    LOG(INFO) << "return back container to path: " << path << ", container id: " << container->id();
    return Status::OK();
}

std::string LogBlockManager::get_storage_path() {
    // @TODO pending fix
    return _local_storage_paths[0];
}

StatusOr<LogBlockContainerPtr> LogBlockManager::get_or_create_container(
    const std::string& path) {
    // create dir first
    LOG(INFO) << "get_or_create_container at path: " << path;
    std::lock_guard<std::mutex> l(_mutex);
    auto iter = _available_containers_by_path.find(path);
    if (iter != _available_containers_by_path.end()) {
        auto& q = iter->second;
        if (!q->empty()) {
            auto container = q->front();
            LOG(INFO) << "return an exist container, path: " << container->path() << ", id: " << container->id();
            q->pop();
            return container;
        }
    } else {
        // _available_containers_by_path.insert({path, std::queue<LogBlockContainerPtr>()});
        _available_containers_by_path.insert({path, std::make_shared<ContainerQueue>()});
        iter = _available_containers_by_path.find(path);
    }
    // create container
    // generate container id
    uint64_t id = ++_next_container_id;
    ASSIGN_OR_RETURN(auto block_container, LogBlockContainer::create(path, id));
    RETURN_IF_ERROR(block_container->open());
    LOG(INFO) << "create new container, path: " << path << ", id: " << id;
    // iter->second->push(block_container);
    return block_container;
}

}
}