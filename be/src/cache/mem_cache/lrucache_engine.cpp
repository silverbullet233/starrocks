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

#include "cache/mem_cache/lrucache_engine.h"

#include <butil/fast_rand.h>

namespace starrocks {

template <class Alloc>
Status LRUCacheEngine<Alloc>::init(const MemCacheOptions& options) {
    _cache = std::make_unique<ShardedLRUCache>(options.mem_space_size);
    _initialized.store(true, std::memory_order_relaxed);
    return Status::OK();
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::insert(const std::string& key, void* value, size_t size, MemCacheDeleter deleter,
                                     MemCacheHandlePtr* handle, const MemCacheWriteOptions& options) {
    if (!_check_write(size, options)) {
        return Status::InternalError("cache insertion is rejected");
    }
    auto* lru_handle = _cache->insert(key, value, size, deleter, static_cast<CachePriority>(options.priority));
    if (handle) {
        *handle = reinterpret_cast<MemCacheHandlePtr>(lru_handle);
    }
    return Status::OK();
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::lookup(const std::string& key, MemCacheHandlePtr* handle, MemCacheReadOptions* options) {
    auto* lru_handle = _cache->lookup(CacheKey(key));
    if (!lru_handle) {
        return Status::NotFound("no such entry");
    }
    *handle = reinterpret_cast<MemCacheHandlePtr>(lru_handle);
    return Status::OK();
}

template <class Alloc>
bool LRUCacheEngine<Alloc>::exist(const std::string& key) const {
    auto* handle = _cache->lookup(CacheKey(key));
    if (!handle) {
        return false;
    } else {
        _cache->release(handle);
        return true;
    }
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::remove(const std::string& key) {
    _cache->erase(CacheKey(key));
    return Status::OK();
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::update_mem_quota(size_t quota_bytes) {
    _cache->set_capacity(quota_bytes);
    return Status::OK();
}

template <class Alloc>
const DataCacheMemMetrics LRUCacheEngine<Alloc>::cache_metrics() const {
    return DataCacheMemMetrics{.mem_quota_bytes = _cache->get_capacity(), .mem_used_bytes = _cache->get_memory_usage()};
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::shutdown() {
    (void)_cache->prune();
    return Status::OK();
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::prune() {
    _cache->prune();
    return Status::OK();
}

template <class Alloc>
void LRUCacheEngine<Alloc>::release(MemCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    _cache->release(lru_handle);
}

template <class Alloc>
const void* LRUCacheEngine<Alloc>::value(MemCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    return _cache->value(lru_handle);
}

template <class Alloc>
Status LRUCacheEngine<Alloc>::adjust_mem_quota(int64_t delta, size_t min_capacity) {
    if (_cache->adjust_capacity(delta, min_capacity)) {
        return Status::OK();
    }
    return Status::InternalError("adjust quota failed");
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::mem_quota() const {
    return _cache->get_capacity();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::mem_usage() const {
    return _cache->get_memory_usage();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::lookup_count() const {
    return _cache->get_lookup_count();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::hit_count() const {
    return _cache->get_hit_count();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::insert_count() const {
    return _cache->get_insert_count();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::insert_evict_count() const {
    return _cache->get_insert_evict_count();
}

template <class Alloc>
size_t LRUCacheEngine<Alloc>::release_evict_count() const {
    return _cache->get_release_evict_count();
}

template <class Alloc>
memory::Allocator* LRUCacheEngine<Alloc>::get_allocator() const {
    return _cache->get_allocator();
}

template <class Alloc>
bool LRUCacheEngine<Alloc>::_check_write(size_t charge, const MemCacheWriteOptions& options) const {
    if (options.evict_probability >= 100) {
        return true;
    }
    if (options.evict_probability <= 0) {
        return false;
    }

    /*
    // TODO: The cost of this call may be relatively high, and it needs to be optimized later.
    if (_cache->get_memory_usage() + charge <= _cache->get_capacity()) {
        return true;
    }
    */

    if (butil::fast_rand_less_than(100) < options.evict_probability) {
        return true;
    }
    return false;
}

// Explicit instantiation for the default allocator to provide symbols to callers.
template class LRUCacheEngine<DefaultCacheAllocator>;
} // namespace starrocks