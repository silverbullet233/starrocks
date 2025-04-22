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

#include <bthread/mutex.h>
#include <google/protobuf/service.h>
#include <mutex>
#include <shared_mutex>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
namespace pipeline {
class FetchContext;
}
struct LookUpRequestCtx {
    // cntl
    ::google::protobuf::RpcController* cntl = nullptr;
    const PLookUpRequest* request = nullptr;
    PLookUpResponse* response = nullptr;
    ::google::protobuf::Closure* done = nullptr;
    std::shared_ptr<pipeline::FetchContext> fetch_ctx = nullptr; // only used for local pass through
    void reset() {
        cntl = nullptr;
        request = nullptr;
        response = nullptr;
        done = nullptr;
        fetch_ctx.reset();
    }
};

class LookUpDispatcher {
public:
    LookUpDispatcher(RuntimeState* state, const TUniqueId& query_id, PlanNodeId lookup_node_id):
        _state(state), _query_id(query_id), _lookup_node_id(lookup_node_id) {}

    ~LookUpDispatcher() = default;

    Status add_request(const LookUpRequestCtx &ctx);

    bool try_get(int32_t driver_sequence, LookUpRequestCtx* ctx);
    bool has_data(int32_t driver_sequence) const;


private:
    [[maybe_unused]]RuntimeState* _state;
    const TUniqueId _query_id;
    [[maybe_unused]]PlanNodeId _lookup_node_id;
    std::mutex _lock;// @TODO remove it
    UnboundedBlockingQueue<LookUpRequestCtx> _queue;
};
using LookUpDispatcherPtr = std::shared_ptr<LookUpDispatcher>;

// used to manager lookup stream for all queries
class LookUpDispatcherMgr {
public:
    LookUpDispatcherMgr() = default;
    ~LookUpDispatcherMgr() = default;

    LookUpDispatcherPtr create_dispatcher(RuntimeState* state, const TUniqueId& query_id, PlanNodeId target_node_id);

    StatusOr<LookUpDispatcherPtr> get_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id);
    Status lookup(const LookUpRequestCtx& ctx);

    void close() {}
private:

    typedef std::shared_mutex Mutex;
    Mutex _lock;

    typedef phmap::flat_hash_map<PlanNodeId, std::shared_ptr<LookUpDispatcher>, StdHash<PlanNodeId>> DispatcherMap;
    typedef phmap::flat_hash_map<TUniqueId, std::shared_ptr<DispatcherMap>> QueryDispatcherMap;
    // query id => dispatcher
    QueryDispatcherMap _dispatchers;

};
}
