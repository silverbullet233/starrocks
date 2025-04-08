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

#include "runtime/lookup_stream_mgr.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

Status LookUpDispatcher::add_request(const LookUpRequestCtx& ctx) {
    _queue.put(ctx);
    return Status::OK();
}

bool LookUpDispatcher::try_get(int32_t driver_sequence, LookUpRequestCtx* ctx) {
    return _queue.try_get(ctx);
}

bool LookUpDispatcher::has_data(int32_t driver_sequence) const {
    return !_queue.empty();
}

std::shared_ptr<LookUpDispatcher> LookUpDispatcherMgr::create_dispatcher(RuntimeState* state, const TUniqueId& query_id,
                                                                         PlanNodeId target_node_id) {
    
    std::shared_ptr<LookUpDispatcher> dispatcher = std::make_shared<LookUpDispatcher>(state, query_id, target_node_id);

    std::lock_guard<Mutex> l(_lock);
    auto [iter_1, _1] = _dispatchers.try_emplace(query_id, std::make_shared<DispatcherMap>());
    auto [iter, created] = iter_1->second->try_emplace(target_node_id, dispatcher);
    if (created) {
        LOG(INFO) << "create LookUpDispatcher for query_id=" << print_id(query_id) << ", target_node_id=" << target_node_id;
    }
    return iter->second;
}

StatusOr<LookUpDispatcherPtr> LookUpDispatcherMgr::get_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id) {
    if (_dispatchers.contains(query_id)) {
        auto& dispatcher_map = _dispatchers.at(query_id);
        if (dispatcher_map->contains(target_node_id)) {
            return dispatcher_map->at(target_node_id);
        }
    }
    LOG(INFO) << "can't find LookUpDisPatcher for query_id=" << print_id(query_id) << ", target_node_id=" << target_node_id;
    return Status::NotFound(fmt::format("can't find LookUpDispatcher for query {}, plan_node {}", print_id(query_id), target_node_id));
}

Status LookUpDispatcherMgr::lookup(const LookUpRequestCtx& ctx) {
    const auto& query_id = ctx.request->query_id();
    TUniqueId t_query_id;
    t_query_id.hi = query_id.hi();
    t_query_id.lo = query_id.lo();
    const auto lookup_node_id = ctx.request->lookup_node_id();
    // LOG(INFO) << "receive lookup from query_id=" << print_id(query_id) << ", lookup_node_id=" << lookup_node_id;
    ASSIGN_OR_RETURN(auto dispatcher, get_dispatcher(t_query_id, lookup_node_id));
    RETURN_IF_ERROR(dispatcher->add_request(ctx));
    return Status::OK();
}

}