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

#include "exec/fetch_node.h"
#include <protocol/TDebugProtocol.h>
#include "common/global_types.h"
#include "exec/exec_node.h"
#include "types/logical_type.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/fetch_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/runtime_state.h"
#include "exec/tablet_info.h"

namespace starrocks {
FetchNode::FetchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    LOG(INFO) << "tnode: " << apache::thrift::ThriftDebugString(tnode);
    _target_node_id = tnode.fetch_node.target_node_id;
    LOG(INFO) << "init FetchNode, target_node: " << _target_node_id;
    for (const auto& tuple : tnode.fetch_node.tuples) {
        _tuple_ids.emplace_back(tuple);
        LOG(INFO) << "tuple_id: " << tuple;
    }
    for (const auto& [tuple_id, slot_id]: tnode.fetch_node.row_id_slots) {
        _row_id_slots.insert({tuple_id, slot_id});
        LOG(INFO) << "tuple_id: " << tuple_id << ", slot_id: " << slot_id;
    }
    _nodes_info = std::make_shared<StarRocksNodesInfo>(tnode.fetch_node.nodes_info);
}
Status FetchNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    LOG(INFO) << "FetchNode::init";
    _dispatcher = state->exec_env()->lookup_dispatcher_mgr()->create_dispatcher(state, state->query_id(), _target_node_id);
    return Status::OK();
}
Status FetchNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    LOG(INFO) << "FetchNOde::prepare";
    return Status::OK();
}

pipeline::OpFactories FetchNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
        OpFactories operators = _children[0]->decompose_to_pipeline(context);
        auto op_factory = std::make_shared<pipeline::FetchOperatorFactory>(
                context->next_operator_id(), id(), _target_node_id, _tuple_ids, _row_id_slots, _nodes_info, _dispatcher);

        operators.emplace_back(op_factory);
        return operators;
}

}