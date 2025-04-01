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

#include "exec/lookup_node.h"
#include <protocol/TDebugProtocol.h>
#include "exec/pipeline/lookup_operator.h"
#include "exec/exec_node.h"
#include "exec/pipeline/lookup_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"

namespace starrocks {
LookUpNode::LookUpNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
        LOG(INFO) << "init LookUpNode, " << apache::thrift::ThriftDebugString(tnode);
        // @TODO
        for (const auto& tuple: tnode.look_up_node.tuples) {
            _tuple_ids.emplace_back(tuple);
            LOG(INFO) << "tuple_id: " << tuple;
        }
        for (const auto& [tuple_id, slot_id]: tnode.look_up_node.row_id_slots) {
            _row_id_slots.insert({tuple_id, slot_id});
            LOG(INFO) << "tuple_id: " << tuple_id << ", slot_id: " << slot_id;
        }
}
Status LookUpNode::init(const TPlanNode& tnode, RuntimeState* state) {
        RETURN_IF_ERROR(ExecNode::init(tnode, state));
        LOG(INFO) << "LookUpNode::init";
        _dispatcher = state->exec_env()->lookup_dispatcher_mgr()->create_dispatcher(state, state->query_id(), id());
    
        return Status::OK();
}
Status LookUpNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
   return Status::OK();
}

pipeline::OpFactories LookUpNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
        LOG(INFO) << "LookUpNode::decompose_to_pipeline";
        auto lookup_op = std::make_shared<pipeline::LookUpOperatorFactory>(context->next_operator_id(), id(), 0, _tuple_ids, _row_id_slots, _dispatcher);
        // auto lookup_pipeline = pipeline::OpFactories{
        //         std::move(lookup_op), std::make_shared<pipeline::NoopSinkOperatorFactory>(context->next_operator_id(), id()),
        // };
        return OpFactories{std::move(lookup_op)};
        // auto lookup_pipeline = pipeline::OpFactories{
        //         std::move(lookup_op),
        // };
        // // @TODO dop
        // context->add_pipeline(lookup_pipeline);
        // return lookup_pipeline;
}
}