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

package com.starrocks.planner;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFetchNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import software.amazon.awssdk.services.lexruntimev2.model.Slot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FetchNode extends PlanNode {

    PlanNodeId targetNodeId;
    // @TODO which to fetch
    List<TupleDescriptor> descs;
    // row id slot for each table
    Map<TupleId, SlotId> rowidSlots;
    // rowid column slots
    // @TODO should know target LookUpNode
    public FetchNode(PlanNodeId id, PlanNode inputNode,
                     PlanNodeId targetNodeId, List<TupleDescriptor> descs, Map<TupleId, SlotId> rowidSlots) {
        super(id, inputNode, "FETCH");
        this.targetNodeId = targetNodeId;
        this.descs = descs;
        this.rowidSlots = rowidSlots;
    }

    @Override
    public final void computeTupleIds() {
        // @TODO
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.FETCH_NODE;
        msg.fetch_node = new TFetchNode();
        msg.fetch_node.tuples = descs.stream().map(desc -> desc.getId().asInt()).collect(Collectors.toList());
    }

    @Override
    protected String debugString() {
        // @TODO
        return "FetchNode";
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(detailPrefix).append("FETCH ").append(targetNodeId);
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        return null;
    }

}
