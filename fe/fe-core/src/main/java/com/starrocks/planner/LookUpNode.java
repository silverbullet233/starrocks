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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;

public class LookUpNode extends PlanNode {
    // @TODO should add other info
    public LookUpNode(PlanNodeId id) {
        super(id, "LookUp");
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return null;
    }
}
