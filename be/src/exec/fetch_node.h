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

#include "exec/exec_node.h"

namespace starrocks {
class FetchNode final : public ExecNode {
public:
    FetchNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~FetchNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override {
        return Status::OK();
    }
    Status prepare(RuntimeState* state) override {
        return Status::OK();
    }
    // Blocks until the first batch is available for consumption via GetNext().
    Status open(RuntimeState* state) override {
        return Status::OK();
    }

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override {
        return Status::OK();
    }

    Status collect_query_statistics(QueryStatistics* statistics) override {
        return Status::OK();
    }

    void close(RuntimeState* state) override {}

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override {
                return {};
            }

};
}