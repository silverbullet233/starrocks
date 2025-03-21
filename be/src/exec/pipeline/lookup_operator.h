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

#include "exec/pipeline/operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

class LookUpOperator final : public Operator {
public:
    LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                int32_t driver_id, int32_t driver_sequence_index, int32_t num_drivers,
                int32_t num_driver_sequences, const std::vector<int32_t>& driver_id_to_sequence_index);

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return false; }

    bool is_finished() const override { return false; }

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override {
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return nullptr;
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::OK();
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override {
        return Status::OK();
    }

};
}