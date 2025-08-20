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

#include <ctime>
#include <utility>

#include "common/status.h"
#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gutil/strings/substitute.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

class Pipeline {
public:
    Pipeline() = delete;
    Pipeline(uint32_t id, OpFactories op_factories, ExecutionGroupRawPtr execution_group);

    uint32_t get_id() const { return _id; }

    Operators create_operators(int32_t degree_of_parallelism, int32_t i);
    void instantiate_drivers(RuntimeState* state);
    Drivers& drivers();
    const Drivers& drivers() const;
    void count_down_driver(RuntimeState* state);
    void clear_drivers();

    SourceOperatorFactory* source_operator_factory();
    const SourceOperatorFactory* source_operator_factory() const;
    OperatorFactory* sink_operator_factory() {
        DCHECK(!_op_factories.empty());
        return _op_factories[_op_factories.size() - 1].get();
    }
    size_t degree_of_parallelism() const;

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    void setup_pipeline_profile(RuntimeState* runtime_state);
    void setup_drivers_profile(const DriverPtr& driver);

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);
    void acquire_runtime_filter(RuntimeState* state);

    std::string to_readable_string() const;

    // STREAM MV
    Status reset_epoch(RuntimeState* state);
    void count_down_epoch_finished_driver(RuntimeState* state);

    size_t output_amplification_factor() const;
    Event* pipeline_event() const { return _pipeline_event.get(); }

private:
    uint32_t _id = 0;
    std::shared_ptr<RuntimeProfile> _runtime_profile = nullptr;
    OpFactories _op_factories;
    Drivers _drivers;
    std::atomic<size_t> _num_finished_drivers = 0;

    EventPtr _pipeline_event;
    ExecutionGroupRawPtr _execution_group = nullptr;
    // STREAM MV
    std::atomic<size_t> _num_epoch_finished_drivers = 0;
};

} // namespace pipeline
} // namespace starrocks
