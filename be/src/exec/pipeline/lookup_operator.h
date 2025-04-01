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

#include <memory>
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "runtime/lookup_stream_mgr.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::pipeline {

// used to process lookup request
class LookUpProcessor;
using LookUpProcessorPtr = std::shared_ptr<LookUpProcessor>;

class LookUpOperator final : public SourceOperator {
public:
    LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                const std::vector<TupleId>& tuple_ids, const std::unordered_map<TupleId, SlotId>& row_id_slots,
            std::shared_ptr<LookUpDispatcher> dispatcher);

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override { return false; }

    bool is_finished() const override;

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::OK();
    }

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override {
        return Status::OK();
    }
private:
    friend class LookUpProcessor;

    struct IOTaskCtx {
        LookUpRequestCtx request_ctx;
        std::atomic_bool is_running = false;
    };
    void _set_io_task_status(const Status& status) {
        std::lock_guard<SpinLock> l(_lock);
        if (_io_task_status.ok()) {
            _io_task_status = status;
        }
    }
    Status _get_io_task_status() const {
        std::lock_guard<SpinLock> l(_lock);
        return _io_task_status;
    }

    Status _try_to_trigger_io_task(RuntimeState* state);

    [[maybe_unused]] const std::vector<TupleId>& _tuple_ids;
    [[maybe_unused]] const std::unordered_map<TupleId, SlotId>& _row_id_slots;
    std::shared_ptr<LookUpDispatcher> _dispatcher;

    std::atomic_int32_t _num_running_io_tasks = 0;

    mutable SpinLock _lock;
    Status _io_task_status;
    std::atomic_bool _is_finished = false;
    mutable std::vector<LookUpProcessorPtr> _processors;
    std::weak_ptr<QueryContext> _query_ctx;
    // mutable std::vector<IOTaskCtx> _io_task_ctxs;
    static const int32_t kIoTasksPerOperator = 4;

};

class LookUpOperatorFactory final : public SourceOperatorFactory {
public:
    LookUpOperatorFactory(int32_t id, int32_t plan_node_id, int32_t target_node_id, 
            std::vector<TupleId> tuple_ids, std::unordered_map<TupleId, SlotId> row_id_slots, std::shared_ptr<LookUpDispatcher> dispatcher);

    ~LookUpOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LookUpOperator>(this, _id, _plan_node_id, driver_sequence, _tuple_ids, _row_id_slots, _dispatcher);
    }
    std::shared_ptr<workgroup::ScanTaskGroup> io_task_group() const { return _io_task_group; }
    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }
private:
    [[maybe_unused]] int32_t _target_node_id;
    std::vector<TupleId> _tuple_ids;
    std::unordered_map<TupleId, SlotId> _row_id_slots;
    std::shared_ptr<LookUpDispatcher> _dispatcher;

    std::shared_ptr<workgroup::ScanTaskGroup> _io_task_group;
};
}