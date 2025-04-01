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

#include <butil/iobuf.h>
#include "exec/pipeline/operator.h"
#include "exec/tablet_info.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "exec/sorting/sort_permute.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class PLookUpRequest;
class PLookUpResponse;
}
namespace starrocks::pipeline {

class FetchOperator final : public Operator {
private:
    struct FetchContext {
        uint32_t be_id; // @TODO target address
        butil::IOBuf iobuf;
        ChunkPtr chunk;
        ChunkPtr result;
    };
public:
    FetchOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                int32_t target_node_id, const std::vector<TupleId>& tuple_ids, const std::unordered_map<TupleId, SlotId>& row_id_slots, std::shared_ptr<StarRocksNodesInfo> nodes_info);

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override;

    bool is_finished() const override;

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override;
    bool pending_finish() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override ;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override {
        return Status::OK();
    }

private:
    using PLookUpRequestPtr = std::shared_ptr<PLookUpRequest>;
    StatusOr<ChunkPtr> build_row_id_chunk(RuntimeState* state);
    // split original chunk to multiple chunks for each be
    Status gen_request_chunk(RuntimeState* state, const ChunkPtr& row_id_chunk, phmap::flat_hash_map<uint32_t, ChunkPtr>* request_chunks);

    Status serialize_chunk(const Chunk* src, ChunkPB* dst);
    Status build_output_chunk(RuntimeState* state);

    Status send_fetch_request(RuntimeState* state, const phmap::flat_hash_map<uint32_t, ChunkPtr>& request_chunks);

    Status fetch_data(RuntimeState* state);
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
    [[maybe_unused]] int32_t _target_node_id;
    [[maybe_unused]] const std::vector<TupleId>& _tuple_ids;
    [[maybe_unused]] const std::unordered_map<TupleId, SlotId>& _row_id_slots;
    [[maybe_unused]] std::shared_ptr<StarRocksNodesInfo> _nodes_info;

    phmap::flat_hash_map<SlotId, SlotDescriptor*> _slot_id_to_desc;

    std::atomic_bool _is_finishing = false;
    std::atomic_bool _is_finished = false;
    std::vector<ChunkPtr> _input_partial_chunks;
    size_t _next_output_idx = 0;
    // need a queue for all output chunks?
    // @TODO used to sort
    Permutation _permutation;

    mutable std::mutex _mu;
    mutable SpinLock _lock;
    Status _io_task_status;

    // std::atomic_int32_t _un_fetched_chunks = 0;
    std::atomic_int32_t _pending_fetched_chunks = 0;
    std::atomic_int32_t _pending_consumed_chunks = 0;

    std::atomic_int32_t _in_flight_request_num = 0;
    phmap::flat_hash_map<uint32_t, std::shared_ptr<FetchContext>> _fetch_ctxs;
    // std::vector<std::shared_ptr<FetchContext>> _fetch_ctxs;

    static const size_t kMaxBufferChunkNums = 8;
};

class FetchOperatorFactory final : public OperatorFactory {
public:
    FetchOperatorFactory(int32_t id, int32_t plan_node_id, int32_t target_node_id, 
            std::vector<TupleId> tuple_ids, std::unordered_map<TupleId, SlotId> row_id_slots, std::shared_ptr<StarRocksNodesInfo> nodes_info):
             OperatorFactory(id, "fetch", plan_node_id), _target_node_id(target_node_id), _tuple_ids(std::move(tuple_ids)), _row_id_slots(std::move(row_id_slots)), _nodes_info(std::move(nodes_info)) {}

    ~FetchOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<FetchOperator>(this, _id, _plan_node_id, driver_sequence, _target_node_id, _tuple_ids, _row_id_slots, _nodes_info);
    }
private:
    [[maybe_unused]] int32_t _target_node_id;
    std::vector<TupleId> _tuple_ids;
    std::unordered_map<TupleId, SlotId> _row_id_slots;
    std::shared_ptr<StarRocksNodesInfo> _nodes_info;
};
}