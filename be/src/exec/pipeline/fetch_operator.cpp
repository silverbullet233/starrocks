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

#include "exec/pipeline/fetch_operator.h"
#include <butil/iobuf.h>
#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <sstream>
#include <unordered_map>
#include "agent/master_info.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/global_types.h"
#include "exec/pipeline/operator.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exec/tablet_info.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/lookup_stream_mgr.h"
#include "serde/column_array_serde.h"
#include "serde/encode_context.h"
#include "serde/protobuf_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/phmap/phmap.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
FetchOperator::FetchOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                int32_t target_node_id, const std::vector<TupleId>& tuple_ids,
                const std::unordered_map<TupleId, SlotId>& row_id_slots, std::shared_ptr<StarRocksNodesInfo> nodes_info,
                std::shared_ptr<LookUpDispatcher> local_dispatcher):
        Operator(factory, id, "Fetch", plan_node_id, true, driver_sequence),
        _target_node_id(target_node_id), _tuple_ids(tuple_ids), _row_id_slots(row_id_slots),
        _nodes_info(std::move(nodes_info)), _local_dispatcher(std::move(local_dispatcher)) {
                _input_partial_chunks.reserve(kMaxBufferChunkNums);
        }

Status FetchOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    if (auto opt = get_backend_id(); opt.has_value()) {
        _local_be_id = opt.value();
    } else {
        return Status::InternalError("can't get local backend id");
    }

    for (const auto& tuple_id: _tuple_ids) {
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        for (const auto& slot: tuple_desc->slots()) {
            _slot_id_to_desc.insert({slot->id(), slot});
        }
    }
    _build_row_id_chunk_timer = ADD_TIMER(_unique_metrics, "BuildRowIdChunkTime");
    _gen_request_chunk_timer = ADD_TIMER(_unique_metrics, "GenRequestChunkTime");
    _serialize_timer = ADD_TIMER(_unique_metrics, "SerializeTime");
    _deserialize_timer = ADD_TIMER(_unique_metrics, "DeserializeTime");
    _build_output_chunk_timer = ADD_TIMER(_unique_metrics, "BuildOutputChunkTime");
    _rpc_count = ADD_COUNTER(_unique_metrics, "RpcCount", TUnit::UNIT);
    _network_timer = ADD_TIMER(_unique_metrics, "NetworkTime");
    _local_request_count = ADD_COUNTER(_unique_metrics, "LocalRequestCount", TUnit::UNIT);
    _local_request_timer = ADD_TIMER(_unique_metrics, "LocalRequestTime");
    return Status::OK();
}
void FetchOperator::close(RuntimeState* state) {
    Operator::close(state);
    LOG(INFO) << "FetchOperator::close, " << get_name();
}

bool FetchOperator::has_output() const {

    std::lock_guard<std::mutex> l(_mu);
    if (!_get_io_task_status().ok()) {
        return true;
    }
    if (!_is_finishing) {
        if (_input_partial_chunks.size() < kMaxBufferChunkNums) {
            // still batching input
            return false;
        }
        if (_pending_consumed_chunks > 0) {
            return true;
        }

        return false;
    }
    

    if (_pending_consumed_chunks > 0) {
        return true;
    }
    if (!_input_partial_chunks.empty() && _fetch_ctxs.empty()) {
        // LOG(INFO) << "has pending data but no fetch_request, should return true to trigger fetch";
        return true;
    }
    
    return false;
}
bool FetchOperator::need_input() const {
    std::lock_guard<std::mutex> l(_mu);
    if (!_is_finished && _input_partial_chunks.size() < kMaxBufferChunkNums) {
        return true;
    }
    return false;
}

bool FetchOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_mu);
    if (_is_finished) {
        return true;
    }
    if (_is_finishing) {
        // all data has been consumed
        if (_input_partial_chunks.empty()) {
            return true;
        }
    }
    return false;
}


Status FetchOperator::set_finishing(RuntimeState* state) {
    _is_finishing = true;

    return Status::OK();
}

bool FetchOperator::pending_finish() const {
    return !is_finished();
}

StatusOr<ChunkPtr> FetchOperator::pull_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_mu);
    RETURN_IF_ERROR(_get_io_task_status());
    DCHECK_EQ(_in_flight_request_num, 0) << "in flight request num should be 0";
    DCHECK(!_input_partial_chunks.empty()) << "input chunk should not be empty";
    // merge chunk

    if (_pending_consumed_chunks > 0) {
        if (!_fetch_ctxs.empty()) {
            // we should merge chunk
            RETURN_IF_ERROR(build_output_chunk(state));
        }
        DCHECK_LT(_next_output_idx, _input_partial_chunks.size()) << "_next_output_idx should smaller than chunk num";
        auto chunk = _input_partial_chunks[_next_output_idx];
        _next_output_idx++;
        _pending_consumed_chunks --;
        if (_next_output_idx == _input_partial_chunks.size()) {
            _next_output_idx = 0;
            _input_partial_chunks.clear();
        }
        return chunk;
    }
    DCHECK(_is_finishing) << "only call this when finishing";
    LOG(INFO) << "fetch_data when finishing, ";
    RETURN_IF_ERROR(fetch_data(state));
    return nullptr;
}

Status FetchOperator::build_output_chunk(RuntimeState* state) {
    // merge response from fetch_ctxs
    SCOPED_TIMER(_build_output_chunk_timer);
    for (const auto& [tuple_id, row_id_slot] : _row_id_slots) {
        // build a tmp chunk contains all columns under a tuple, and re-sort by position
        auto chunk = std::make_shared<Chunk>();
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        ColumnPtr position_column = UInt32Column::create();

        // create column for each slot
        for (const auto& slot: tuple_desc->slots()) {
            auto col = ColumnHelper::create_column(slot->type(), slot->is_nullable());
            chunk->append_column(std::move(col), slot->id());
        }

        for (const auto& [be_id, fetch_ctx]: _fetch_ctxs) {
            if (fetch_ctx->request_columns->contains(row_id_slot)) {
                // if current be_id contains result of current tuple, we should append position column
                auto [_, pos_col] = fetch_ctx->request_columns->at(row_id_slot);
                position_column->append(*pos_col);
                // append partial data into chunk
                for (const auto& slot: tuple_desc->slots()) {
                    DCHECK(fetch_ctx->response_columns.contains(slot->id())) << "response columns should contains slot: " << slot->debug_string();
                    auto partial_col = fetch_ctx->response_columns.at(slot->id());
                    chunk->get_column_by_slot_id(slot->id())->append(*partial_col);
                }
            }
        }
        chunk->check_or_die();

        // re-sort data by position
        ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, chunk, {position_column}));

        // assign columns to output chunk
        for (const auto& slot: tuple_desc->slots()) {
            size_t offset = 0;
            auto src_column = sorted_chunk->get_column_by_slot_id(slot->id());
            for(size_t i = 0;i < _input_partial_chunks.size();i++) {
                size_t num_rows = _input_partial_chunks[i]->num_rows();
                auto dst_column = src_column->clone_empty()->as_mutable_ptr();
                dst_column->append(*src_column, offset, num_rows);
                _input_partial_chunks[i]->append_column(std::move(dst_column), slot->id());
                _input_partial_chunks[i]->check_or_die();
                offset += num_rows;
            }
        }

    }
    _fetch_ctxs.clear();
    return Status::OK();
}

Status FetchOperator::_send_remote_request(RuntimeState* state, const FetchContextPtr& fetch_ctx) {
    const auto be_id = fetch_ctx->be_id;
    const auto& request_columns = fetch_ctx->request_columns;

    auto* closure = new DisposableClosure<PLookUpResponse, FetchContextPtr>(fetch_ctx);
    closure->addSuccessHandler([this, closure](const FetchContextPtr& ctx, const PLookUpResponse& result) noexcept {
        DeferOp defer([&]() {
            if (--_in_flight_request_num == 0) {
                _pending_consumed_chunks = _input_partial_chunks.size();
                // LOG(INFO) << "all request finished, pending consumed chunks: " << _pending_consumed_chunks;
            }
        });
        COUNTER_UPDATE(_rpc_count, 1);
        COUNTER_UPDATE(_network_timer, MonotonicNanos() - ctx->send_ts);
        if (result.status().status_code() != TStatusCode::OK) {
            auto msg = fmt::format("fetch request failed, status: {}", result.status().DebugString());
            LOG(WARNING) << msg;
            _set_io_task_status(Status::InternalError(msg));
            return;
        }

        if (closure->cntl.response_attachment().size() > 0) {
            SCOPED_TIMER(_deserialize_timer);
            butil::IOBuf& io_buf = closure->cntl.response_attachment();
            for (size_t i = 0;i < result.columns_size();i++) {
                const auto& pcolumn = result.columns(i);
                if (UNLIKELY(io_buf.size() < pcolumn.data_size())) {
                    auto msg = fmt::format("io_buf size {} is less than column data size {}", io_buf.size(), pcolumn.data_size());
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                // @TODO reuse buffer
                std::string buffer;
                buffer.resize(pcolumn.data_size());
                size_t size = io_buf.cutn(buffer.data(), pcolumn.data_size());
                if (UNLIKELY(size != pcolumn.data_size())) {
                    auto msg = fmt::format("iobuf read {} != expected {}", size, pcolumn.data_size());
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                int32_t slot_id = pcolumn.slot_id();
                SlotDescriptor* slot_desc = _slot_id_to_desc.at(slot_id);
                auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
                const uint8_t* buff = reinterpret_cast<const uint8_t*>(buffer.data());
                auto ret = serde::ColumnArraySerde::deserialize(buff, column.get());
                if (ret == nullptr) {
                    auto msg = fmt::format("deserialize column error, slot_id: {}", slot_id);
                    LOG(WARNING) << msg;
                    _set_io_task_status(Status::InternalError(msg));
                    return;
                }
                ctx->response_columns.insert({slot_id, std::move(column)});
            }
        }
    });
    closure->addFailedHandler([this](const FetchContextPtr& ctx, std::string_view rpc_error_msg) noexcept {
        --_in_flight_request_num;
        LOG(INFO) << "request failed: " << rpc_error_msg;
        _set_io_task_status(Status::InternalError(rpc_error_msg));
    });

    ++_in_flight_request_num;
    closure->cntl.Reset();
    closure->cntl.set_timeout_ms(state->query_options().query_timeout * 1000); // @TODO

    // send rpc
    PLookUpRequest request;
    PUniqueId p_query_id;
    p_query_id.set_hi(state->query_id().hi);
    p_query_id.set_lo(state->query_id().lo);
    *request.mutable_query_id() = std::move(p_query_id);
    request.set_lookup_node_id(_target_node_id);
    {
        SCOPED_TIMER(_serialize_timer);
        size_t max_serialize_size = 0;
        // slot_id => request
        for (const auto& [slot_id, columns] : *request_columns) {
            const auto& row_id_column = columns.first;
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*row_id_column);
        }
        _serialize_buffer.clear();
        _serialize_buffer.resize(max_serialize_size);
        uint8_t* buff = reinterpret_cast<uint8_t*>(_serialize_buffer.data());
        uint8_t* begin = buff;

        for (const auto& [slot_id, columns]: *request_columns) {
            auto p_row_id_column = request.add_row_id_columns();
            p_row_id_column->set_slot_id(slot_id);
            const auto& row_id_column = columns.first;
            uint8_t* start = buff;
            buff = serde::ColumnArraySerde::serialize(*row_id_column, buff);
            p_row_id_column->set_data_size(buff - start);
        }
        size_t actual_serialize_szie = buff - begin;
        
        closure->cntl.request_attachment().append(_serialize_buffer.data(), actual_serialize_szie);
    }

    const auto* node_info = _nodes_info->find_node(be_id);
    auto stub = state->exec_env()->brpc_stub_cache()->get_stub(node_info->host, node_info->brpc_port);
    stub->lookup(&closure->cntl, &request, &closure->result, closure);
    return Status::OK();
}

Status FetchOperator::_send_local_request(RuntimeState* state, const FetchContextPtr& fetch_ctx) {
    fetch_ctx->callback = [this, send_ts = fetch_ctx->send_ts] (const Status& status) {
        DeferOp defer([this] () {
            if (--_in_flight_request_num == 0) {
                _pending_consumed_chunks = _input_partial_chunks.size();
                // LOG(INFO) << "_pending_consumed_chunks: " << _pending_consumed_chunks;
            }
        });
        if (status.ok()) {
            COUNTER_UPDATE(_local_request_count, 1);
            COUNTER_UPDATE(_local_request_timer, MonotonicNanos() - send_ts);
        }
        
        if (!status.ok()) {
            auto msg = fmt::format("fetch request failed, status: {}", status.to_string());
            LOG(WARNING) << msg;
            _set_io_task_status(Status::InternalError(msg));
            return;
        }
    };

    ++_in_flight_request_num;
    LookUpRequestCtx lookup_ctx;
    lookup_ctx.fetch_ctx = fetch_ctx;
    RETURN_IF_ERROR(_local_dispatcher->add_request(lookup_ctx));
    return Status::OK();
}

Status FetchOperator::send_fetch_request(RuntimeState* state, const phmap::flat_hash_map<uint32_t, RequestColumnsPtr>& request_chunks) {
    _fetch_ctxs.clear();
    for (const auto& [be_id, request_columns]: request_chunks) {
        auto fetch_ctx = std::make_shared<FetchContext>();
        fetch_ctx->be_id = be_id;
        fetch_ctx->request_columns = request_columns;
        fetch_ctx->send_ts = MonotonicNanos();
        _fetch_ctxs[be_id] = fetch_ctx;

        if (be_id == _local_be_id && config::enable_fetch_local_pass_through) {
            RETURN_IF_ERROR(_send_local_request(state, fetch_ctx));
        } else {
            RETURN_IF_ERROR(_send_remote_request(state, fetch_ctx));
        }
    }
    return Status::OK();
}

Status FetchOperator::fetch_data(RuntimeState* state) {
    DCHECK(!_input_partial_chunks.empty()) << "input chunk should not be empty";
    DCHECK_EQ(_pending_consumed_chunks, 0) << "pending consumed chunks should be 0 before fetch_data";
    ASSIGN_OR_RETURN(auto row_id_chunk, build_row_id_chunk(state));
    phmap::flat_hash_map<uint32_t, RequestColumnsPtr> request_chunks;
    RETURN_IF_ERROR(gen_request_chunk(state, row_id_chunk, &request_chunks));
    RETURN_IF_ERROR(send_fetch_request(state, request_chunks));
    _pending_fetched_chunks -= _input_partial_chunks.size();
    return Status::OK();
}

Status FetchOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // LOG(INFO) << "FetchOperator::push_chunk, " << chunk->debug_columns() << ", " << get_name();
    std::lock_guard<std::mutex> l(_mu);
    _input_partial_chunks.push_back(std::move(chunk));
    _pending_fetched_chunks++;
    if (_input_partial_chunks.size() < config::fetch_max_buffer_chunk_num) {
        return Status::OK();
    }
    RETURN_IF_ERROR(fetch_data(state));

    return Status::OK();
}


StatusOr<ChunkPtr> FetchOperator::build_row_id_chunk(RuntimeState* state) {
    SCOPED_TIMER(_build_row_id_chunk_timer);
    auto chunk = std::make_shared<Chunk>();
    const int virtual_idx_slot = INT32_MAX;

    // use virtual_idx to identify origin position of a row
    auto virtual_idx_column = UInt32Column::create();
    size_t total_rows = 0;
    for (const auto& chunk: _input_partial_chunks) {
        total_rows += chunk->num_rows();
    }
    virtual_idx_column->resize_uninitialized(total_rows);
    auto& virtual_idx_data = virtual_idx_column->get_data();
    // build virtual idx
    {
        size_t idx = 0;
        for (size_t i = 0;i < _input_partial_chunks.size();i++) {
            for (size_t j = 0;j < _input_partial_chunks[i]->num_rows();j++) {
                uint32_t virtual_idx = ((uint32_t)i << 16) | ((uint32_t)j);
                virtual_idx_data[idx++] = virtual_idx;
            }
        }
    }

    for (const auto& [_, slot_id]: _row_id_slots) {
        // @TODO avoid copy
        auto column = _input_partial_chunks[0]->get_column_by_slot_id(slot_id)->clone();

        for (size_t i = 1;i < _input_partial_chunks.size();i++) {
            auto src_column = _input_partial_chunks[i]->get_column_by_slot_id(slot_id);
            column->append(*src_column);
        }
        chunk->append_column(std::move(column), slot_id);
    }
    chunk->append_column(virtual_idx_column, virtual_idx_slot);

    chunk->check_or_die();
    return chunk;
}
StatusOr<ChunkPtr> FetchOperator::_sort_chunk(RuntimeState* state, const ChunkPtr& chunk, const Columns& order_by_columns) {
    // sort by row_id
    SortDescs sort_descs;
    sort_descs.descs.reserve(order_by_columns.size());
    for (size_t i = 0;i < order_by_columns.size();i++) {
        sort_descs.descs.emplace_back(SortDesc{true, true});
    }
    _permutation.resize(0);
    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));
    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {chunk}, _permutation);
    return sorted_chunk;
}

Status FetchOperator::gen_request_chunk(RuntimeState* state, const ChunkPtr& row_id_chunk, phmap::flat_hash_map<uint32_t, RequestColumnsPtr>* request_chunks) {
    SCOPED_TIMER(_gen_request_chunk_timer);
    // sort by row_id
    for (const auto& [tuple_id, slot_id]: _row_id_slots) {
        // we build a tmp chunk to sort data by row_id

        auto tmp_chunk = std::make_shared<Chunk>();
        auto row_id_column = RowIdColumn::static_pointer_cast(row_id_chunk->get_column_by_slot_id(slot_id));
        auto position_column = row_id_chunk->get_column_by_slot_id(INT32_MAX);
        // @TODO can we remove clone??
        tmp_chunk->append_column(row_id_column->clone(), slot_id);
        tmp_chunk->append_column(position_column->clone(), INT32_MAX);

        tmp_chunk->check_or_die();
        // we only need to sort by be_id
        ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, tmp_chunk, {row_id_column->be_ids_column()}));

        // now tmp chunk is order by row id
        row_id_column = RowIdColumn::static_pointer_cast(sorted_chunk->get_column_by_slot_id(slot_id));
        position_column = sorted_chunk->get_column_by_slot_id(INT32_MAX);

        RowIdColumn::Ptr row_id_column_ptr = down_cast<RowIdColumn*>(row_id_column.get());
        const auto& be_ids = UInt32Column::static_pointer_cast(row_id_column_ptr->be_ids_column())->get_data();
        auto iter = be_ids.begin();
        while (iter != be_ids.end()) {
            uint32_t cur_be_id = *iter;
            auto range = std::equal_range(iter, be_ids.end(), cur_be_id);
            size_t start = std::distance(be_ids.begin(), range.first);
            size_t end = std::distance(be_ids.begin(), range.second);
            size_t num_rows = end - start;
            // build request chunk for this BE
            auto new_row_id_column = RowIdColumn::create();
            new_row_id_column->reserve(num_rows);
            new_row_id_column->append(*row_id_column, start, num_rows);
            auto new_position_column = UInt32Column::create();
            new_position_column->reserve(num_rows);
            new_position_column->append(*position_column, start, num_rows);

            auto [request_columns_iter, _] = request_chunks->try_emplace(cur_be_id, std::make_shared<RequestColumns>());
            auto& request_columns = request_columns_iter->second;
            // LOG(INFO) << "add request column for be: " << cur_be_id << ", slot_id: " << slot_id << ", num: " << new_row_id_column->size();
            request_columns->emplace(slot_id, std::make_pair(std::move(new_row_id_column), std::move(new_position_column)));
            iter = range.second;
        }

    }

    return Status::OK();
}

}