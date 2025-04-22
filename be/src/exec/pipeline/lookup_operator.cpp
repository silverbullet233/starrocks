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

#include "exec/pipeline/lookup_operator.h"
#include <memory>
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/workgroup/work_group.h"
#include "exec/workgroup/scan_executor.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/common.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "exec/pipeline/fetch_operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {


class LookUpProcessor {
public:
    LookUpProcessor(LookUpOperator* parent): _parent(parent) {}
    ~LookUpProcessor() = default;

    void close();

    bool is_running() const {
        return _is_running;
    }
    void set_running(bool running) {
        _is_running = running;
    }

    bool need_input() const {
        return false;
    }

    Status process(RuntimeState* state);

    void set_ctx(LookUpRequestCtx ctx) {
        DCHECK(_ctx.request == nullptr) << "_ctx is not empty";
        _ctx = ctx;
    }

private:
    typedef phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> RowIdColumnMap;
    Status _deserialize_row_id_columns(const PLookUpRequest* request, RowIdColumnMap* row_id_columns);

    LookUpRequestCtx _ctx;
    std::atomic_bool _is_running = false;

    Permutation _permutation;
    raw::RawString _serialize_buffer;

    [[maybe_unused]] LookUpOperator* _parent = nullptr;

    OlapReaderStatistics _stats;
};

void LookUpProcessor::close() {
    // update parent counter
    COUNTER_UPDATE(_parent->_bytes_read_counter, _stats.bytes_read);
    COUNTER_UPDATE(_parent->_io_timer, _stats.io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, _stats.compressed_bytes_read);
    COUNTER_UPDATE(_parent->_decompress_timer, _stats.decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, _stats.uncompressed_bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, _stats.block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, _stats.blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, _stats.block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, _stats.block_seek_ns);
    COUNTER_UPDATE(_parent->_block_seek_counter, _stats.block_seek_num);
    COUNTER_UPDATE(_parent->_raw_rows_counter, _stats.raw_rows_read);

    COUNTER_UPDATE(_parent->_read_pages_num_counter, _stats.total_pages_num);
    COUNTER_UPDATE(_parent->_cached_pages_num_counter, _stats.cached_pages_num);
    COUNTER_UPDATE(_parent->_total_columns_data_page_count, _stats.total_columns_data_page_count);

}

Status LookUpProcessor::process(RuntimeState* state) {
    auto request = _ctx.request;
    if (_ctx.fetch_ctx == nullptr) {
        DCHECK(request != nullptr) << "request should not be null";
    }
    auto response = _ctx.response;
    auto* cntl = static_cast<brpc::Controller*>(_ctx.cntl);

    phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> row_id_columns;
    if (_ctx.fetch_ctx != nullptr) {
        // this is local pass through request, we don't serialize data
        for (const auto& [slot_id, columns]: *_ctx.fetch_ctx->request_columns) {
            auto row_id_column = RowIdColumn::static_pointer_cast(columns.first);
            row_id_columns[slot_id] = row_id_column;
        }
    } else {
        RETURN_IF_ERROR(_deserialize_row_id_columns(request, &row_id_columns));
    }

    auto* glm_ctx = state->query_ctx()->global_late_materialization_ctx();

    for (const auto& [tuple_id, slot_id]: _parent->_row_id_slots) {
        if (!row_id_columns.contains(slot_id)) {
            continue;
        }
        const auto& row_id_column = row_id_columns[slot_id];
        // seg original index
        UInt32Column::Ptr position_column = UInt32Column::create();
        position_column->resize_uninitialized(row_id_column->size());
        auto& position_data = position_column->get_data();
        for (size_t i = 0;i < row_id_column->size();i++) {
            position_data[i] = i;
        }
        _permutation.resize(0);

        auto input_chunk = std::make_shared<Chunk>();
        input_chunk->append_column(row_id_column, slot_id);
        input_chunk->append_column(position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        input_chunk->check_or_die();

        Columns order_by_columns{row_id_column->seg_ids_column(), row_id_column->ord_ids_column()};;
        SortDescs sort_descs;
        sort_descs.descs = {SortDesc{true, true}, SortDesc{true, true}};
        
        // since be_id is same, we only sort by seg_id and ord_id
        RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));

        auto sorted_chunk = input_chunk->clone_empty_with_slot(input_chunk->num_rows());

        materialize_by_permutation(sorted_chunk.get(), {input_chunk}, _permutation);

        auto ordered_row_id_column = down_cast<RowIdColumn*>(sorted_chunk->get_column_by_slot_id(slot_id).get());

        const auto& seg_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->seg_ids_column())->get_data();
        const auto& ord_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->ord_ids_column())->get_data();
        size_t num_rows = ordered_row_id_column->size();
        // build parse range

        uint32_t cur_seg_id = seg_ids[0];
        uint32_t cur_ord_id = ord_ids[0];

        Range<rowid_t> cur_range(cur_ord_id, cur_ord_id + 1);
        // determine the range of each segment
        // segment_id => ranges, make sure that data range is ordered
        // @TODO use ordered map
        std::map<int32_t, std::shared_ptr<SparseRange<rowid_t>>> seg_ranges;

        // @TODO need optimize
        Buffer<uint32_t> replicate_offsets;
        replicate_offsets.emplace_back(0);
        replicate_offsets.emplace_back(1);

        for (size_t i = 1;i < num_rows;i++) {
            uint32_t seg_id = seg_ids[i];
            uint32_t ord_id = ord_ids[i];
            // LOG(INFO) << "seg id: " << seg_id << ", ord id: " << ord_id;
            if (seg_id == cur_seg_id) {
                // same segment, check if need add a new range
                if (ord_id == cur_range.end() - 1) {
                    // duplicated ord_ids, do nothing, we should mark which idx is duplicated
                    replicate_offsets.back()++;
                    continue;
                }
                if (ord_id == cur_range.end()) {
                    // continous range, just expand current range
                    cur_range.expand(1);
                } else {
                    // not continous, add the old one into seg_ranges
                    auto [iter, _] = seg_ranges.try_emplace(seg_id, std::make_shared<SparseRange<rowid_t>>());
                    iter->second->add(cur_range);
                    cur_range = Range<rowid_t>(ord_id, ord_id + 1);
                }
            } else {
                // move to next segment, we should add the old range into seg_ranges
                auto [iter, _] = seg_ranges.try_emplace(cur_seg_id, std::make_shared<SparseRange<rowid_t>>());
                iter->second->add(cur_range);
                // reset all
                cur_seg_id = seg_id;
                cur_range = Range<rowid_t>(ord_id, ord_id + 1);
            }
            replicate_offsets.emplace_back(replicate_offsets.back() + 1);
        }
        // handle the last one
        auto [iter, _] = seg_ranges.try_emplace(cur_seg_id, std::make_shared<SparseRange<rowid_t>>());
        iter->second->add(cur_range);

        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        const auto& slots = tuple_desc->slots();

        // sorted chunk only contain rowid and position, we should add fetched column into it.
        ChunkPtr result_chunk;

        for (const auto& [seg_id, range]: seg_ranges) {
            auto seg_info = glm_ctx->get_segment(seg_id);
            auto segment = seg_info.segment;
            auto tablet_schema = segment->tablet_schema_share_ptr();
            // build cid
            std::vector<ColumnId> cids;
            phmap::flat_hash_map<ColumnId, SlotId> cid_to_slot_id;
            for (const auto& slot : slots) {
                auto idx = tablet_schema->field_index(slot->col_name());
                DCHECK(idx != static_cast<size_t>(-1)) << "not find col: " << slot->col_name();
                cids.push_back(idx);
                cid_to_slot_id.emplace(idx, slot->id());
            }
            Schema schema = ChunkHelper::convert_schema(tablet_schema, cids);
            SegmentReadOptions seg_options;
            seg_options.fs = seg_info.fs;
            seg_options.tablet_schema = tablet_schema;
            seg_options.stats = &_stats;
            seg_options.rowid_range_option = range;
            auto segment_iterator = new_segment_iterator(segment, schema, seg_options);
            
            auto tmp = std::make_shared<Chunk>();
            do {
                tmp.reset(ChunkHelper::new_chunk_pooled(schema, state->chunk_size()));
                
                auto status = segment_iterator->get_next(tmp.get());
                if (status.is_end_of_file()) {
                    // LOG(INFO) << "reach end of file, " << tmp->debug_columns();
                    break;
                } else if (!status.ok()) {
                    LOG(INFO) << "get next error: " << status.to_string();
                    return status;
                }
                if (tmp->is_empty()) {
                    // LOG(INFO) << "reach end of segment";
                    break;
                }
                // LOG(INFO) << "tmp chunk: " << tmp->debug_columns() << ", seg: " << seg_id;
                if (result_chunk == nullptr) {
                    result_chunk = std::make_shared<Chunk>();
                    for (const auto& [cid, idx] : tmp->get_column_id_to_index_map()) {
                        auto tmp_column = tmp->get_column_by_index(idx)->clone();
                        SlotId sid = cid_to_slot_id[cid];
                        result_chunk->append_or_update_column(std::move(tmp_column), sid);
                    }
                } else {
                    // should append data
                    for (const auto& [cid, idx]: tmp->get_column_id_to_index_map()) {
                        auto tmp_column = tmp->get_column_by_index(idx);
                        SlotId sid = cid_to_slot_id[cid];
                        result_chunk->get_column_by_slot_id(sid)->append(*tmp_column);
                    }
                }
            } while(true);
        }
        // LOG(INFO) << "result chunk: " << result_chunk->debug_columns();
        // @TODO
        // 1. re-sort columns by position column
        {
            for (const auto& [slot_id, _]: result_chunk->get_slot_id_to_index_map()) {
                auto old_column = result_chunk->get_column_by_slot_id(slot_id);
                // @TODO if nothing changed, we should avoid replicate
                ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicate_offsets));
                result_chunk->append_or_update_column(std::move(new_column), slot_id);
            }

            result_chunk->append_column(sorted_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID), Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
            result_chunk->check_or_die();
            // resort
            _permutation.resize(0);
            order_by_columns = {result_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID)};
            sort_descs.descs = {SortDesc{true, true}};
            RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));
            auto sorted_result_chunk = result_chunk->clone_empty_with_slot(result_chunk->num_rows());
            materialize_by_permutation(sorted_result_chunk.get(), {result_chunk}, _permutation);
            sorted_result_chunk->check_or_die();

            if (_ctx.fetch_ctx != nullptr) {
                // this is local pass through request, we don't serialize data
                // LOG(INFO) << "fill response for local request";
                for (const auto& slot: slots) {
                    auto column = sorted_result_chunk->get_column_by_slot_id(slot->id());
                    _ctx.fetch_ctx->response_columns[slot->id()] = column;
                    // LOG(INFO) << "append column to response, slot_id: " << slot->id() << ", size: " << column->size();
                }
            } else {
                size_t max_serialize_size = 0;
                for (const auto& slot: slots) {
                    auto column = sorted_result_chunk->get_column_by_slot_id(slot->id());
                    max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*column);
                }
                _serialize_buffer.clear();
                _serialize_buffer.resize(max_serialize_size);
                uint8_t* buff = reinterpret_cast<uint8_t*>(_serialize_buffer.data());
                uint8_t* begin = buff;
                for (const auto& slot: slots) {
                    auto column = sorted_result_chunk->get_column_by_slot_id(slot->id());
                    auto pcolumn = response->add_columns();
                    pcolumn->set_slot_id(slot->id());
                    uint8_t* start = buff;
                    buff = serde::ColumnArraySerde::serialize(*column, buff);
                    pcolumn->set_data_size(buff - start);
                    // LOG(INFO) << "append column to response, slot_id: " << slot->id() << ", size: " << pcolumn->data_size();
                }
                // LOG(INFO) << "total serialize size: " << (buff - begin);
                size_t actual_serialize_size = buff - begin;
                cntl->response_attachment().append(_serialize_buffer.data(), actual_serialize_size);
            }

        }
        // 2. append data into response
    }
    if (_ctx.fetch_ctx != nullptr) {
        _ctx.fetch_ctx->callback(Status::OK());
    } else {
        _ctx.done->Run();
    }
    _ctx.reset();
    return Status::OK();
}

Status LookUpProcessor::_deserialize_row_id_columns(const PLookUpRequest* request, RowIdColumnMap* row_id_columns) {
    row_id_columns->clear();
    for(size_t i = 0;i < request->row_id_columns_size();i++) {
        const auto& pcolumn = request->row_id_columns(i);
        // deserialize
        int32_t slot_id = pcolumn.slot_id();
        [[maybe_unused]] int64_t data_size = pcolumn.data_size();
        // LOG(INFO) << "row id column data_size: " << data_size << ", slot: " << slot_id;
        RowIdColumn::Ptr row_id_column = RowIdColumn::create();
        const uint8_t* buff = reinterpret_cast<const uint8_t*>(pcolumn.data().data());
        auto ret = serde::ColumnArraySerde::deserialize(buff, row_id_column.get());
        if (ret == nullptr) {
            LOG(INFO) << "deserialize row id column error, slot_id: " << slot_id;
            return Status::InternalError("deserialize row id column error");
        }
        row_id_columns->emplace(slot_id, row_id_column);
        // LOG(INFO) << "deserialize row id column success, slot_id: " << slot_id << ", size: " << row_id_column->size();
    }
    return Status::OK();
}


LookUpOperator::LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
    const std::vector<TupleId>& tuple_ids, const std::unordered_map<TupleId, SlotId>& row_id_slots,
    std::shared_ptr<LookUpDispatcher> dispatcher):
    SourceOperator(factory, id, "LookUp", plan_node_id, true, driver_sequence),
    _tuple_ids(tuple_ids), _row_id_slots(row_id_slots), _dispatcher(std::move(dispatcher)) {
    LOG(INFO) << "create LookUpOperator, plan_node_id=" << plan_node_id << ", driver_sequence=" << driver_sequence;
    for (int32_t i = 0;i < kIoTasksPerOperator;i++) {
        _processors.emplace_back(std::make_shared<LookUpProcessor>(this));
    }
}

Status LookUpOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    LOG(INFO) << "LookUpOperator::prepare";
    _query_ctx = state->query_ctx()->get_shared_ptr();
    // init counter
    _init_counter(state);


    return Status::OK();
}

void LookUpOperator::_init_counter(RuntimeState* state) {
    _bytes_read_counter = ADD_COUNTER(_runtime_profile, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);


    _read_compressed_counter = ADD_COUNTER(_runtime_profile, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _read_pages_num_counter = ADD_COUNTER(_runtime_profile, "ReadPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_runtime_profile, "CachedPagesNum", TUnit::UNIT);

    _io_task_exec_timer = ADD_TIMER(_runtime_profile, "IOTaskExecTime");
    // SegmentInit

    // SegmentRead
    const std::string segment_read_name = "SegmentRead";
    _block_load_timer = ADD_CHILD_TIMER(_runtime_profile, segment_read_name, IO_TASK_EXEC_TIMER_NAME);
    _block_fetch_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockFetch", segment_read_name);
    _block_load_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockFetchCount", TUnit::UNIT, segment_read_name);
    _block_seek_timer = ADD_CHILD_TIMER(_runtime_profile, "BlockSeek", segment_read_name);
    _block_seek_counter = ADD_CHILD_COUNTER(_runtime_profile, "BlockSeekCount", TUnit::UNIT, segment_read_name);
    _decompress_timer = ADD_CHILD_TIMER(_runtime_profile, "DecompressT", segment_read_name);
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_runtime_profile, "TotalColumnsDataPageCount", TUnit::UNIT, segment_read_name);

    // IOTime
    _io_timer = ADD_CHILD_TIMER(_runtime_profile, "IOTime", IO_TASK_EXEC_TIMER_NAME);

}

void LookUpOperator::close(RuntimeState* state) {
    LOG(INFO) << "LookUpOperator::close";
    for (auto& processor: _processors) {
        processor->close();
    }
    Operator::close(state);

}

bool LookUpOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (!_get_io_task_status().ok()) {
        return true;
    }
    if (_num_running_io_tasks >= kIoTasksPerOperator) {
        return false;
    }
    // if we can trigger new io task
    for (auto& processor: _processors) {
        if (!processor->is_running() && _dispatcher->has_data(_driver_sequence)) {
            // LOG(INFO) << "can submit new io task";
            return true;
        }
    }
    return false;
}

bool LookUpOperator::is_finished() const {
    return _is_finished;
}

Status LookUpOperator::set_finishing(RuntimeState* state) {
    // LOG(INFO) << "LookUpOperator::set_finishing";
    _is_finished = true;
    return Status::OK();
}

StatusOr<ChunkPtr> LookUpOperator::pull_chunk(RuntimeState* state) {
    // LOG(INFO) << "LookUpOperator::pull_chunk";
    RETURN_IF_ERROR(_get_io_task_status());
    RETURN_IF_ERROR(_try_to_trigger_io_task(state));
    return nullptr;
}

Status LookUpOperator::_try_to_trigger_io_task(RuntimeState* state) {
    LookUpRequestCtx request_ctx;
    for (int32_t i = 0;i < kIoTasksPerOperator;i++) {
        auto processor = _processors[i]; 
        if (!processor->is_running() && _dispatcher->try_get(_driver_sequence, &request_ctx)) {
            processor->set_ctx(request_ctx);
            workgroup::ScanTask task;
            task.workgroup = state->fragment_ctx()->workgroup();
            task.priority = 10; // @TODO
            task.task_group = down_cast<const LookUpOperatorFactory*>(_factory)->io_task_group();
            task.work_function = [wp = _query_ctx, this, state, idx = i] (auto& ctx) {
                auto& processor = _processors[idx];
                DeferOp defer([&] {
                    _num_running_io_tasks--;
                    processor->set_running(false);
                });
                if (auto sp = wp.lock()) {
                    // LOG(INFO) << "run processor " << idx;
                    Status status = processor->process(state);
                    if (!status.ok()) {
                        LOG(INFO) << "process error: " << status.to_string();
                        _set_io_task_status(status);
                    }
                }
            };
            _num_running_io_tasks++;
            processor->set_running(true);
            task.workgroup->executors()->scan_executor()->submit(std::move(task));
        }
    }

    return Status::OK();
}

LookUpOperatorFactory::LookUpOperatorFactory(int32_t id, int32_t plan_node_id, int32_t target_node_id, 
    std::vector<TupleId> tuple_ids, std::unordered_map<TupleId, SlotId> row_id_slots, std::shared_ptr<LookUpDispatcher> dispatcher):
     SourceOperatorFactory(id, "lookup", plan_node_id), 
     _target_node_id(target_node_id), _tuple_ids(std::move(tuple_ids)), 
     _row_id_slots(std::move(row_id_slots)), _dispatcher(std::move(dispatcher)),
     _io_task_group(std::make_shared<workgroup::ScanTaskGroup>()) {}

}