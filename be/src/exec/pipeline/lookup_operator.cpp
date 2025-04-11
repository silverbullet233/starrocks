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

namespace starrocks::pipeline {


class LookUpProcessor {
public:
    LookUpProcessor(LookUpOperator* parent): _parent(parent) {}
    ~LookUpProcessor() = default;

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
    // reset ctx

private:
    typedef phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> RowIdColumnMap;
    Status _deserialize_row_id_columns(const PLookUpRequest* request, RowIdColumnMap* row_id_columns);

    Status _build_chunk_meta(const ChunkPB& pchunk);
    Status _deserialize_chunk(const ChunkPB& pchunk, Chunk* chunk);
    LookUpRequestCtx _ctx;
    std::atomic_bool _is_running = false;

    Permutation _permutation;
    raw::RawString _serialize_buffer;

    [[maybe_unused]] LookUpOperator* _parent = nullptr;

    serde::ProtobufChunkMeta _chunk_meta; // all data should be same
    // bool _is_first = true;

    OlapReaderStatistics _stats;
};

Status LookUpProcessor::process(RuntimeState* state) {
    // @TODO use SegmentIterator to get data
    // @TODO get data from xx
    auto request = _ctx.request;
    if (_ctx.fetch_ctx == nullptr) {
        DCHECK(request != nullptr) << "request should not be null";
    }
    // LOG(INFO) << "process lookup request, row_id_column num: " << request->row_id_columns_size();
    auto response = _ctx.response;
    auto* cntl = static_cast<brpc::Controller*>(_ctx.cntl);

    phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> row_id_columns;
    // @TODO if local 
    if (_ctx.fetch_ctx != nullptr) {
        // this is local pass through request, we don't serialize data
        LOG(INFO) << "process lookup request, local pass through";
        for (const auto& [slot_id, columns]: *_ctx.fetch_ctx->request_columns) {
            auto row_id_column = RowIdColumn::static_pointer_cast(columns.first);
            row_id_columns[slot_id] = row_id_column;
        }
    } else {
        RETURN_IF_ERROR(_deserialize_row_id_columns(request, &row_id_columns));
    }
    // use row id columns to get data from storage
    // collect row id

    auto* glm_ctx = state->query_ctx()->global_late_materialization_ctx();

    // sort...
    // @TODO get SparseRange for each segment

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
        // sort by row_id

        // Permutation permutation;
        _permutation.resize(0);
        // permutation.resize(0);

        auto input_chunk = std::make_shared<Chunk>();
        input_chunk->append_column(row_id_column, slot_id);
        input_chunk->append_column(position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        input_chunk->check_or_die();
        // for (size_t i = 0;i < input_chunk->num_rows();i++) {
        //     LOG(INFO) << "input: " << input_chunk->debug_row(i);
        // }
        Columns order_by_columns{row_id_column};
        SortDescs sort_descs;
        sort_descs.descs = {SortDesc{true, true}, SortDesc{true, true}, SortDesc{true, true}};
        
        // @TODO since be_id is same, we only sort by seg_id + ord_id
        RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), row_id_column->columns(), sort_descs, &_permutation));

        auto sorted_chunk = input_chunk->clone_empty_with_slot(input_chunk->num_rows());

        materialize_by_permutation(sorted_chunk.get(), {input_chunk}, _permutation);
        // for (size_t i = 0;i < sorted_chunk->num_rows();i++) {
        //     LOG(INFO) << "sorted: " << sorted_chunk->debug_row(i);
        // }
        // LOG(INFO) << "tuple_id: " << tuple_id << ", slot_id: " << slot_id << ", sorted chunk: " << sorted_chunk->debug_columns();
        // data is sorted by row id
        // auto ordered_row_id_column = sorted_chunk->get_column_by_slot_id(slot_id);
        // const auto& ordered_row_id_data = down_cast<RowIdColumn*>(ordered_row_id_column.get())->get_data();
        auto ordered_row_id_column = down_cast<RowIdColumn*>(sorted_chunk->get_column_by_slot_id(slot_id).get());

        const auto& seg_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->seg_ids_column())->get_data();
        const auto& ord_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->ord_ids_column())->get_data();
        // const auto& ord_ids = down_cast<RowIdColumn*>(ordered_row_id_column.get())->ord_ids_column()->get_data();
        size_t num_rows = ordered_row_id_column->size();
        // build parse range

        uint32_t cur_seg_id = seg_ids[0];
        uint32_t cur_ord_id = ord_ids[0];

        Range<rowid_t> cur_range(cur_ord_id, cur_ord_id + 1);
        // determine the range of each segment
        // segment_id => ranges, make sure that data range is ordered
        std::map<int32_t, std::shared_ptr<SparseRange<rowid_t>>> seg_ranges;
        // @TODO use ordered map

        // @TODO need optimize
        Buffer<uint32_t> replicate_offsets;
        replicate_offsets.emplace_back(0);
        // uint32_t cur_offset = 1;
        replicate_offsets.emplace_back(1);

        // @TODO what if there are duplicated row ids
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
        // replicate_offsets.emplace_back(replicate_offsets.back() + 1);
        // for (const auto& [seg_id, range]: seg_ranges) {
        //     LOG(INFO) << "ranges for seg: " << seg_id << ", range: " << range->to_string();
        // }
        // LOG(INFO) << "last offset: " << replicate_offsets.back() << ", num_rows: " << num_rows;
        // for(const auto& offset: replicate_offsets) {
        //     LOG(INFO) << "replicate offsets: " << offset;
        // }
        // @TODO replicate if rowid has duplicated data

        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        const auto& slots = tuple_desc->slots();

        // sorted chunk only contain rowid and position, we should add fetched column into it.
        ChunkPtr result_chunk;

        for (const auto& [seg_id, range]: seg_ranges) {
            auto seg_info = glm_ctx->get_segment(seg_id);
            auto segment = seg_info.segment;
            auto tablet_schema = segment->tablet_schema_share_ptr();
            // LOG(INFO) << "segment schema: " << tablet_schema->debug_string();
            // build cid
            std::vector<ColumnId> cids;
            phmap::flat_hash_map<ColumnId, SlotId> cid_to_slot_id;
            for (const auto& slot : slots) {
                auto idx = tablet_schema->field_index(slot->col_name());
                DCHECK(idx != static_cast<size_t>(-1)) << "not find col: " << slot->col_name();
                // LOG(INFO) << "name: " << slot->col_name() << ", cid: " << idx << ", slot id: " << slot->id();
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
            // do while get eof
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
                    // should 
                    result_chunk = std::make_shared<Chunk>();
                    // @TODO how to pass slot id to indx
                    // @TODO should build slot id map
                    for (const auto& [cid, idx] : tmp->get_column_id_to_index_map()) {
                        auto tmp_column = tmp->get_column_by_index(idx)->clone();
                        SlotId sid = cid_to_slot_id[cid];
                        result_chunk->append_or_update_column(std::move(tmp_column), sid);
                        // LOG(INFO) << "append column with slot: " << sid;
                    }
                } else {
                    // should append data
                    // @TODO tmp chunk only has cid_to_index, we should chagne it back
                    for (const auto& [cid, idx]: tmp->get_column_id_to_index_map()) {
                        auto tmp_column = tmp->get_column_by_index(idx);
                        SlotId sid = cid_to_slot_id[cid];
                        result_chunk->get_column_by_slot_id(sid)->append(*tmp_column);
                    }
                }
            } while(true);
            // @TODO we should append chunk into final chunk
        }
        // LOG(INFO) << "result chunk: " << result_chunk->debug_columns();
        // @TODO
        // 1. re-sort columns by position column
        {
            // for (size_t i = 0;i < result_chunk->num_rows();i++) {
            //     LOG(INFO) << "result chunk: " << result_chunk->debug_row(i);
            // }
            for (const auto& [slot_id, _]: result_chunk->get_slot_id_to_index_map()) {
                auto old_column = result_chunk->get_column_by_slot_id(slot_id);
                // @TODO if nothing changed, we should avoid replivate
                ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicate_offsets));
                result_chunk->append_or_update_column(std::move(new_column), slot_id);
            }
            // LOG(INFO) << "result chunk after replicate: " << result_chunk->debug_columns();


            // @TODO duplicate result_sink, we should handle duplicated row ids
            // @TODO append pos column to result_chunk, and resort
            result_chunk->append_column(sorted_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID), Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
            // for (size_t i = 0;i < result_chunk->num_rows();i++) {
            //     LOG(INFO) << "result chunk: " << result_chunk->debug_row(i);
            // }
            result_chunk->check_or_die();
            // resort
            // LOG(INFO) << "result chunk: " << result_chunk->debug_columns();
            _permutation.resize(0);
            order_by_columns = {result_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID)};
            sort_descs.descs = {SortDesc{true, true}};
            RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));
            auto sorted_result_chunk = result_chunk->clone_empty_with_slot(result_chunk->num_rows());
            materialize_by_permutation(sorted_result_chunk.get(), {result_chunk}, _permutation);
            sorted_result_chunk->check_or_die();

            // @TODO append data into reposonse;
            if (_ctx.fetch_ctx != nullptr) {
                // this is local pass through request, we don't serialize data
                // LOG(INFO) << "local pass through, do nothing";
                // fill data 
                LOG(INFO) << "fill response for local request";
                for (const auto& slot: slots) {
                    auto column = sorted_result_chunk->get_column_by_slot_id(slot->id());
                    _ctx.fetch_ctx->response_columns[slot->id()] = column;
                    LOG(INFO) << "append column to response, slot_id: " << slot->id() << ", size: " << column->size();
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
    // @TODO re-sort result_chunk by position
    if (_ctx.fetch_ctx != nullptr) {
        // @TODO
        _ctx.fetch_ctx->callback(Status::OK());
    } else {
        _ctx.done->Run();
        // @TODO reduce in flight request num
    }
    // LOG(INFO) << "reset current ctx";
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


Status LookUpProcessor::_build_chunk_meta(const ChunkPB& pchunk) {
    return Status::OK();
}

Status LookUpProcessor::_deserialize_chunk(const ChunkPB& pchunk, Chunk* chunk) {
    // @TODO
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
    // for (const auto tuple_id : _tuple_ids) {
    //     LOG(INFO) << "tuple_id: " << tuple_id;
    // }
    // for (const auto& [tuple_id, row_id] : _row_id_slots) {
    //     LOG(INFO) << "tuple_id: " << tuple_id << ", row_id: " << row_id;
    // }
}

Status LookUpOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    LOG(INFO) << "LookUpOperator::prepare";
    _query_ctx = state->query_ctx()->get_shared_ptr();
    return Status::OK();
}
void LookUpOperator::close(RuntimeState* state) {
    Operator::close(state);
    LOG(INFO) << "LookUpOperator::close";
}

bool LookUpOperator::has_output() const {
    // LOG(INFO) << "LookUpOperator::has_output";
    if (_is_finished) {
        return false;
    }
    if (!_get_io_task_status().ok()) {
        return true;
    }
    // 1. try get request ctx from queue
    // 2. submit io task
    // 3. wait for response
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
    // LOG(INFO) << "no output";
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
    // @TODO

    // @TODO find all processor that can run
    LookUpRequestCtx request_ctx;
    for (int32_t i = 0;i < kIoTasksPerOperator;i++) {
        auto processor = _processors[i]; // @TODO use ref
        if (!processor->is_running() && _dispatcher->try_get(_driver_sequence, &request_ctx)) {
            processor->set_ctx(request_ctx);
            // @TODO if we can get a new request ctx, submit an io task to process
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
                    // @TODO consider eof
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

// LookUp workflow

// LookUpDispatcherMgr: manage all LookUpDispatcher for each query_id + plan_node_id
// LookUpDispatcher: add_request, try_get by driver_sequence
// LookUpOperator: schedule lookup io task, get request from LookUpDispatcher and invoke LookUpProcessor
// LookUpProcessor: child of LookUpOperator, process data
// SegmentIterator: get data by rowid

}