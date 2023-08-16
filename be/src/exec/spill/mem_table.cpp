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

#include "exec/spill/mem_table.h"

#include <glog/logging.h>

#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/chunks_sorter.h"
#include "exec/spill/input_stream.h"
#include "runtime/current_thread.h"

namespace starrocks::spill {

bool UnorderedMemTable::is_empty() {
    return _chunks.empty();
}

Status UnorderedMemTable::append(ChunkPtr chunk) {
    _tracker->consume(chunk->memory_usage());
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, chunk->memory_usage());
    _chunks.emplace_back(std::move(chunk));
    return Status::OK();
}

Status UnorderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunks.empty() || _chunks.back()->num_rows() + size > _runtime_state->chunk_size()) {
        _chunks.emplace_back(src.clone_empty());
        _tracker->consume(_chunks.back()->memory_usage());
        COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, _chunks.back()->memory_usage());
    }

    Chunk* current = _chunks.back().get();
    size_t mem_usage = current->memory_usage();
    current->append_selective(src, indexes, from, size);
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);

    return Status::OK();
}

Status UnorderedMemTable::flush(FlushCallBack callback) {
    for (const auto& chunk : _chunks) {
        RETURN_IF_ERROR(callback(chunk));
    }
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
    _chunks.clear();
    return Status::OK();
}

StatusOr<std::shared_ptr<SpillInputStream>> UnorderedMemTable::as_input_stream(bool shared) {
    if (shared) {
        return SpillInputStream::as_stream(_chunks, _spiller);
    } else {
        return SpillInputStream::as_stream(std::move(_chunks), _spiller);
    }
}

bool OrderedMemTable::is_empty() {
    return _chunk == nullptr || _chunk->is_empty();
}

Status OrderedMemTable::append(ChunkPtr chunk) {
    if (_chunk == nullptr) {
        _chunk = chunk->clone_empty();
    }
    int64_t old_mem_usage = _chunk->memory_usage();
    _chunk->append(*chunk);
    int64_t new_mem_usage = _chunk->memory_usage();
    _tracker->set(_chunk->memory_usage());
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, new_mem_usage - old_mem_usage);
    return Status::OK();
}

Status OrderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunk == nullptr) {
        _chunk = src.clone_empty();
    }

    Chunk* current = _chunk.get();
    size_t mem_usage = current->memory_usage();
    _chunk->append_selective(src, indexes, from, size);
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);

    return Status::OK();
}

Status OrderedMemTable::flush(FlushCallBack callback) {
    while (!_chunk_slice.empty()) {
        auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
        RETURN_IF_ERROR(callback(chunk));
    }
    _chunk_slice.reset(nullptr);
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
    _chunk.reset();
    return Status::OK();
}

Status OrderedMemTable::done() {
    // do sort
    ASSIGN_OR_RETURN(_chunk, _do_sort(_chunk));
    _chunk_slice.reset(_chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> OrderedMemTable::_do_sort(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    DataSegment segment(_sort_exprs, chunk);
    _permutation.resize(0);

    auto& order_bys = segment.order_by_columns;
    {
        SCOPED_TIMER(_spiller->metrics().sort_chunk_timer);
        RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), order_bys, _sort_desc, &_permutation));
    }

    ChunkPtr sorted_chunk = _chunk->clone_empty_with_slot(_chunk->num_rows());
    {
        SCOPED_TIMER(_spiller->metrics().materialize_chunk_timer);
        materialize_by_permutation(sorted_chunk.get(), {_chunk}, _permutation);
    }

    return sorted_chunk;
}
template <typename BinaryColumnType>
void reserve_memory(Column* dst_col, const std::vector<ChunkPtr>& src_chunks, size_t col_idx) {
    auto* binary_dst_col = down_cast<BinaryColumnType*>(dst_col);
    size_t total_num_bytes = 0;
    for (const auto& src_chk : src_chunks) {
        const auto* src_data_col = ColumnHelper::get_data_column(src_chk->get_column_by_index(col_idx).get());
        const auto* src_binary_col = down_cast<const BinaryColumnType*>(src_data_col);
        total_num_bytes += src_binary_col->get_bytes().size();
    }
    binary_dst_col->get_bytes().reserve(total_num_bytes);
}

void concat_chunks(ChunkPtr& dst_chunk, const std::vector<ChunkPtr>& src_chunks, size_t num_rows) {
    DCHECK(!src_chunks.empty());
    // Columns like FixedLengthColumn have already reserved memory when invoke Chunk::clone_empty(num_rows).
    dst_chunk = src_chunks.front()->clone_empty(num_rows);
    const auto num_columns = dst_chunk->num_columns();
    for (auto i = 0; i < num_columns; ++i) {
        auto dst_col = dst_chunk->get_column_by_index(i);
        auto* dst_data_col = ColumnHelper::get_data_column(dst_col.get());
        // Reserve memory room for bytes array in BinaryColumn here.
        if (dst_data_col->is_binary()) {
            reserve_memory<BinaryColumn>(dst_data_col, src_chunks, i);
        } else if (dst_col->is_large_binary()) {
            reserve_memory<LargeBinaryColumn>(dst_data_col, src_chunks, i);
        }
    }
    for (const auto& src_chk : src_chunks) {
        dst_chunk->append(*src_chk);
    }
}


bool OrderedMemTableV2::is_empty() {
    return _staging_unsorted_rows ==0 && _sorted_chunks.empty();
}

Status OrderedMemTableV2::append(ChunkPtr chunk) {
    size_t mem_usage = chunk->memory_usage();
    _staging_unsorted_rows += chunk->num_rows();
    _staging_unsorted_bytes += chunk->bytes_usage();
    _staging_unsorted_chunks.push_back(std::move(chunk));

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);
    RETURN_IF_ERROR(_partial_sort(false));
    return Status::OK();
}

Status OrderedMemTableV2::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    // @TODO reuse
    ChunkPtr chunk = src.clone_empty();
    chunk->append_selective(src, indexes, from, size);

    size_t mem_usage = chunk->memory_usage();
    _staging_unsorted_rows += chunk->num_rows();
    _staging_unsorted_bytes += chunk->bytes_usage();
    _staging_unsorted_chunks.push_back(std::move(chunk));
    _tracker->consume(mem_usage);

    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);
    RETURN_IF_ERROR(_partial_sort(false));
    return Status::OK();
}

Status OrderedMemTableV2::flush(FlushCallBack callback) {
    // @TODO iterate all sorted chunk
    // LOG(INFO) << "flush mem table, chunk num: " << _merged_runs.num_chunks();
    for (size_t i = 0;i < _merged_runs.num_chunks();i++) {
        _chunk_slice.reset(_merged_runs.get_chunk(i));
        while (!_chunk_slice.empty()) {
            auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
            RETURN_IF_ERROR(callback(chunk));
        }
    }
    
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
    _merged_runs.clear();
    return Status::OK();
}

Status OrderedMemTableV2::done() {
    RETURN_IF_ERROR(_partial_sort(true));
    _permutation = {};
    _unsorted_chunk.reset();
    RETURN_IF_ERROR(_merge_sorted());
    return Status::OK();
}

Status OrderedMemTableV2::_partial_sort(bool done) {
    if (!_staging_unsorted_rows) {
        return Status::OK();
    }
    bool reach_limit = _staging_unsorted_rows >= max_buffered_rows || _staging_unsorted_bytes >= max_buffered_bytes;
    if (done || reach_limit) {
        // LOG(INFO) << "do partial_sort, rows: " << _staging_unsorted_rows << ", bytes: " << _staging_unsorted_bytes;
        // @TODO force late materialize
        concat_chunks(_unsorted_chunk, _staging_unsorted_chunks, _staging_unsorted_rows);
        _staging_unsorted_chunks.clear();
        RETURN_IF_ERROR(_unsorted_chunk->upgrade_if_overflow());

        DataSegment segment(_sort_exprs, _unsorted_chunk);
        _permutation.resize(0);
        {
            SCOPED_TIMER(_spiller->metrics().sort_chunk_timer);
            RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), segment.order_by_columns, _sort_desc, &_permutation));
        }
        auto sorted_chunk = _unsorted_chunk->clone_empty_with_slot(_unsorted_chunk->num_rows());
        // @TODO can we skip mt, until final merge?
        {
            SCOPED_TIMER(_spiller->metrics().materialize_chunk_timer);
            // @TODO we can skip permutation?
            // @TODO store permutation?
            // @TODO add a column in sorted_chunk
            // @TODO add a new Column in chunk to store permutation
            materialize_by_permutation(sorted_chunk.get(), {_unsorted_chunk}, _permutation);
            RETURN_IF_ERROR(sorted_chunk->upgrade_if_overflow());
        }

        _sorted_chunks.emplace_back(std::move(sorted_chunk));
        _unsorted_chunk->reset();
        _staging_unsorted_rows = 0;
        _staging_unsorted_bytes = 0;
    } 
    return Status::OK();
}

Status OrderedMemTableV2::_merge_sorted() {
    SCOPED_TIMER(_spiller->metrics().merge_chunk_timer);
    RETURN_IF_ERROR(merge_sorted_chunks(_sort_desc, _sort_exprs, _sorted_chunks, &_merged_runs));
    _sorted_chunks.clear();
    return Status::OK();
}

// v3
bool OrderedMemTableV3::is_empty() {
    return _staging_unsorted_rows == 0 && _sorted_early_chunks.empty();
}

Status OrderedMemTableV3::append(ChunkPtr chunk) {
    _split_and_append_chunks(chunk);
    RETURN_IF_ERROR(_partial_sort(false));
    return Status::OK();
}

Status OrderedMemTableV3::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    // @TODO reuse
    ChunkPtr chunk = src.clone_empty();
    chunk->append_selective(src, indexes, from, size);
    _split_and_append_chunks(chunk);
    RETURN_IF_ERROR(_partial_sort(false));
    return Status::OK();
}

static constexpr SlotId ORDINAL_COLUMN_SLOT_ID = -2;
using OrdinalColumn = FixedLengthColumn<uint32_t>;

ChunkPtr OrderedMemTableV3::_late_materialize(const ChunkPtr& sorted_early_chunk) {
    SCOPED_TIMER(_spiller->metrics().late_materialize_timer);
    // @TODO need a template?
    const size_t num_rows = sorted_early_chunk->num_rows();
    auto ordinal_column = sorted_early_chunk->get_column_by_slot_id(ORDINAL_COLUMN_SLOT_ID);
    auto& ordinal_data = down_cast<OrdinalColumn*>(ordinal_column.get())->get_data();

    const auto& early_materialized_slots = _spiller->options().early_materialized_slots;
    // LOG(INFO) << "late mt: " << num_rows;
    // auto sorted_late_chunk = _staging_late_chunks[0]->clone_empty(num_rows);
    auto sorted_late_chunk = _final_late_chunks[0]->clone_empty(num_rows);
    {
        SCOPED_TIMER(_spiller->metrics().build_late_chunk_timer);
        const uint32_t mask = (1L << 24) - 1;
        for (size_t i = 0;i < num_rows;i++) {
            uint32_t ordinal = ordinal_data[i];
            uint32_t chunk_idx = ordinal >> 24;
            uint32_t idx_in_chunk = ordinal & mask;
            // @TODO column by column?
            // @TODO cost a lot of time
            // sorted_late_chunk->append(*_staging_late_chunks[chunk_idx], idx_in_chunk, 1);
            // LOG(INFO) << "chunk idx: " << chunk_idx << ", idx_in_chunk: " << idx_in_chunk;
            sorted_late_chunk->append(*_final_late_chunks[chunk_idx], idx_in_chunk, 1);
        }
        

    }
    // @TODO seems we can push merge until restore? need lots of memory, maybe not good
    auto final_chunk = std::make_shared<Chunk>();
    // merge two chunk
    for (auto slot_id : _col_idx_to_slot_id) {
        if (early_materialized_slots.count(slot_id)) {
            final_chunk->append_column(sorted_early_chunk->get_column_by_slot_id(slot_id), slot_id);
        } else {
            final_chunk->append_column(sorted_late_chunk->get_column_by_slot_id(slot_id), slot_id);
        }
    }
    return final_chunk;
}

Status OrderedMemTableV3::flush(FlushCallBack callback) {
    // LOG(INFO) << "flush mem table, late chunks: " << _staging_late_chunks.size() << ", merged runs: " << _merged_runs.num_chunks();
    // handle materialze
    for (size_t i = 0;i < _merged_runs.num_chunks();i++) {
        _chunk_slice.reset(_merged_runs.get_chunk(i));
        while (!_chunk_slice.empty()) {
            auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
            auto final_chunk = _late_materialize(chunk);
            RETURN_IF_ERROR(callback(final_chunk));
        }
    }
    
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
    _merged_runs.clear();
    // _sorted_early_chunks.clear();
    _staging_late_chunks.clear();
    _final_late_chunks.clear();
    _col_idx_to_slot_id.clear();
    return Status::OK();
}

Status OrderedMemTableV3::done() {
    RETURN_IF_ERROR(_partial_sort(true));
    _permutation = {};
    _unsorted_early_chunk.reset();
    // _staging_late_chunks.clear();
    // @TODO seems no need
    // _col_idx_to_slot_id.clear();
    // _unsorted_chunk.reset();
    RETURN_IF_ERROR(_merge_sorted());
    return Status::OK();
}

// void OrderedMemTableV3::_append_chunks(ChunkPtr chunk) {
//     DCHECK(!src_chunk->is_empty());
//     size_t mem_usage = chunk->memory_usage();
//     _staging_unsorted_rows += chunk->num_rows();
//     _staging_unsorted_bytes += chunk->bytes_usage();
//     _staging_unsorted_chunks.emplace_back(std::move(chunk));
//     _tracker->consume(mem_usage);
//     COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);
// }


void OrderedMemTableV3::_split_and_append_chunks(ChunkPtr src_chunk) {
    // split chunk to early chunk and late chunk
    DCHECK(!src_chunk->is_empty());

    if (_col_idx_to_slot_id.empty()) {
        // LOG(INFO) << "first chunk in mem table, late chunks: " << _staging_late_chunks.size();
        const auto& slot_id_to_col_idx = src_chunk->get_slot_id_to_index_map();
        _col_idx_to_slot_id.resize(slot_id_to_col_idx.size());
        for (const auto& [slot_id, col_idx]: slot_id_to_col_idx) {
            _col_idx_to_slot_id[col_idx] = slot_id;
        }
    }

    auto& early_materialized_slots = _spiller->options().early_materialized_slots;

    auto dst_early_chunk = std::make_shared<Chunk>();
    auto dst_late_chunk = std::make_shared<Chunk>();

    for (auto col_idx = 0; col_idx <  src_chunk->num_columns();col_idx++) {
        auto slot_id = _col_idx_to_slot_id[col_idx];
        auto src_column = src_chunk->get_column_by_index(col_idx);
        if (early_materialized_slots.count(slot_id)) {
            dst_early_chunk->append_column(src_column, slot_id);
        } else {
            dst_late_chunk->append_column(src_column, slot_id);
        }
    }


    // size_t chunk_idx = _staging_late_chunks.size();
    // use ordinal column to find other columns in _staging_late_chunks
    // auto ordinal_column = OrdinalColumn::create();
    // auto& ordinal_data = down_cast<OrdinalColumn*>(ordinal_column.get())->get_data();
    // raw::make_room(&ordinal_data, src_chunk->num_rows());
    // for (uint32_t offset = 0;offset < src_chunk->num_rows();offset++) {
    //     ordinal_data[offset] = ((uint32_t)(chunk_idx << 16) | offset);
    // }

    // dst_early_chunk->append_column(ordinal_column, ORDINAL_COLUMN_SLOT_ID);
    // LOG(INFO) << "split chunk, early chunk columns: " << dst_early_chunk->num_columns() << ", mem: " << dst_early_chunk->memory_usage()
    //     << ", late chunk columns: " << dst_late_chunk->num_columns() << ", mem: " << dst_late_chunk->memory_usage();

    size_t mem_usage = dst_early_chunk->memory_usage() + dst_late_chunk->memory_usage();
    _staging_unsorted_rows += src_chunk->num_rows();
    _staging_unsorted_bytes += src_chunk->bytes_usage();
    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);

    _staging_unsorted_early_chunks.emplace_back(std::move(dst_early_chunk));
    _staging_late_chunks.emplace_back(std::move(dst_late_chunk));
}

void OrderedMemTableV3::_concat_chunks(const std::vector<ChunkPtr>& src_chunks, size_t num_rows, ChunkPtr& dst_chunk) {
    DCHECK(!src_chunks.empty());
    dst_chunk = src_chunks[0]->clone_empty(num_rows);
    const size_t num_columns = dst_chunk->num_columns();
    for (size_t i = 0;i < num_columns;i++) {
        auto dst_column = dst_chunk->get_column_by_index(i);
        auto* dst_data_column = ColumnHelper::get_data_column(dst_column.get());
        if (dst_data_column->is_binary()) {
            reserve_memory<BinaryColumn>(dst_data_column, src_chunks, i);
        } else if (dst_column->is_large_binary()) {
            reserve_memory<LargeBinaryColumn>(dst_data_column, src_chunks, i);
        }
    }
    for (const auto src_chunk: src_chunks) {
        dst_chunk->append(*src_chunk);
    }
}

Status OrderedMemTableV3::_partial_sort(bool done) {
    if (!_staging_unsorted_rows) {
        return Status::OK();
    }
    bool reach_limit = _staging_unsorted_rows >= max_buffered_rows || _staging_unsorted_bytes >= max_buffered_bytes;
    if (done || reach_limit) {
        // LOG(INFO) << "do partial_sort, rows: " << _staging_unsorted_rows << ", bytes: " << _staging_unsorted_bytes;
        _concat_chunks(_staging_unsorted_early_chunks, _staging_unsorted_rows, _unsorted_early_chunk);
        _staging_unsorted_early_chunks.clear();

        _concat_chunks(_staging_late_chunks, _staging_unsorted_rows, _concat_late_chunk);
        _staging_late_chunks.clear();

        // assign ordinal here
        auto ordinal_column = OrdinalColumn::create();
        auto& ordinal_data = down_cast<OrdinalColumn*>(ordinal_column.get())->get_data();
        raw::make_room(&ordinal_data, _staging_unsorted_rows);
        const size_t chunk_idx = _final_late_chunks.size();
        for (uint32_t offset = 0;offset < _staging_unsorted_rows;offset++) {
            ordinal_data[offset] = ((uint32_t)(chunk_idx << 24) | offset);
        }
        _unsorted_early_chunk->append_column(ordinal_column, ORDINAL_COLUMN_SLOT_ID);

        _final_late_chunks.emplace_back(std::move(_concat_late_chunk));


        RETURN_IF_ERROR(_unsorted_early_chunk->upgrade_if_overflow());


        DataSegment segment(_sort_exprs, _unsorted_early_chunk);

        _permutation.resize(0);
        {
            SCOPED_TIMER(_spiller->metrics().sort_chunk_timer);
            RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), segment.order_by_columns, _sort_desc, &_permutation));
        }

        auto sorted_early_chunk = _unsorted_early_chunk->clone_empty_with_slot(_unsorted_early_chunk->num_rows());
        {
            SCOPED_TIMER(_spiller->metrics().materialize_chunk_timer);
            materialize_by_permutation(sorted_early_chunk.get(), {_unsorted_early_chunk}, _permutation);
            RETURN_IF_ERROR(sorted_early_chunk->upgrade_if_overflow());
        }

        _sorted_early_chunks.emplace_back(std::move(sorted_early_chunk));
        _unsorted_early_chunk->reset();

        _staging_unsorted_rows = 0;
        _staging_unsorted_bytes = 0;
    } 
    return Status::OK();
}

Status OrderedMemTableV3::_merge_sorted() {
    // LOG(INFO) << "merge sorted, sorted_early_chunks: " << _sorted_early_chunks.size();
    SCOPED_TIMER(_spiller->metrics().merge_chunk_timer);
    RETURN_IF_ERROR(merge_sorted_chunks(_sort_desc, _sort_exprs, _sorted_early_chunks, &_merged_runs));
    _sorted_early_chunks.clear();
    return Status::OK();
}
} // namespace starrocks::spill