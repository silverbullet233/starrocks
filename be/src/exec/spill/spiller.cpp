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

#include "exec/spill/spiller.h"

#include <butil/iobuf.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller_path_provider.h"
#include "exec/spill/log_block_manager.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"

namespace starrocks {
// Not thread safe
class ColumnSpillFormater : public SpillFormater {
public:
    ColumnSpillFormater(ChunkBuilder chunk_builder) : _chunk_builder(std::move(chunk_builder)) {}
    ColumnSpillFormater(ChunkBuilder chunk_builder, const BlockCompressionCodec* compress_codec):
        _chunk_builder(std::move(chunk_builder)), _compress_codec(compress_codec) {}
    Status spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                        const ChunkPtr& chunk) const noexcept override;
    StatusOr<ChunkUniquePtr> restore_from_fmt(SpillFormatContext& context,
                                              std::unique_ptr<RawInputStreamWrapper>& readable) const override;
    Status flush(std::unique_ptr<WritableFile>& writable) const override;

private:
    size_t _spill_size(const ChunkPtr& chunk) const;
    ChunkBuilder _chunk_builder;
    const BlockCompressionCodec* _compress_codec = nullptr;
};

size_t ColumnSpillFormater::_spill_size(const ChunkPtr& chunk) const {
    size_t serialize_sz = 0;
    for (const auto& column : chunk->columns()) {
        serialize_sz += serde::ColumnArraySerde::max_serialized_size(*column);
    }
    return serialize_sz + sizeof(serialize_sz);
}

Status ColumnSpillFormater::spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                                         const ChunkPtr& chunk) const noexcept {
    size_t serialize_sz = _spill_size(chunk);
    // @TODO serialize_sz may be larger than real size
    context.uncompressed_buffer.resize(serialize_sz);
    DCHECK_GT(serialize_sz, 4);

    auto* buff = reinterpret_cast<uint8_t*>(context.uncompressed_buffer.data());
    uint8_t* begin = buff;
    // UNALIGNED_STORE64(buff, serialize_sz);
    // @TODO record compressed size and decompressed size
    // buff += sizeof(serialize_sz);
    // uint8_t* start = buff;
    // buff += sizeof(size_t) * 2;

    for (const auto& column : chunk->columns()) {
        buff = serde::ColumnArraySerde::serialize(*column, buff);
    }
    size_t uncompressed_size = buff - begin;
    context.uncompressed_buffer.resize(uncompressed_size);
    LOG(INFO) << "max_serialize_sz: " << serialize_sz << ", real size: " << uncompressed_size;
    Slice compress_input(context.uncompressed_buffer.data(), uncompressed_size);
    Slice compressed_slice;

    // @TODO fix compression pool issue
    // if (!use_compression_pool(_compress_codec->type())) {
    //     RETURN_IF_ERROR(_compress_codec->compress(compress_input, &compressed_slice,
    //         true, uncompressed_size, nullptr, &context.compressed_buffer));
    // } else {
        int max_compressed_size = _compress_codec->max_compressed_len(uncompressed_size);
        if (context.compressed_buffer.size() < max_compressed_size) {
            context.compressed_buffer.resize(max_compressed_size);
        }
        compressed_slice = Slice(context.compressed_buffer.data(), context.compressed_buffer.size());
        RETURN_IF_ERROR(_compress_codec->compress(compress_input, &compressed_slice));
        context.compressed_buffer.resize(compressed_slice.size);
    // }
    LOG(INFO) << "uncompressed size: " << uncompressed_size << ", compressed size: " << compressed_slice.size
        << "," << context.compressed_buffer.size();

    uint8_t tmp_buff[sizeof(size_t) * 2];
    UNALIGNED_STORE64(tmp_buff, compressed_slice.size);
    UNALIGNED_STORE64(tmp_buff + sizeof(size_t), uncompressed_size);

    RETURN_IF_ERROR(writable->append(Slice(tmp_buff, sizeof(size_t) * 2)));
    RETURN_IF_ERROR(writable->append(compressed_slice));
    {
        // @TODO multi-thread issue?
        std::string tmp;
        tmp.resize(uncompressed_size);
        Slice output{tmp.data(), uncompressed_size};
        if (Status st = _compress_codec->decompress(compressed_slice, &output);!st.ok()) {
            LOG(INFO) << "cannot decompress, error: " << st.to_string();
            DCHECK(false);
        }
    }

    // RETURN_IF_ERROR(writable->append(context.io_buffer));
    // @TODO record spill bytes
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnSpillFormater::restore_from_fmt(SpillFormatContext& context,
                                                               std::unique_ptr<RawInputStreamWrapper>& readable) const {
    // size_t serialize_sz;
    size_t compressed_size, uncompressed_size;
    RETURN_IF_ERROR(readable->read_fully(&compressed_size, sizeof(size_t)));
    RETURN_IF_ERROR(readable->read_fully(&uncompressed_size, sizeof(size_t)));
    // RETURN_IF_ERROR(readable->read_fully(&serialize_sz, sizeof(serialize_sz)));
    // DCHECK_GT(serialize_sz, sizeof(serialize_sz));
    LOG(INFO) << "read compressed size: " << compressed_size << ", uncompressed size: " << uncompressed_size;
    // context.io_buffer.resize(serialize_sz);
    context.compressed_buffer.resize(compressed_size);
    context.uncompressed_buffer.resize(uncompressed_size);


    auto buf = reinterpret_cast<uint8_t*>(context.compressed_buffer.data());
    RETURN_IF_ERROR(readable->read_fully(buf, compressed_size));
    LOG(INFO) << "read compressed data done";
    // decompress
    Slice input_slice(context.compressed_buffer.data(), compressed_size);
    Slice uncompressed_slice(context.uncompressed_buffer.data(), uncompressed_size);
    RETURN_IF_ERROR(_compress_codec->decompress(input_slice, &uncompressed_slice));
    LOG(INFO) << "decompress done";

    // deserialize
    auto chunk = _chunk_builder();
    const uint8_t* read_cursor = reinterpret_cast<uint8_t*>(context.uncompressed_buffer.data());
    for (const auto& column: chunk->columns()) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    }
    LOG(INFO) << "return chunk";
    // auto buff = reinterpret_cast<uint8_t*>(context.io_buffer.data());
    // RETURN_IF_ERROR(readable->read_fully(buff, serialize_sz - sizeof(serialize_sz)));

    // auto chunk = _chunk_builder();
    // const uint8_t* read_cursor = buff;
    // for (const auto& column : chunk->columns()) {
    //     read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
    // }
    return chunk;
}

Status ColumnSpillFormater::flush(std::unique_ptr<WritableFile>& writable) const {
    // TODO: test flush async and sync
    RETURN_IF_ERROR(writable->flush(WritableFile::FLUSH_ASYNC));
    return Status::OK();
}

StatusOr<std::unique_ptr<SpillFormater>> SpillFormater::create(SpillFormaterType type, ChunkBuilder chunk_builder) {
    if (type == SpillFormaterType::SPILL_BY_COLUMN) {
        return std::make_unique<ColumnSpillFormater>(std::move(chunk_builder));
    } else {
        return Status::InternalError(fmt::format("unsupported spill type:{}", type));
    }
}


StatusOr<std::unique_ptr<SpillFormater>> SpillFormater::create(SpilledOptions& options) {
    auto format_type = options.spill_type;
    if (format_type == SpillFormaterType::SPILL_BY_COLUMN) {
        auto compress_type = options.compress_type;
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(compress_type, &codec));
        return std::make_unique<ColumnSpillFormater>(std::move(options.chunk_builder), codec);
    } else {
        return Status::InternalError(fmt::format("unsupported spill type:{}", format_type));
    }
}


Status Spiller::prepare(RuntimeState* state) {
    // prepare
    // ASSIGN_OR_RETURN(_spill_fmt, SpillFormater::create(_opts.spill_type, _opts.chunk_builder));
    // ASSIGN_OR_RETURN(_spill_fmt, SpillFormater::create(_opts));

    for (size_t i = 0; i < _opts.mem_table_pool_size; ++i) {
        if (_opts.is_unordered) {
            _mem_table_pool.push(
                    std::make_unique<UnorderedMemTable>(state, _opts.spill_file_size, state->instance_mem_tracker()));
        } else {
            _mem_table_pool.push(std::make_unique<OrderedMemTable>(&_opts.sort_exprs->lhs_ordering_expr_ctxs(),
                                                                   _opts.sort_desc, state, _opts.spill_file_size,
                                                                   state->instance_mem_tracker()));
        }
    }

    _file_group = std::make_shared<SpilledFileGroup>(*_spill_fmt);

    ASSIGN_OR_RETURN(_formatter, spill::create_formatter(&_opts));
    _block_group = std::make_shared<spill::BlockGroup>(_formatter.get());
    // _block_manager = _opts.block_manager;
    _block_manager = state->query_ctx()->spill_block_manager();
    // _block_manager = std::make_shared<spill::LogBlockManager>();
    // RETURN_IF_ERROR(_block_manager->open());

    return Status::OK();
}

Status Spiller::_open(RuntimeState* state) {
    std::lock_guard guard(_mutex);
    if (_has_opened) {
        return Status::OK();
    }

    RETURN_IF_ERROR(get_block_compression_codec(_opts.compress_type, &_compress_codec));

    // init path provider
    ASSIGN_OR_RETURN(_path_provider, _opts.path_provider_factory());
    RETURN_IF_ERROR(_path_provider->open(state));
    _has_opened = true;

    return Status::OK();
}

Status Spiller::_flush_and_closed(std::unique_ptr<WritableFile>& writable) {
    // flush
    RETURN_IF_ERROR(_spill_fmt->flush(writable));
    // be careful close method return a status
    RETURN_IF_ERROR(writable->close());
    writable.reset();
    return Status::OK();
}

Status Spiller::_run_flush_task(RuntimeState* state, const MemTablePtr& mem_table) {
    if (state->is_cancelled()) {
        LOG(INFO) << "query cancelled, just return";
        return Status::OK();
    }
    RETURN_IF_ERROR(this->_open(state));
    // @TODO cache a file and write multi times
    // prepare current file
    ASSIGN_OR_RETURN(auto file, _path_provider->get_file());
    ASSIGN_OR_RETURN(auto writable, file->as<WritableFile>());
    // TODO: reuse io context
    SpillFormatContext spill_ctx;
    {
        std::lock_guard guard(_mutex);
        _file_group->append_file(std::move(file));
    }
    DCHECK(writable != nullptr);
    {
        // flush all pending result to spilled files
        size_t num_rows_flushed = 0;
        RETURN_IF_ERROR(mem_table->flush([&](const auto& chunk) {
            num_rows_flushed += chunk->num_rows();
            RETURN_IF_ERROR(_spill_fmt->spill_as_fmt(spill_ctx, writable, chunk));
            return Status::OK();
        }));
        TRACE_SPILL_LOG << "spill flush rows:" << num_rows_flushed << ",spiller:" << this;
    }
    // then release the pending memory
    RETURN_IF_ERROR(_flush_and_closed(writable));
    return Status::OK();
}

void Spiller::_update_spilled_task_status(Status&& st) {
    std::lock_guard guard(_mutex);
    if (_spilled_task_status.ok() && !st.ok()) {
        _spilled_task_status = std::move(st);
    }
}

StatusOr<std::shared_ptr<SpilledInputStream>> Spiller::_acquire_input_stream(RuntimeState* state) {
    DCHECK(_restore_tasks.empty());
    std::vector<SpillRestoreTaskPtr> restore_tasks;
    std::shared_ptr<SpilledInputStream> input_stream;
    if (_opts.is_unordered) {
        ASSIGN_OR_RETURN(auto res, _file_group->as_flat_stream(_parent));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    } else {
        ASSIGN_OR_RETURN(auto res, _file_group->as_sorted_stream(_parent, state, _opts.sort_exprs, _opts.sort_desc));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    }

    std::lock_guard guard(_mutex);
    {
        // put all restore_tasks to pending lists
        for (auto& task : restore_tasks) {
            _restore_tasks.push(task);
        }
        _total_restore_tasks = _restore_tasks.size();
    }
    return input_stream;
}

StatusOr<std::shared_ptr<spill::InputStream>> Spiller::_acquire_input_stream_v2(RuntimeState* state) {
    if (_opts.is_unordered) {
        return _block_group->as_unordered_stream();
    }
    return _block_group->as_ordered_stream(state, _opts.sort_exprs, _opts.sort_desc);
}

Status Spiller::_decrease_running_flush_tasks() {
    if (_running_flush_tasks.fetch_sub(1) == 1) {
        if (_flush_all_callback) {
            RETURN_IF_ERROR(_flush_all_callback());
            if (_inner_flush_all_callback) {
                RETURN_IF_ERROR(_inner_flush_all_callback());
            }
        }
    }
    return Status::OK();
}

// refactor begin

Status Spiller::flush_mem_table(RuntimeState* state, const MemTablePtr& mem_table) {
    if (state->is_cancelled()) {
        LOG(INFO) << "query is cancelled, just return";
        return Status::OK();
    }
    // LOG(INFO) << "invoke flush_mem_table";
    // flush mem table
    // acuire block
    spill::AcquireBlockOptions opts;
    opts.query_id = state->query_id();
    opts.plan_node_id = _opts.plan_node_id;
    opts.name = _opts.name;
    // @TODO plan node id, name
    ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));
    // LOG(INFO) << "get block";
    spill::FormatterContext ctx;

    size_t num_rows_flushed = 0;
    // @TODO remove callback
    RETURN_IF_ERROR(mem_table->flush([&] (const auto& chunk) {
        num_rows_flushed += chunk->num_rows();
        // LOG(INFO) << "serialize chunk";
        RETURN_IF_ERROR(_formatter->serialize(ctx, chunk, block));
        return Status::OK();
    }));
    // LOG(INFO) << "flush block begin";
    RETURN_IF_ERROR(block->flush());
    _block_manager->release_block(block);
    // LOG(INFO) << "flush block end";
    std::lock_guard<std::mutex> l(_mutex);
    _block_group->append(block);
    LOG(INFO) << "append block: " << block->debug_string();
    return Status::OK();
}
// refactor end


} // namespace starrocks