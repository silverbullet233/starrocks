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

#include "exec/pipeline/lookup_request.h"
#include <brpc/controller.h>

#include "column/chunk.h"
#include "serde//column_array_serde.h"
#include "util/raw_container.h"
#include "connector/hive_connector.h"
#include "runtime/descriptors.h"
#include "util/logging.h"

namespace starrocks::pipeline {

Status LocalLookUpRequestContext::collect_input_columns(ChunkPtr chunk) {
    // put all related columns into chunk, include source_id column and other related columns
    size_t num_rows = fetch_ctx->request_chunk->num_rows();
    for (const auto& [slot_id, idx] : fetch_ctx->request_chunk->get_slot_id_to_index_map()) {
        auto src_col = fetch_ctx->request_chunk->get_column_by_index(idx);
        auto dst_col = chunk->get_column_by_slot_id(slot_id);
        dst_col->append(*src_col, 0, num_rows);
    }
    chunk->check_or_die();
    return Status::OK();
}
StatusOr<size_t> LocalLookUpRequestContext::fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                                 const std::vector<SlotDescriptor*>& slots, size_t start_offset) {
    size_t num_rows = fetch_ctx->request_chunk->num_rows();
    for (const auto& slot : slots) {
        auto src_col = result_chunk->get_column_by_slot_id(slot->id());
        auto dst_col = src_col->clone_empty();
        dst_col->append(*src_col, start_offset, num_rows);
        DCHECK(!fetch_ctx->response_columns.contains(slot->id())) << "slot id: " << slot->id()
                                                      << " already exists in response columns";
        fetch_ctx->response_columns[slot->id()] = std::move(dst_col);
    }
    return num_rows;
}

void LocalLookUpRequestContext::callback(const Status& status) {
    // @TODO call back function
}

Status RemoteLookUpRequestContext::collect_input_columns(ChunkPtr chunk) {
    for (size_t i = 0;i < request->request_columns_size(); i++) {
        const auto& pcolumn = request->request_columns(i);
        SlotId slot_id = pcolumn.slot_id();
        int64_t data_size = pcolumn.data_size();
        // @TODO we should know slot desc
        auto dst_col = chunk->get_column_by_slot_id(slot_id);
        auto col = dst_col->clone_empty();
        const uint8_t* buff = reinterpret_cast<const uint8_t*>(pcolumn.data().data());
        auto ret = serde::ColumnArraySerde::deserialize(buff, col.get());
        if (ret == nullptr) {
            auto msg = fmt::format("deserialize column failed, slot_id: {}, data_size: {}", slot_id, data_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        dst_col->append(*col, 0, col->size());
    }
    chunk->check_or_die();
    return Status::OK();
}

StatusOr<size_t> RemoteLookUpRequestContext::fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                                   const std::vector<SlotDescriptor*>& slots, size_t start_offset) {
    size_t num_rows = request_chunk->num_rows();
    std::vector<ColumnPtr> columns;
    size_t max_serialized_size = 0;
    for (const auto& slot : slots) {
        auto src_col = result_chunk->get_column_by_slot_id(slot->id());
        auto dst_col = src_col->clone_empty();
        dst_col->append(*src_col, start_offset, num_rows);
        max_serialized_size += serde::ColumnArraySerde::max_serialized_size(*dst_col);
        columns.emplace_back(std::move(dst_col));
    }
    // @TODO reuse serialize buffer
    raw::RawString serialize_buffer;
    serialize_buffer.resize(max_serialized_size);
    uint8_t* buff = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buff;
    for (size_t i = 0;i < slots.size();i++) {
        auto column = columns[i];
        auto pcolumn = response->add_columns();
        pcolumn->set_slot_id(slots[i]->id());
        uint8_t* start = buff;
        buff = serde::ColumnArraySerde::serialize(*column, buff);
        pcolumn->set_data_size(buff - start);
    }
    size_t actual_serialize_size = buff - begin;
    auto* brpc_cntl = static_cast<brpc::Controller*>(cntl);
    brpc_cntl->response_attachment().append(serialize_buffer.data(), actual_serialize_size);

    return num_rows;
}

void RemoteLookUpRequestContext::callback(const Status& status) {
    status.to_protobuf(response->mutable_status());
    done->Run();
}

Status IcebergV3LookUpTask::process(RuntimeState* state) {
    // 获取请求上下文中的信息
    if (_ctx->request_ctxs.empty()) {
        return Status::OK();
    }
    
    // 获取第一个请求上下文作为参考
    auto& request_ctx = _ctx->request_ctxs[0];
    TupleId request_tuple_id = request_ctx->request_tuple_id();
    
    // 获取tuple描述符
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(request_tuple_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor for tuple_id: " + std::to_string(request_tuple_id));
    }
    
    // 获取表描述符
    const TableDescriptor* table_desc = tuple_desc->table_desc();
    if (table_desc == nullptr) {
        return Status::InternalError("Failed to get table descriptor for tuple_id: " + std::to_string(request_tuple_id));
    }
    
    // 检查是否为Iceberg表
    const IcebergTableDescriptor* iceberg_table = dynamic_cast<const IcebergTableDescriptor*>(table_desc);
    if (iceberg_table == nullptr) {
        return Status::InternalError("Table is not an Iceberg table");
    }
    // @TODO: we should keep some options in original scan node, and reuse them here
    
    // 创建TPlanNode和THdfsScanNode
    // 注意：在实际实现中，需要从RuntimeState或其他地方获取这些信息
    TPlanNode plan_node;
    plan_node.__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    plan_node.__set_node_id(0); // 需要根据实际情况设置
    
    // 设置THdfsScanNode
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(request_tuple_id);
    hdfs_scan_node.__set_case_sensitive(true);
    hdfs_scan_node.__set_can_use_any_column(false);
    hdfs_scan_node.__set_can_use_min_max_count_opt(false);
    hdfs_scan_node.__set_use_partition_column_value_only(false);
    
    // 设置输出列信息
    std::vector<SlotDescriptor*> output_slots = tuple_desc->slots();
    std::vector<std::string> hive_column_names;
    for (size_t i = 0; i < output_slots.size(); i++) {
        hive_column_names.push_back(output_slots[i]->col_name());
    }
    hdfs_scan_node.__set_hive_column_names(hive_column_names);
    
    // 设置扫描谓词（如果有的话）
    // 注意：在实际实现中，需要根据具体的查询条件设置谓词
    std::vector<TExpr> conjuncts;
    // 这里可以添加具体的谓词表达式
    // hdfs_scan_node.__set_conjuncts(conjuncts); // 注意：THdfsScanNode可能没有conjuncts字段
    
    plan_node.__set_hdfs_scan_node(hdfs_scan_node);
    
    // 创建ConnectorScanNode（模拟）
    // 注意：在实际实现中，需要从RuntimeState或其他地方获取真实的ConnectorScanNode
    ConnectorScanNode* scan_node = nullptr; // 需要根据实际情况获取
    
    // 创建HiveDataSourceProvider
    auto provider = std::make_unique<connector::HiveDataSourceProvider>(scan_node, plan_node);
    
    // 处理每个请求上下文
    for (auto& request_ctx : _ctx->request_ctxs) {
        // 收集输入列
        ChunkPtr request_chunk = std::make_shared<Chunk>();
        RETURN_IF_ERROR(request_ctx->collect_input_columns(request_chunk));
        
        // 从请求chunk中提取row_id信息
        // 这里假设请求chunk包含了row_id列，用于定位具体的行
        // 实际实现中需要根据具体的row_id来构建扫描范围
        
        // 创建扫描范围
        THdfsScanRange hdfs_scan_range;
        hdfs_scan_range.__set_file_length(0); // 需要根据实际情况设置
        hdfs_scan_range.__set_offset(0);
        hdfs_scan_range.__set_length(0);
        hdfs_scan_range.__set_partition_id(0);
        hdfs_scan_range.__set_relative_path(""); // 需要根据实际情况设置
        hdfs_scan_range.__set_file_format(THdfsFileFormat::PARQUET); // Iceberg通常使用Parquet格式
        hdfs_scan_range.__set_full_path(""); // 需要根据实际情况设置
        
        // 设置Iceberg V3特有的字段
        hdfs_scan_range.__set_first_row_id(0); // 需要根据实际情况设置
        
        TScanRange scan_range;
        scan_range.__set_hdfs_scan_range(hdfs_scan_range);
        
        // 创建HiveDataSource
        auto data_source = std::make_unique<connector::HiveDataSource>(provider.get(), scan_range);
        
        // 打开数据源
        RETURN_IF_ERROR(data_source->open(state));
        
        // 读取数据
        ChunkPtr result_chunk = std::make_shared<Chunk>();
        size_t total_rows = 0;
        
        Status status = data_source->get_next(state, &result_chunk);
        while (status.ok()) {
            if (result_chunk->num_rows() == 0) {
                break;
            }
            
            // 处理结果数据
            // 这里需要根据实际情况填充响应
            // 例如：request_ctx->fill_response(result_chunk, source_id_slot, slots, start_offset);
            
            total_rows += result_chunk->num_rows();
            
            // 清空结果chunk，准备下一次读取
            result_chunk->reset();
            
            status = data_source->get_next(state, &result_chunk);
        }
        
        // 关闭数据源
        data_source->close(state);
        
        LOG(INFO) << "IcebergV3LookUpTask::process - Processed " << total_rows << " rows for tuple_id: " << request_tuple_id;
    }
    
    return Status::OK();
}

}