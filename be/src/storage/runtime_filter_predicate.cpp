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

#include "storage/runtime_filter_predicate.h"
#include <cstring>
#include "simd/simd.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

bool RuntimeFilterPredicate::init(int32_t driver_sequence) {
    if (_rf) {
        return true;
    }
    _rf = _rf_desc->runtime_filter(driver_sequence);
    return _rf != nullptr;
}

Status RuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    auto column = chunk->get_column_by_id(_column_id);

    // @TODO change to selection now, will support later
    // std::vector<uint16_t> sels;
    // for (uint16_t i = from;i < to;i++) {
    //     if (selection[i]) {
    //         sels.push_back(i);
    //     }
    // }
    // if (_rf->num_hash_partitions() == 0) {
    //     return Status::OK();
    // }
    // if (_rf_desc->filter_id() >= 0 && _rf_desc->filter_id() <= 5) {
    //     LOG(INFO) << "RuntimeFilterPredicate::evaluate, rf: " <<_rf_desc->debug_string() << ", cid: " << _column_id << ", column: " << column->get_name()
    //     << ", partitions: " << _rf->num_hash_partitions() << ", column: " << column->get_name();
    //     for (const auto& [cid, idx]: chunk->get_column_id_to_index_map()) {
    //         LOG(INFO) << "before eval runtime_filter_predicate, column_id: " << cid << ", idx: " << idx << ", column: " << chunk->get_column_by_index(idx)->get_name();
    //     }
    // }
    // JoinRuntimeFilter::RunningContext ctx;
    // ctx.use_merged_selection = false;
    // ctx.compatibility = true;
    // ctx.selection.resize(column->size());
    // // ctx.merged_selection.resize(column->size());
    // // memcpy(ctx.selection.data(), selection, column->size());
    // ctx.hash_values.resize(column->size());
    if (_rf->num_hash_partitions() > 0) {
        hash_values.resize(column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, selection, from, to, hash_values);
        // _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, &ctx);
        // _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, sels.data(), sels.size(), hash_values);
    }
    // _rf->evaluate(column.get(), ctx.hash_values, selection, from, to);
    _rf->evaluate(column.get(), hash_values, selection, from, to);
    // _rf->evaluate(column.get(), &ctx);
    // for (uint16_t i = from;i < to;i++) {
    //     selection[i] &= ctx.selection[i];
    // }
    return Status::OK();
}

Status DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    RETURN_IF_ERROR(prepare());

    auto column = chunk->get_column_by_id(_column_id).get();
    if (column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(column);
        // dict code column must be int column
        const auto& data = GetContainer<TYPE_INT>::get_data(nullable_column->data_column());
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (uint16_t i = from;i < to;i++) {
                if (!selection[i]) continue;
                if (null_data[i]) {
                    selection[i] = _rf->has_null();
                } else {
                    selection[i] = _result[data[i]];
                }
            }
        } else {
            for (uint16_t i = from;i < to;i ++) {
                if (!selection[i]) continue;
                selection[i] = _result[data[i]];
            }
        }
    } else {
        const auto& data = GetContainer<TYPE_INT>::get_data(column);
        for (uint16_t i = from;i < to;i ++) {
            if (!selection[i]) continue;
            selection[i] = _result[data[i]];
        }
    }
    return Status::OK();
}

Status DictColumnRuntimeFilterPredicate::prepare() {
    DCHECK(_rf != nullptr);
    if (!_result.empty()) {
        return Status::OK();
    }
    LOG(INFO) << "DictColumnRuntimeFilterPredicate::prepare, rf: " << _rf_desc->debug_string();
    auto binary_column = BinaryColumn::create();
    std::vector<Slice> data_slice;
    for (const auto& word : _dict_words) {
        data_slice.emplace_back(word.data(), word.size());
    }
    binary_column->append_strings(data_slice.data(), data_slice.size());

    JoinRuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    ctx.selection.assign(_dict_words.size(), 1);

    if (_rf->num_hash_partitions() > 0) {
        // compute hash
        ctx.hash_values.resize(binary_column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {binary_column.get()}, &ctx);
    }
    _rf->evaluate(binary_column.get(), &ctx);
    _result.resize(_dict_words.size());

    for (size_t i = 0; i < _dict_words.size(); ++i) {
        // LOG(INFO) << "dict code: " << i << ", word: " << _dict_words[i] << ", selection: " << (int)ctx.selection[i];
        _result[i] = ctx.selection[i];
    }


    return Status::OK();
}

void RuntimeFilterPredicates::_update_selectivity_map() {
    // LOG(INFO) << "_update_selectivity_map";
    _selectivity_map.clear();
    for (size_t i = 0;i < _sampling_predicates.size();i ++) {
        auto pred = _sampling_predicates[i];
        double filter_ratio = _filter_rows[i] * 1.0 / _sample_rows;
        if (filter_ratio <= 0.05) {
            // very good, only keep one
            _selectivity_map.clear();
            _selectivity_map.emplace(filter_ratio, pred);
            // LOG(INFO) << "very good filter, just use this one, filter_ratio: " << filter_ratio << ", filter_id: " << pred->get_rf_desc()->filter_id();
            return;
        }
        if (filter_ratio <= 0.5) {
            if (_selectivity_map.size() < 3) {
                _selectivity_map.emplace(filter_ratio, pred);
            } else {
                auto iter = _selectivity_map.end();
                iter --;
                if (filter_ratio < iter->first) {
                    _selectivity_map.erase(iter);
                    _selectivity_map.emplace(filter_ratio, pred);
                }
            }
        }
    }
    // LOG(INFO) << "selectivity_map size: " << _selectivity_map.size();
    // for (const auto& [ratio, pred]: _selectivity_map) {
    //     LOG(INFO) << "final used pred, filter_ratio: " << ratio << ", filter_id: " << pred->get_rf_desc()->filter_id();
    // }
}

Status RuntimeFilterPredicates::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    // @TODO need maintain state
    // INIT -> SAMPLE -> NORMAL
    switch (_state) {
        case INIT: {
            if (_sampling_predicates.empty()) {
                for (auto pred: _rf_predicates) {
                    if (pred->init(_driver_sequence)) {
                        _sampling_predicates.push_back(pred);
                        _filter_rows.push_back(0);
                    }
                }
                if (!_sampling_predicates.empty()) {
                    _state = SAMPLE;
                    _sample_rows = 0;
                    // LOG(INFO) << "used predicates size: " << _sampling_predicates.size() << ", go to SAMPLE, " << (void*)this;
                } else {
                    // no filter can be used, just return
                    return Status::OK();
                }
            }
            break;
        }
        case SAMPLE: {
            size_t old_count = SIMD::count_nonzero(selection + from, to - from);
            _selection.resize(chunk->num_rows());
            memcpy(_selection.data() + from, selection + from, to - from);

            std::vector<uint8_t> tmp_selection;
            tmp_selection.resize(chunk->num_rows());
            for (size_t i = 0;i < _sampling_predicates.size();i ++) {
                auto pred = _sampling_predicates[i];
                auto& filter_rows = _filter_rows[i];
                memcpy(tmp_selection.data() + from, _selection.data() + from, to - from);
                RETURN_IF_ERROR(pred->evaluate(chunk, tmp_selection.data(), from, to));
                size_t new_count = SIMD::count_nonzero(tmp_selection.data() + from, to - from);
                filter_rows += old_count - new_count;

                for (uint16_t idx = from; idx < to;idx ++) {
                    selection[idx] = selection[idx] & tmp_selection[idx];
                }
            }
            _sample_rows += old_count;
            if (_sample_rows >= 4096) {
                // should decide selectivity map
                _update_selectivity_map();
                _state = NORMAL;
                _skip_rows = 0;
                // LOG(INFO) << "sample rows: " << _sample_rows << ", go to NORMAL, " << (void*)this;
            }
            return Status::OK();
        }
        case NORMAL: {
            size_t origin_count = SIMD::count_nonzero(selection, to - from);
            for (auto& [_, pred]: _selectivity_map) {
                RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
                auto remain_count = SIMD::count_nonzero(selection, to - from);
                if (remain_count == 0) {
                    break;
                }
            }
            _skip_rows += origin_count;
            if (_skip_rows >= 1024 * 128) {
                _state = SAMPLE;
                _sample_rows = 0;
                // LOG(INFO) << "skip rows: " << _skip_rows << ", go back to SAMPLE";
            }
            return Status::OK();
        }
        default:
            CHECK(false) << "not support state: " << _state;
            break;
    }

    // INIT: collect can used rf, if not empty, use these rfs to sample
    // SAMPLE: sample 4k rows, decide which will be used next time
    // NORMAL: run..

    // for (auto& pred: _rf_predicates) {
    //     if (pred->init(_driver_sequence)) {
    //         size_t old_count = SIMD::count_nonzero(selection, to - from);
    //         RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
    //         size_t new_count = SIMD::count_nonzero(selection, to - from);
    //         LOG(INFO) << "rf filter " << old_count - new_count << " rows, old:" << old_count << ", new:" << new_count <<
    //         ", filter_id:" << pred->get_rf_desc()->filter_id();
    //     }
    // }
    return Status::OK();
}


Status RuntimeFilterPredicatesRewriter::rewrite(ObjectPool* obj_pool,
    RuntimeFilterPredicates& preds, const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators, const Schema& schema) {
    
    auto& predicates = preds._rf_predicates;
    for (size_t i = 0;i < predicates.size();i ++) {
        auto* pred = predicates[i];
        ColumnId column_id = pred->get_column_id();
        if (!column_iterators[column_id]->all_page_dict_encoded()) {
            continue;
        }
        // @TODO consider global dict column
        auto* rf_desc = pred->get_rf_desc();
        std::vector<Slice> all_words;
        RETURN_IF_ERROR(column_iterators[column_id]->fetch_all_dict_words(&all_words));
        std::vector<std::string> dict_words;
        for (const auto& word : all_words) {
            dict_words.emplace_back(std::string(word.get_data(), word.get_size()));
        }
        predicates[i] = obj_pool->add(new DictColumnRuntimeFilterPredicate(rf_desc, column_id, std::move(dict_words)));
        LOG(INFO) << "rewrite runtime filter predicate, column id: " << column_id << ", rf_desc: " << rf_desc->debug_string();
        // @TODO how about global_dict
    }
    return Status::OK();
}
}