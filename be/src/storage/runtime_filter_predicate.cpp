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
#include "gutil/strings/fastmem.h"
#include "runtime/mem_tracker.h"
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
    // auto column = chunk->get_column_by_id(_column_id);
    // @TODO pending fix
    auto column = chunk->is_cid_exist(_column_id) ? chunk->get_column_by_id(_column_id): chunk->get_column_by_slot_id(_column_id);
    if (_rf->num_hash_partitions() > 0) {
        hash_values.resize(column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, selection, from, to, hash_values);
    }
    _rf->evaluate(column.get(), hash_values, selection, from, to);
    return Status::OK();
}

StatusOr<uint16_t> RuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size, uint16_t* dst_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    // auto column = chunk->get_column_by_id(_column_id);

    auto column = chunk->is_cid_exist(_column_id) ? chunk->get_column_by_id(_column_id): chunk->get_column_by_slot_id(_column_id);
    uint16_t* target_sel = (dst_sel != nullptr) ? dst_sel: sel;
    if (_rf->num_hash_partitions() > 0) {
        hash_values.resize(column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, sel, sel_size, hash_values);
    }
    uint16_t ret = _rf->evaluate(column.get(), hash_values, sel, sel_size, target_sel);
    return ret;
}

Status DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    RETURN_IF_ERROR(prepare());

    // auto column = chunk->get_column_by_id(_column_id).get();

    auto column = chunk->is_cid_exist(_column_id) ? chunk->get_column_by_id(_column_id).get(): chunk->get_column_by_slot_id(_column_id).get();
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

StatusOr<uint16_t> DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size, uint16_t* dst_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    RETURN_IF_ERROR(prepare());
    uint16_t* target_sel = (dst_sel != nullptr) ? dst_sel: sel;
    // auto column = chunk->get_column_by_id(_column_id).get();
    auto column = chunk->is_cid_exist(_column_id) ? chunk->get_column_by_id(_column_id).get(): chunk->get_column_by_slot_id(_column_id).get();
    uint16_t new_size = 0;
    if (column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(column);
        const auto& data = GetContainer<TYPE_INT>::get_data(nullable_column->data_column());
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (uint16_t i = 0;i < sel_size;i++) {
                uint16_t idx = sel[i];
                target_sel[new_size] = idx;
                if (null_data[idx]) {
                    new_size += _rf->has_null();
                } else {
                    new_size += _result[data[idx]];
                }
            }
        } else {
            for (uint16_t i = 0;i < sel_size;i++) {
                uint16_t idx = sel[i];
                target_sel[new_size] = idx;
                new_size += _result[data[idx]];
            }
        }
    } else {
        const auto& data = GetContainer<TYPE_INT>::get_data(column);
        for (uint16_t i = 0;i < sel_size;i++) {
            uint16_t idx = sel[i];
            target_sel[new_size] = idx;
            new_size += _result[data[idx]];
        }
    }
    return new_size;
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
    std::sort(_sampling_ctxs.begin(), _sampling_ctxs.end(), [](const SamplingCtx& a, const SamplingCtx& b) {
        return a.filter_rows > b.filter_rows;
    });
    // for (size_t i = 0;i < _sampling_predicates.size();i ++) {
    for (size_t i = 0;i < _sampling_ctxs.size();i++) {
        // auto pred = _sampling_predicates[i];
        // double filter_ratio = _filter_rows[i] * 1.0 / _sample_rows;
        auto pred = _sampling_ctxs[i].pred;
        double filter_ratio = _sampling_ctxs[i].filter_rows * 1.0 / _sample_rows;

        // LOG(INFO) << "filter_id:" << pred->get_rf_desc()->filter_id() << ", filter_rows:" << _filter_rows[i]
        //     << ", sample_rows:" << _sample_rows << ", filter_ratio:" << filter_ratio << ", " << (void*)this;
        if (filter_ratio >= 0.95) {
            // use the best?
            // very good, only keep one
            _selectivity_map.clear();
            _selectivity_map.emplace(filter_ratio, pred);
            // LOG(INFO) << "very good filter, just use this one, filter_ratio: " << filter_ratio << ", filter_id: " << pred->get_rf_desc()->filter_id();
            break;
        }
        if (filter_ratio >= 0.5) {
            if (_selectivity_map.size() < 3) {
                _selectivity_map.emplace(filter_ratio, pred);
            } else {
                break;
            }
        }
    }
    // LOG(INFO) << "selectivity_map size: " << _selectivity_map.size() << ", " << (void*)this;
    // for (const auto& [ratio, pred]: _selectivity_map) {
    //     LOG(INFO) << "final used pred, filter_ratio: " << ratio << ", filter_id: " << pred->get_rf_desc()->filter_id() << ", " << (void*)this;
    // }
}

Status RuntimeFilterPredicates::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    if (_rf_predicates.empty()) {
        return Status::OK();
    }
    return _evaluate_selection(chunk, selection, from, to);
}

Status RuntimeFilterPredicates::_evaluate_selection(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    if (config::rf_skip_sample) {
        if (config::enable_rf_branchless) {
            _input_sel.clear();
            for (uint16_t i = from;i < to;i++) {
                if (selection[i]) {
                    _input_sel.emplace_back(i);
                }
            }
            uint16_t origin_count = _input_sel.size();
            uint16_t new_count = _input_sel.size();
            bool has_rf = false;
            for(auto pred: _rf_predicates) {
                if (pred->init(_driver_sequence)) {
                    ASSIGN_OR_RETURN(new_count, pred->evaluate(chunk, _input_sel.data(), new_count));
                    has_rf = true;
                }
            }
            if (has_rf) {
                _state = SAMPLE;
            }
            memset(selection + from, 0, to - from);
            for (uint16_t i = 0;i < new_count;i++) {
                selection[_input_sel[i]] = 1;
            }
        } else {
            for (auto pred: _rf_predicates) {
                if(pred->init(_driver_sequence)) {
                    RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
                }
            }
        }
        // not selection?

        return Status::OK();
    }
    switch (_state) {
        case INIT: {
            // if (_sampling_predicates.empty()) {
            if (_sampling_ctxs.empty()) {
                // std::ostringstream oss;
                for (auto pred: _rf_predicates) {
                    if (pred->init(_driver_sequence)) {
                        _sampling_ctxs.push_back(SamplingCtx{pred, 0});
                        // oss << pred->get_rf_desc()->filter_id() << ",";
                        // _sampling_predicates.push_back(pred);
                        // _filter_rows.push_back(0);
                    }
                }
                // @TODO wait all ?
                // if (!_sampling_predicates.empty()) {
                if (!_sampling_ctxs.empty()) {
                    _state = SAMPLE;
                    _sample_rows = 0;
                    _sample_times = 0;
                    // LOG(INFO) << "used predicates size: " << _sampling_predicates.size() << ", go to SAMPLE, " << (void*)this;
                    // LOG(INFO) << "used predicates[" << oss.str() << "], go to SAMPLE, " << (void*)this;
                } else {
                    // LOG(INFO) << "rf count: " << _rf_predicates.size() << ", no arrived, return... " << (void*)this;
                    // no filter can be used, just return
                    return Status::OK();
                }
            } else {
                break;
            }
            
        }
        case SAMPLE: {
            // @TODO use branchless mode
            if (config::enable_rf_branchless) {
                _input_sel.clear();
                for (uint16_t i = from;i < to;i++) {
                    if (selection[i]) {
                        _input_sel.push_back(i);
                    }
                }
                if (_input_sel.empty()) {
                    return Status::OK();
                }
                uint16_t old_count = _input_sel.size();
                uint16_t new_count = old_count;
                if (_sampling_ctxs.size() == 1) {
                    auto pred = _sampling_ctxs[0].pred;
                    auto& filter_rows = _sampling_ctxs[0].filter_rows;
                    ASSIGN_OR_RETURN(new_count, pred->evaluate(chunk, _input_sel.data(), _input_sel.size(), _input_sel.data()));
                    memset(selection + from, 0, to - from);
                    for (uint16_t i = 0;i < new_count;i++) {
                        selection[_input_sel[i]] = 1;
                    }
                    filter_rows += old_count - new_count;
                } else {
                    // _hit_count.assign(_input_sel.size(), 0);
                    _hit_count.resize(chunk->num_rows());
                    memset(_hit_count.data() + from, 0, to - from);
                    // for (uint16_t i = from;i < to;i++) {
                    //     _hit_count[i] = 0;
                    // }
                    _tmp_sel.resize(_input_sel.size());
                    for (size_t i = 0;i < _sampling_ctxs.size();i ++) {
                        auto pred = _sampling_ctxs[i].pred;
                        auto& filter_rows = _sampling_ctxs[i].filter_rows;
                        ASSIGN_OR_RETURN(new_count, pred->evaluate(chunk, _input_sel.data(), _input_sel.size(), _tmp_sel.data()));
                        for (uint16_t j = 0;j < new_count;j++) {
                            _hit_count[_tmp_sel[j]]++;
                        }
                        filter_rows += old_count - new_count;
                        // LOG(INFO) << "sample, old_count: " << old_count << ", new_count: " << new_count << ", rf:" << pred->get_rf_desc()->filter_id() << ", " << (void*)this;
                    }
                    // memset(selection + from, 0, to - from);
                    size_t target_num = _sampling_ctxs.size();
                    for (uint16_t i = from;i < to;i++) {
                        selection[i] = (_hit_count[i] == target_num);
                    }
                    new_count = SIMD::count_nonzero(selection + from, to - from);
                    // LOG(INFO) << "sample rows, old_count: " << old_count << ", new_count: " << new_count << ", " << (void*)this;
                }
                _sample_rows += old_count;
                // _hit_rows += new_count;
                _sample_times++;

            } else {
                size_t old_count = SIMD::count_nonzero(selection + from, to - from);
                // if (_sampling_predicates.size() == 1) {
                if (_sampling_ctxs.size() == 1) {
                    // no need merge, just evaluate
                    // auto pred = _sampling_predicates[0];
                    // auto& filter_rows = _filter_rows[0];
                    auto pred = _sampling_ctxs[0].pred;
                    auto& filter_rows = _sampling_ctxs[0].filter_rows;
                    RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
                    size_t new_count = SIMD::count_nonzero(selection + from, to - from);
                    filter_rows += old_count - new_count;
                } else {
                    // selection keep the original input
                    _input_selection.resize(chunk->num_rows());
                    _tmp_selection.resize(chunk->num_rows());
                    strings::memcpy_inlined(_input_selection.data() + from, selection + from, to - from);

                    // merged selection keep final result, selection keep tmp result
                    // for (size_t i = 0;i < _sampling_predicates.size();i ++) {
                    for (size_t i = 0;i < _sampling_ctxs.size();i ++) {
                        // auto pred = _sampling_predicates[i];
                        // auto& filter_rows = _filter_rows[i];
                        auto pred = _sampling_ctxs[i].pred;
                        auto& filter_rows = _sampling_ctxs[i].filter_rows;
                        strings::memcpy_inlined(_tmp_selection.data() + from, _input_selection.data() + from, to - from);
                        RETURN_IF_ERROR(pred->evaluate(chunk, _tmp_selection.data(), from, to));
                        size_t new_count = SIMD::count_nonzero(_tmp_selection.data() + from, to - from);
                        filter_rows += old_count - new_count;
                        for (uint16_t j = from;j < to;j++) {
                            selection[j] = selection[j] & _tmp_selection[j];
                        }
                    }
                }
                _sample_rows += old_count;
                _sample_times++;
            }
            if (_sample_rows >= config::rf_sample_rows) {
                // should decide selectivity map
                _update_selectivity_map();
                _state = NORMAL;
                _skip_rows = 0;
                // LOG(INFO) << "sample rows: " << _sample_rows << ", hit rows: " << _hit_rows << ", times:" << _sample_times << ", go to NORMAL, " << (void*)this;
            }
            return Status::OK();
        }
        case NORMAL: {
            if (config::enable_rf_branchless) {
                _input_sel.clear();
                for (uint16_t i = from;i < to;i++) {
                    if (selection[i]) {
                        _input_sel.emplace_back(i);
                    }
                }
                if (_input_sel.empty()) {
                    return Status::OK();
                }
                uint16_t origin_count = _input_sel.size();
                uint16_t new_count = _input_sel.size();
                for (auto& [_, pred]: _selectivity_map) {
                    ASSIGN_OR_RETURN(new_count, pred->evaluate(chunk, _input_sel.data(), new_count));
                }
                memset(selection + from, 0, to - from);
                for (uint16_t i = 0;i < new_count;i++) {
                    selection[_input_sel[i]] = 1;
                }
                _skip_rows += origin_count;
            } else {
                // @TODO use branch less mode?
                size_t origin_count = SIMD::count_nonzero(selection + from, to - from);
                for (auto& [_, pred]: _selectivity_map) {
                    RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
                    auto remain_count = SIMD::count_nonzero(selection + from, to - from);
                    // LOG(INFO) << "normal eval , input: " << origin_count << ", output: " << remain_count << ", filter_id: " << pred->get_rf_desc()->filter_id()
                    //     << ", " << (void*)this;
                    if (remain_count == 0) {
                        break;
                    }
                }
                _skip_rows += origin_count;
            }
            if (_skip_rows >= config::rf_skip_rows) {
                _state = INIT;
                // _sampling_predicates.clear();
                _sampling_ctxs.clear();
                // _filter_rows.clear();
                _sample_rows = 0;
                _sample_times = 0;
                // _hit_rows = 0;
                // LOG(INFO) << "skip rows: " << _skip_rows << ", go back to INIT";
            }
            // LOG(INFO) << "normal input: " << origin_count << ", output: " << SIMD::count_nonzero(selection + from, to - from)
                // << ", " << (void*)this;
            // size_t new_count = SIMD::count_nonzero(selection, to - from);
            // LOG(INFO) << "NORMAL mode, input: " << origin_count << ", output: " << new_count;
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
        // LOG(INFO) << "rewrite runtime filter predicate, column id: " << column_id << ", rf_desc: " << rf_desc->debug_string();
        // @TODO how about global_dict
    }
    return Status::OK();
}

Status RuntimeFilterPredicatesRewriter::rewrite(ObjectPool* obj_pool,
    RuntimeFilterPredicatesPtr& preds, const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators, const Schema& schema) {
    if (preds == nullptr) {
        return Status::OK();
    }
    auto& predicates = preds->_rf_predicates;
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
        // LOG(INFO) << "rewrite runtime filter predicate, column id: " << column_id << ", rf_desc: " << rf_desc->debug_string();
        // @TODO how about global_dict
    }
    return Status::OK();
}
}