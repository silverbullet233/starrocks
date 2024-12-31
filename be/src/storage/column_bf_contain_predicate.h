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

#include <cstring>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/statusor.h"
#include "exprs/runtime_filter.h"
#include "gutil/casts.h"
#include "storage/column_predicate.h"
#include "exprs/runtime_filter_bank.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class ColumnBloomFilterContainPredicate final : public ColumnPredicate {
public:
    enum State: uint8_t {
        WAIT_RF = 0,
        SAMPLE = 1,
        DISABLE = 2,
        ENABLE = 3,
    };
    explicit ColumnBloomFilterContainPredicate(const TypeInfoPtr& type_info, ColumnId id,
        const RuntimeFilterProbeDescriptor* rf_desc, int32_t driver_sequence):
        ColumnPredicate(type_info, id), _rf_desc(rf_desc), _driver_sequence(driver_sequence) {
        }
    
    std::vector<uint16_t> build_sel(uint8_t* selection, uint16_t from, uint16_t to) const {
        std::vector<uint16_t> sel;
        sel.reserve(to - from);
        for (uint16_t i = from;i < to;i++) {
            if (selection[i]) {
                sel.emplace_back(i);
            }
        }
        return sel;
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (UNLIKELY(from == to)) {
            return Status::OK();
        }
        if (config::enable_rf_pushdown) {
            return Status::OK();
        }
        // @TODO convert to evalute_branchless?
        std::vector<uint16_t> sel = build_sel(selection, from, to);
        ASSIGN_OR_RETURN(uint16_t ret, evaluate_branchless(column, sel.data(), sel.size()));
        // reset selection
        memset(selection + from, 0, (to - from) * sizeof(uint8_t));
        for (uint16_t i = 0;i < ret;i++) {
            uint16_t idx = sel[i];
            selection[idx] = 1;
        }
        return Status::OK();
        // return Status::NotSupported("BloomFilterContains::evaluate");
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (UNLIKELY(from == to)) {
            return Status::OK();
        }
        if (config::enable_rf_pushdown) {
            return Status::OK();
        }
        std::vector<uint16_t> sel = build_sel(selection, from, to);
        ASSIGN_OR_RETURN(uint16_t ret, evaluate_branchless(column, sel.data(), sel.size()));
        std::vector<uint8_t> new_selection(to - from, 0);
        for (uint16_t i = 0;i < ret;i++) {
            uint16_t idx = sel[i];
            new_selection[idx] = 1;
        }
        for (uint16_t i = from;i < to;i++) {
            selection[i] &= new_selection[i];
        }
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        if (UNLIKELY(from == to)) {
            return Status::OK();
        }
        if (config::enable_rf_pushdown) {
            return Status::OK();
        }
        std::vector<uint16_t> sel = build_sel(selection, from, to);
        ASSIGN_OR_RETURN(uint16_t ret, evaluate_branchless(column, sel.data(), sel.size()));
        std::vector<uint8_t> new_selection(to - from, 0);
        for (uint16_t i = 0;i < ret;i++) {
            uint16_t idx = sel[i];
            new_selection[idx] = 1;
        }
        for (uint16_t i = from;i < to;i++) {
            selection[i] |= new_selection[i];
        }
        return Status::OK();
    }

    StatusOr<uint16_t> _evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const {
        if (config::enable_rf_pushdown) {
            return sel_size;
        }
        DCHECK(_rf) << "_rf should not be null";
        if (_is_dict_code_column) {
            RETURN_IF_ERROR(precompute_for_dict_code());
            uint16_t new_size = 0;
            if (column->is_nullable()) {
                const auto* nullable_column = down_cast<const NullableColumn*>(column);
                // dict code column must be int column
                const auto& data = GetContainer<TYPE_INT>::get_data(nullable_column->data_column());
                if (nullable_column->has_null()) {
                    const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                    for(int i = 0;i < sel_size;i++) {
                        uint16_t idx = sel[i];
                        sel[new_size] = idx;
                        if (null_data[idx]) {
                            new_size += _rf->has_null();
                        } else {
                            new_size += _dict_words_result[data[idx]];
                        }
                    }
                } else {
                    for (int i = 0;i < sel_size;i++){
                        uint16_t idx = sel[i];
                        sel[new_size] = idx;
                        new_size += _dict_words_result[data[idx]];
                    }
                }
            } else {
                const auto& data = GetContainer<TYPE_INT>::get_data(column);
                for (int i = 0;i < sel_size;i++){
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    new_size += _dict_words_result[data[idx]];
                }
            }
            // LOG(INFO) << "BloomFilterContains::evaluate_branchless dict input size:" << sel_size << ", ret:" << new_size << ", always_true:" << rf->always_true()
            //     << ", filter_id: " <<  _rf_desc->filter_id();

            // @TODO update selectivit

            return new_size;
        } else {
            // std::vector<uint32_t> hash_values;
            if (_rf->num_hash_partitions() > 0) {
                // compute hash 
                // @TODO compute hash may calculate multi times
                // hash_values.resize(column->size());
                hash_values.resize(sel_size);
                _rf->compute_partition_index(_rf_desc->layout(), {column}, sel, sel_size, hash_values);
            }
    
            uint16_t ret = _rf->evaluate(column, hash_values, sel, sel_size);
            // LOG(INFO) << "BloomFilterContains::evaluate_branchless input size:" << sel_size << ", ret:" << ret << ", column_size:" << column->size()
            //     << ", filter_id: " <<  _rf_desc->filter_id();
            return ret;
        }
        // @TODO if selectivity is very poor, should give it up
        return sel_size;
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        switch (_state) {
            case WAIT_RF: {
                if (auto rf = try_to_get_rf(); rf != nullptr) {
                    _state = SAMPLE;
                    // LOG(INFO) << "WAIT_RF to SAMPLE, rf: " << _rf_desc->debug_string();
                } else {
                    return sel_size;
                }
            }
            case SAMPLE: {
                ASSIGN_OR_RETURN(uint16_t ret, _evaluate_branchless(column, sel, sel_size));
                _input_rows += sel_size;
                _filtered_rows += sel_size - ret;
                if (_input_rows >= kSampleRows) {
                    double filter_ratio = _filtered_rows * 1.0 / _input_rows;
                    if (filter_ratio < 0.5) {
                        // selectivity is poor, should skip
                        _state = DISABLE;
                    } else {
                        // selectivity is good, should keep it
                        _state = ENABLE;
                    }
                    // LOG(INFO) << "SAMPLE done, input rows: " << _input_rows << ", filtered rows: " << _filtered_rows
                    //     << ", ratio: " << filter_ratio << ", go to: " << (_state == DISABLE ? "DISABLE": "ENABLE")
                    //         << ", rf: " << _rf_desc->debug_string() << ", " << (void*)this;
                    _skiped_rows = 0;
                }
                return ret;
            }
            case DISABLE: {
                _skiped_rows += sel_size;
                if (UNLIKELY(_skiped_rows >= kSkipRowsThreshold)) {
                    _state = SAMPLE;
                    _input_rows = 0;
                    _filtered_rows = 0;
                    // LOG(INFO) << "DISABLE done, go back to SAMPLE, skiped rows: " << _skiped_rows << ", rf: "<<  _rf_desc->debug_string();
                }
                return sel_size;
            }
            case ENABLE: {
                ASSIGN_OR_RETURN(uint16_t ret, _evaluate_branchless(column, sel, sel_size));
                _skiped_rows += sel_size;
                if (UNLIKELY(_skiped_rows >= kSkipRowsThreshold)) {
                    _state = SAMPLE;
                    _input_rows = 0;
                    _filtered_rows = 0;
                //     LOG(INFO) << "ENABLE done, go back to SAMPLE, skiped rows: " << _skiped_rows << ", rf:" << _rf_desc->debug_string();
                }
                return ret;
            }
            default:
                CHECK(false) << "unreachable path";
                break;
        }
        // if (auto rf = try_to_get_rf()) {
        //     ASSIGN_OR_RETURN(uint16_t ret, _evaluate_branchless(column, sel, sel_size));
        //     return ret;
        // } 
        // return sel_size;
    }

    bool can_vectorized() const override {
        return false;
    }

    PredicateType type() const override {
        return PredicateType::kBloomFilterContain;
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info, ObjectPool* obj_pool) const override {
        return Status::NotSupported("not implemented");
    }
    std::string debug_string() const override {
        std::stringstream ss;
        ss << "bloom_filter_contain" << ColumnPredicate::debug_string() << ", " << _rf_desc->debug_string();
        return ss.str();
    }

    // @TODO for dict code column, we should change rf into in?
    // for each dict word, check if it is in rf, build a result[code] array to store the result
    Status precompute_for_dict_code() const;

    void set_dict_words(std::vector<std::string> dict_words) {
        // LOG(INFO) << "set_dict_word, pred: " << debug_string() << ", dict size: " << dict_words.size();
        _is_dict_code_column = true;
        _dict_words = std::move(dict_words);
    }

private:
    const JoinRuntimeFilter* try_to_get_rf() const {
        if (_rf) {
            return _rf;
        }
        _rf = _rf_desc->runtime_filter(_driver_sequence);
        return _rf;
    }

    const RuntimeFilterProbeDescriptor* _rf_desc;
    int32_t _driver_sequence;
    mutable const JoinRuntimeFilter* _rf = nullptr;
    // for tupn filter
    mutable size_t _rf_version = 0;
    // @TODO for dict code column, skip temporary, will support it later
    bool _is_dict_code_column = false;
    std::vector<std::string> _dict_words;
    mutable std::vector<uint8_t> _dict_words_result;
    // @TODO should consider filter ratio, if selectivy is very poor, just skip it
    // @TODO calculate filter ratio in 4K rows, if rati

    // @TODO count selectivity each 4K rows, if selectivity is poor, skip use rf in next 128k rows, then try it again

    mutable State _state = State::WAIT_RF;
    // total skiped rows since disable this filter
    mutable size_t _skiped_rows = 0;
    // total input rows in a sample period
    mutable size_t _input_rows = 0;
    // total fitlerd rows in a sample period
    mutable size_t _filtered_rows = 0;
    // @TODO for reused
    mutable std::vector<uint32_t> hash_values;

    static const size_t kSampleRows = 4 * 1024;
    static const size_t kSkipRowsThreshold = 128 * 1024;

};
}