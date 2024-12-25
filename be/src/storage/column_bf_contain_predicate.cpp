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
#include "storage/column_bf_contain_predicate.h"
#include "column/vectorized_fwd.h"
#include "exprs/runtime_filter.h"

namespace starrocks {

Status ColumnBloomFilterContainPredicate::precompute_for_dict_code() const {
    DCHECK(_is_dict_code_column);
    DCHECK(_rf != nullptr);
    if (!_dict_words_result.empty()) {
        return Status::OK();
    }
    // @TODO should build a binary column here?
    auto binary_column = BinaryColumn::create();
    std::vector<Slice> data_slice;
    for (const auto& word : _dict_words) {
        data_slice.emplace_back(word.data(), word.size());
    }
    binary_column->append_strings(data_slice.data(), data_slice.size());
    // std::vector<uint32_t> hash_values;
    // Filter selection(_dict_words.size(), 1);

    JoinRuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    ctx.selection.assign(_dict_words.size(), 1);

    // @TODO need selection?
    if (_rf->num_hash_partitions() > 0) {
        // compute hash
        ctx.hash_values.resize(binary_column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {binary_column.get()}, &ctx);
    }
    _rf->evaluate(binary_column.get(), &ctx);

    _dict_words_result.resize(_dict_words.size());
    for (size_t i = 0; i < _dict_words.size(); ++i) {
        // LOG(INFO) << "dict code: " << i << ", word: " << _dict_words[i] << ", selection: " << (int)ctx.selection[i];
        _dict_words_result[i] = ctx.selection[i];
    }

    return Status::OK();
}


ColumnPredicate* new_column_bf_contains_predicate(const TypeInfoPtr& type_info, ColumnId id, const RuntimeFilterProbeDescriptor* rf_desc, int32_t driver_sequence) {
    return new ColumnBloomFilterContainPredicate(type_info, id, rf_desc, driver_sequence);
}
}