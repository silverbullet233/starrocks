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
#include <cstring>

#include "column/column.h"
#include "column/nullable_column.h"
#include "exprs/runtime_filter.h"
#include "gutil/casts.h"
#include "storage/column_predicate.h"
#include "exprs/runtime_filter_bank.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class ColumnBloomFilterContainPredicate final : public ColumnPredicate {
public:
    explicit ColumnBloomFilterContainPredicate(const TypeInfoPtr& type_info, ColumnId id,
        const RuntimeFilterProbeDescriptor* rf_desc, int32_t driver_sequence):
        ColumnPredicate(type_info, id), _rf_desc(rf_desc), _driver_sequence(driver_sequence) {
        }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        LOG(INFO) << "BloomFilterContains::evaluate";
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        LOG(INFO) << "BloomFilterContains::evaluate_and";
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        LOG(INFO) << "BloomFilterContains::evaluate_or";
        return Status::OK();
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        // @TODO if dict column, should convert dict code column to dict word
        if (auto rf = try_to_get_rf()) {
            std::vector<uint32_t> hash_values;
            if (rf->num_hash_partitions() > 0) {
                // conpute hash
                hash_values.resize(column->size());
                rf->compute_partition_index(_rf_desc->layout(), {column}, sel, sel_size, hash_values);
            }
    
            uint16_t ret = rf->evaluate(column, hash_values, sel, sel_size);
            // LOG(INFO) << "BloomFilterContains::evaluate_branchless input size:" << sel_size << ", ret:" << ret << ", always_true:" << rf->always_true()
            //     << ", filter_id: " <<  _rf_desc->filter_id();
            return ret;
        }
        // @TODO check if rf received
        return sel_size;
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
        ss << "bloom_filter_contain" << ColumnPredicate::debug_string();
        return ss.str();
    }

    Status compuate_dict_hash(const std::vector<Slice>& dict_words)  {
        // @TODO we need a lazy way to compute dict word hash
        return Status::OK();
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
    // @TODO for dict code column, skip temporary, will support it later
};

ColumnPredicate* new_column_bf_contains_predicate(const TypeInfoPtr& type_info, ColumnId id, const RuntimeFilterProbeDescriptor* rf_desc, int32_t driver_sequence) {
    return new ColumnBloomFilterContainPredicate(type_info, id, rf_desc, driver_sequence);
}
}