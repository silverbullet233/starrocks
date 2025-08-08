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

#include <memory>

#include "cache/cache_options.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/CloudConfiguration_types.h"

namespace starrocks::pipeline {

// global late materialization context
class GlobalLateMaterilizationContext {
public:
    virtual ~GlobalLateMaterilizationContext() = default;
};

class IcebergGlobalLateMaterilizationContext : public GlobalLateMaterilizationContext {
public:
    THdfsScanRange hdfs_scan_range;
    THdfsScanNode hdfs_scan_node;
    TPlanNode plan_node;

    // DataCacheOptions datacache_options;
    // int tuple_id;
    // TCloudConfiguration cloud_configuration;

};

// manage all global late materialization contexts for different data sources
class GlobalLateMaterilizationContextMgr {
public:
    // @TODO
    void add_ctx(int tuple_id, GlobalLateMaterilizationContext* ctx);
    GlobalLateMaterilizationContext* get_ctx(int tuple_id) const;
    // @TODO tuple_id -> GlobalLateMaterilizationContext*
    // @TODO mock
    GlobalLateMaterilizationContext* _ctx;
    // @TODO slot id -> glm ctx?
};
}