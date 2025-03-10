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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class GroupProviderLog  extends GenericNameWithPropsPersistInfo {
    @SerializedName(value = "t")
    private String type;

    @SerializedName(value = "c")
    private String comment;

    public GroupProviderLog(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }
}
