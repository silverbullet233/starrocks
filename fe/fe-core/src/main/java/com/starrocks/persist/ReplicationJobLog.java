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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.replication.ReplicationJob;

import java.io.DataInput;
import java.io.IOException;

public class ReplicationJobLog implements Writable {
    @SerializedName(value = "replicationJob")
    private final ReplicationJob replicationJob;

    public ReplicationJobLog(ReplicationJob replicationJob) {
        this.replicationJob = replicationJob;
    }

    public ReplicationJob getReplicationJob() {
        return replicationJob;
    }



    public static ReplicationJobLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplicationJobLog.class);
    }
}
