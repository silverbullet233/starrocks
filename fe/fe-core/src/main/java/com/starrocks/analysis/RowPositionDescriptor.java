// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TRowPositionDescriptor;
import com.starrocks.thrift.TRowPositionType;

import java.util.List;

// describe how to find a row in specific table, only used by global late materialization
public class RowPositionDescriptor {
    public enum Type {
        ICEBERG_V3
    }
    private Type type;
    private SlotId sourceNodeSlot;
    private List<SlotId> refSlots;

    public RowPositionDescriptor(Type type, SlotId sourceNodeSlot, List<SlotId> refSlots) {
        Preconditions.checkState(refSlots != null && !refSlots.isEmpty(),
                "refSlot can't be null or empty");
        this.type = type;
        this.sourceNodeSlot = sourceNodeSlot;
        this.refSlots = refSlots;
    }

    public Type getType() {
        return type;
    }

    public SlotId getSourceNodeSlot() {
        return sourceNodeSlot;
    }

    public List<SlotId> getRefSlots() {
        return refSlots;
    }

    public TRowPositionDescriptor toThrift() {
        TRowPositionDescriptor msg = new TRowPositionDescriptor();
        switch (type) {
            case ICEBERG_V3:
                msg.setRow_position_type(TRowPositionType.ICEBERG_V3_ROW_POSITION);
                break;
            default:
                throw new RuntimeException("unknown type");
        }
        // msg.setExec_node_slot(execNodeSlot.asInt());
        refSlots.forEach(slotId -> {
            msg.addToRef_slots(slotId.asInt());
        });
        return msg;
    }
}
