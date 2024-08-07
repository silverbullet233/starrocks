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

package com.starrocks.catalog;

import java.util.Objects;

public class ComplexTypeAccessPath {
    private final ComplexTypeAccessPathType accessPathType;

    // Only set when ComplexTypeAccessType = STRUCT_SUBFIELD
    private final String structSubfieldName;

    public ComplexTypeAccessPath(ComplexTypeAccessPathType accessPathType, String structSubfieldName) {
        this.accessPathType = accessPathType;
        this.structSubfieldName = structSubfieldName;
    }

    public ComplexTypeAccessPath(ComplexTypeAccessPathType accessPathType) {
        this(accessPathType, null);
    }

    public ComplexTypeAccessPathType getAccessPathType() {
        return this.accessPathType;
    }

    public String getStructSubfieldName() {
        return this.structSubfieldName;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComplexTypeAccessPath)) {
            return false;
        }

        ComplexTypeAccessPath that = (ComplexTypeAccessPath) o;
        return accessPathType == that.accessPathType &&
                Objects.equals(structSubfieldName, that.structSubfieldName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(accessPathType);
        result = 31 * result + Objects.hashCode(structSubfieldName);
        return result;
    }
}
