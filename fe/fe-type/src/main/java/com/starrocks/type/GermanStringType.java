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

package com.starrocks.type;

/**
 * Query-path-only string type backed by BE's GermanStringColumn. Users cannot
 * declare this type in DDL; the FE rewrites VARCHAR slots/exprs to GERMAN_STRING
 * at Thrift serialization time when {@code enable_german_string} is set.
 * Metadata mirrors VARCHAR (same length semantics, same MySQL wire type).
 */
public class GermanStringType extends ScalarType {
    public static final GermanStringType GERMAN_STRING = new GermanStringType(-1);

    public GermanStringType(int len) {
        super(PrimitiveType.GERMAN_STRING);
        setLength(len);
    }
}
