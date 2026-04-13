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

#include <cstddef>

#include "base/string/slice.h"

namespace starrocks {

class Column;
template <typename T>
class BinaryColumnBase;
class GermanStringColumn;

// A uniform Slice-view over any byte-string column:
//   - BinaryColumn (TYPE_VARCHAR / TYPE_CHAR / TYPE_VARBINARY)
//   - LargeBinaryColumn
//   - GermanStringColumn (TYPE_STRING_V2)
//
// This is a **compute/storage boundary** abstraction: at the runtime-filter /
// bloom-filter / minmax-filter boundary we need byte-level access to string data
// regardless of which concrete column type the upstream operator produced.
// Inside the compute layer proper, operators should use the native container
// type for their LogicalType (GermanStringImmContainer for STRING_V2, etc.).
class StringSliceView {
public:
    StringSliceView() = default;

    template <typename T>
    explicit StringSliceView(const BinaryColumnBase<T>& column) {
        _init_from_binary(column);
    }

    // Wrap a GermanStringColumn so downstream byte-level code (bloom filters,
    // min/max range checks, group_concat sizing, ...) can read Slice views
    // without allocating a converted BinaryColumn.
    explicit StringSliceView(const GermanStringColumn& column);

    Slice operator[](size_t index) const;

    size_t size() const;

    size_t immutable_bytes_size() const;

private:
    template <typename T>
    void _init_from_binary(const BinaryColumnBase<T>& column);

    const Column* _column = nullptr;
    bool _is_large = false;
    bool _is_german = false;
};

} // namespace starrocks
