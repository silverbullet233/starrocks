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

#include "column/string_slice_view.h"

#include <glog/logging.h>

#include <type_traits>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/german_string_column.h"
#include "gutil/casts.h"

namespace starrocks {

StringSliceView::StringSliceView(const GermanStringColumn& column) {
    _column = &column;
    _is_large = false;
    _is_german = true;
}

template <typename T>
void StringSliceView::_init_from_binary(const BinaryColumnBase<T>& column) {
    _column = &column;
    _is_large = std::is_same_v<T, uint64_t>;
    _is_german = false;
}

// Explicit instantiations so the header-friend constructor works from headers.
template void StringSliceView::_init_from_binary<uint32_t>(const BinaryColumnBase<uint32_t>&);
template void StringSliceView::_init_from_binary<uint64_t>(const BinaryColumnBase<uint64_t>&);

Slice StringSliceView::operator[](size_t index) const {
    DCHECK(_column != nullptr);
    if (_is_german) {
        return down_cast<const GermanStringColumn*>(_column)->get_slice(index);
    }
    if (_is_large) {
        return down_cast<const LargeBinaryColumn*>(_column)->get_slice(index);
    }
    return down_cast<const BinaryColumn*>(_column)->get_slice(index);
}

size_t StringSliceView::size() const {
    return _column == nullptr ? 0 : _column->size();
}

size_t StringSliceView::immutable_bytes_size() const {
    if (_column == nullptr) {
        return 0;
    }
    if (_is_german) {
        // GermanStringColumn does not store a contiguous byte buffer; report the
        // sum of live string lengths so callers that size buffers (e.g. group_concat)
        // still see the right footprint.
        const auto* gs = down_cast<const GermanStringColumn*>(_column);
        size_t total = 0;
        for (size_t i = 0, n = gs->size(); i < n; ++i) {
            total += gs->get_slice(i).size;
        }
        return total;
    }
    if (_is_large) {
        return down_cast<const LargeBinaryColumn*>(_column)->get_immutable_bytes().size();
    }
    return down_cast<const BinaryColumn*>(_column)->get_immutable_bytes().size();
}

} // namespace starrocks
