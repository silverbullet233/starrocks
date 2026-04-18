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

#include "column/german_string.h"
#include "column/runtime_type_traits.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

struct FixedLengthTypeGetter {
    template <LogicalType ltype>
    size_t operator()() {
        return RunTimeFixedTypeLength<ltype>::value;
    }
};

size_t get_size_of_fixed_length_type(LogicalType ltype) {
    // TYPE_GERMAN_STRING is intentionally excluded from `type_dispatch_all` to
    // avoid compiling the polymorphic functors (agg/hash/serde) against the
    // GermanString Cpp type. For fixed-length size we already know the answer
    // here: a GermanString row is a 16-byte value (inline payload or
    // prefix+ptr), so callers estimating row bytes need the same
    // `sizeof(GermanString)`.
    if (ltype == TYPE_GERMAN_STRING) {
        return sizeof(GermanString);
    }
    return type_dispatch_all(ltype, FixedLengthTypeGetter());
}

} // namespace starrocks
