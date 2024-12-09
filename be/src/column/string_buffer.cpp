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

#include "column/string_buffer.h"
#include "gutil/strings/fastmem.h"
#include "runtime/memory/mem_hook_allocator.h"
#include "glog/logging.h"
#include "util/string_view.h"

namespace starrocks {

StringView StringBuffer::add_string(const char* data, size_t len) {
    DCHECK(len > StringView::kInlineBytes) << "string length should be larger than " << StringView::kInlineBytes;
    char* ptr = reinterpret_cast<char*>(_pool.allocate(len));
    strings::memcpy_inlined(ptr, data, len);
    return StringView{ptr, (uint32_t) len};
}

size_t StringBuffer::allocated_bytes() const {
    return _pool.total_reserved_bytes();
}
}

