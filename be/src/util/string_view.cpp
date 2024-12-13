#include "util/string_view.h"

#include "util/slice.h"
namespace starrocks {

StringView::StringView(const Slice& slice): StringView(slice.data, slice.size) {}
}