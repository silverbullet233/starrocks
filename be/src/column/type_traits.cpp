#include "column/type_traits.h"
#include "column/binary_column.h"

namespace starrocks {


// Implementation of BinaryDataProxyContainer methods
template <typename T>
Slice BinaryDataProxyContainer<T>::operator[](size_t index) const {
    return _column.get_slice(index);
}

template <typename T>
size_t BinaryDataProxyContainer<T>::size() const {
    return _column.size();
}

// Explicit template instantiations
template struct BinaryDataProxyContainer<uint32_t>;  // For BinaryColumn
template struct BinaryDataProxyContainer<uint64_t>;  // For LargeBinaryColumn

} // namespace starrocks
