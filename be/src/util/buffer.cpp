#include "util/buffer.h"

namespace starrocks::util {

// Definition of empty_raw_buffer
alignas(std::max_align_t) uint8_t empty_raw_buffer[empty_raw_buffer_size] = {0};

} // namespace starrocks
