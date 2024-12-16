#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include "common/status.h"
#include "gutil/strings/fastmem.h"
#include "gutil/port.h"
#include "util/memcmp.h"

namespace starrocks {
class Slice;

class StringView {
public:
    static const size_t kInlineBytes = 12;
    static const size_t kPrefixBytes = 4;

    StringView(): StringView("") {}
    StringView(const char* data): StringView(data, strlen(data)) {}
    StringView(const char* data, uint32_t length) {
        value.inlined.length = length;
        if (is_inlined()) {
            memset(value.inlined.data, 0, kInlineBytes);
            if (length == 0) {
                return;
            }
            strings::memcpy_inlined(value.inlined.data, data, length);
        } else {
            strings::memcpy_inlined(value.pointer.prefix, data, kPrefixBytes);
            value.pointer.data = (char*) data;
        }
    }
    StringView(const uint8_t* data, size_t length): StringView(reinterpret_cast<const char*>(data), length) {} 
    StringView(const Slice& slice);
    StringView(const std::string& data): StringView(data.data(), data.size()) {}

    inline bool is_inlined() const {
        return value.inlined.length <= kInlineBytes;
    }

    inline const char* get_prefix() const {
        return value.pointer.prefix;
    }
    inline const char* get_data() const {
        return is_inlined() ? value.inlined.data : value.pointer.data;
    }
    uint32_t get_size() const {
        return value.inlined.length;
    }
    bool empty() const {
        return value.inlined.length == 0; 
    }
    std::string to_string() const {
        return is_inlined() ? std::string(value.inlined.data, value.inlined.length): std::string(value.pointer.data, value.pointer.length);
    }

    StringView tolower(std::string& buf) {
        buf.assign(get_data(), get_size());
        std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c);});
        return StringView(buf.data(), buf.size());
    }

    operator std::string_view() const { return {get_data(), get_size()}; }

    struct Comparator {
        bool operator()(const StringView& lhs, const StringView& rhs) const {
            return StringView::greater_than(rhs, lhs);
        }
    };


    static inline bool equals(const StringView& lhs, const StringView& rhs) {
        uint64_t left_comp = UNALIGNED_LOAD64(reinterpret_cast<const uint8_t*>(&lhs));
        uint64_t right_comp = UNALIGNED_LOAD64(reinterpret_cast<const uint8_t*>(&rhs));
        if (left_comp != right_comp) {
            return false;
        }
        left_comp = UNALIGNED_LOAD64(reinterpret_cast<const uint8_t*>(&lhs) + 8u);
        right_comp = UNALIGNED_LOAD64(reinterpret_cast<const uint8_t*>(&rhs) + 8u);
        if (left_comp == right_comp) {
            return true;
        }
        if (!lhs.is_inlined()) {
            if (strings::memeq(lhs.value.pointer.data, rhs.value.pointer.data, lhs.get_size())) {
                return true;
            }
        }
        return false;
    }

    static inline bool greater_than(const StringView& lhs, const StringView& rhs) {
        const uint32_t left_len = lhs.get_size();
        const uint32_t right_len = rhs.get_size();
        const uint32_t min_len = std::min(left_len, right_len);

        uint32_t left_prefix = UNALIGNED_LOAD32(reinterpret_cast<const uint8_t*>(lhs.value.pointer.prefix));
        uint32_t right_prefix = UNALIGNED_LOAD32(reinterpret_cast<const uint8_t*>(rhs.value.pointer.prefix));

        auto byte_swap = [](uint32_t v) -> uint32_t {
            uint32_t t1 = (v >> 16u) | (v << 16u);
            uint32_t t2 = t1 & 0x00ff00ff;
            uint32_t t3 = t1 & 0xff00ff00;
            return (t2 << 8u) | (t3 >> 8u);
        };
        if (left_prefix != right_prefix) {
            return byte_swap(left_prefix) > byte_swap(right_prefix);
        }
        int ret = memcmp(lhs.get_data(), rhs.get_data(), min_len);
        return ret > 0 || (ret == 0 && left_len > right_len);
    }

    int compare(const StringView& rhs) const {
        return StringView::compare(*this, rhs);
    }

    static inline int compare(const StringView& lhs, const StringView& rhs) {
        // @TODO pending fix
        if (equals(lhs, rhs)) {
            return 0;
        }
        return greater_than(lhs, rhs) ? 1: -1;
    }

private:
    union {
        struct {
            uint32_t length;
            char prefix[4];
            char* data;
        } pointer;
        struct {
            uint32_t length;
            char data[12];
        } inlined;
    } value;
};

inline std::ostream& operator<<(std::ostream &os, const StringView& sv) {
    os << sv.to_string();
    return os;
}

inline bool operator==(const StringView& x, const StringView& y) {
    return StringView::equals(x, y);
}

inline bool operator!=(const StringView& x, const StringView& y) {
    return !(x == y);
}
// @TODO compare

inline bool operator<(const StringView& x, const StringView& y) {
    return StringView::greater_than(y, x);
}

inline bool operator<=(const StringView& x, const StringView& y) {
    // !(y > x) <==> (x <= y)
    // x <= y <=> x < y or x == y
    return !StringView::greater_than(x, y);
}

inline bool operator>(const StringView& x, const StringView& y) {
    return StringView::greater_than(x, y);
}

inline bool operator>=(const StringView& x, const StringView& y) {
    return !StringView::greater_than(y, x);
}
// how to hash



class StringViewEqual {
public:
    bool operator()(const StringView& x, const StringView& y) const {
        return x == y;
    }
};


}

namespace std {
inline std::string to_string(const starrocks::StringView& value) {
    return value.to_string();
}
}