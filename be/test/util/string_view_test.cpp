#include <gtest/gtest.h>

#include "util/string_view.h"

namespace starrocks {

TEST(StringViewTest, test_all) {
    // inline & inline
    {
        StringView a = "12345";
        StringView b = "123456";
        ASSERT_FALSE(a == b);
        ASSERT_TRUE(a != b);
        ASSERT_TRUE(a < b);
        ASSERT_TRUE(a <= b);
        ASSERT_FALSE(a > b);
        ASSERT_FALSE(a >= b);
        ASSERT_TRUE(a.is_inlined());
        ASSERT_TRUE(b.is_inlined());
        a = "1234567";
        b = "1234567";
        ASSERT_TRUE(a == b);
    }
    {
        StringView a = "12345";
        StringView b = "1234567890123456";
        ASSERT_TRUE(a.is_inlined());
        ASSERT_FALSE(b.is_inlined());
        ASSERT_FALSE(a == b);
        ASSERT_TRUE(a != b);
        ASSERT_TRUE(a < b);
        ASSERT_TRUE(a <= b);
        ASSERT_FALSE(a > b);
        ASSERT_FALSE(a >= b);
    }
    {
        StringView a = "12345678901234567890";
        StringView b = "12345678911234567890";
        ASSERT_FALSE(a.is_inlined());
        ASSERT_FALSE(b.is_inlined());
        ASSERT_FALSE(a == b);
        ASSERT_TRUE(a != b);
        ASSERT_TRUE(a < b);
        ASSERT_TRUE(a <= b);
        ASSERT_FALSE(a > b);
        ASSERT_FALSE(a >= b);
        a = "12345678901234567890";
        b = "12345678901234567890";
        ASSERT_TRUE(a == b);
    }
}
}