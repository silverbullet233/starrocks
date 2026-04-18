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

#include "column/column_helper.h"

#include "column/column_builder.h"
#include "column/german_string_column.h"
#include "gtest/gtest.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class ColumnHelperTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    MutableColumnPtr create_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(false);
    }

    MutableColumnPtr create_nullable_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"), true);
        return builder.build(false);
    }

    MutableColumnPtr create_const_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(true);
    }

    MutableColumnPtr create_only_null_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append_null();
        return builder.build(true);
    }
};

TEST_F(ColumnHelperTest, cast_to_nullable_column) {
    auto col = ColumnHelper::cast_to_nullable_column(create_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());

    col = ColumnHelper::cast_to_nullable_column(create_nullable_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());
}

TEST_F(ColumnHelperTest, align_return_type) {
    auto nullable_column = create_nullable_column();
    auto not_null_column = create_column();
    auto col_size = not_null_column->size();
    nullable_column->append(*ColumnHelper::align_return_type(
            std::move(not_null_column), TypeDescriptor::from_logical_type(TYPE_VARCHAR), col_size, true));
    ASSERT_TRUE(nullable_column->is_nullable());
    ASSERT_FALSE(nullable_column->is_constant());
}

TEST_F(ColumnHelperTest, get_data_column_by_type) {
    auto column = create_nullable_column();
    const auto* data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_const_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());
}

TEST_F(ColumnHelperTest, get_null_column) {
    auto column = create_nullable_column();
    const auto* null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");

    column = create_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_TRUE(null_column == nullptr);

    column = create_const_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");
}

// GetContainer::get_data
TEST_F(ColumnHelperTest, get_container_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    auto data = GetContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST_F(ColumnHelperTest, get_container_varchar) {
    auto col = ColumnTestHelper::build_column<Slice>({"hello", "world"});
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "hello");
    EXPECT_EQ(data[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, get_container_large_binary) {
    auto col = LargeBinaryColumn::create();
    col->append_string("abc");
    col->append_string("def");
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "abc");
    EXPECT_EQ(data[1].to_string(), "def");
}

// GetContainer::get_data(column, row)
TEST_F(ColumnHelperTest, get_container_get_data_with_row_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_const_column) {
    // ConstColumn: is_constant() == true, index is always 0
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto const_col = ConstColumn::create(std::move(inner), 5);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 0), 42);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 3), 42);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_nullable) {
    // NullableColumn: get_data_column unwraps it, row index used as-is
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_data_column_from_const_nullable_column) {
    auto data_column = BinaryColumn::create();
    data_column->append(Slice("v1"));
    auto null_column = NullColumn::create();
    null_column->append(0);
    ColumnPtr nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
    ColumnPtr const_nullable_column = ConstColumn::create(nullable_column, 4);

    const Column* const_data = ColumnHelper::get_data_column(const_nullable_column.get());
    ASSERT_FALSE(const_data->is_nullable());
    ASSERT_FALSE(const_data->is_constant());
    ASSERT_TRUE(const_data->is_binary());

    const auto* typed_data = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(const_nullable_column.get());
    ASSERT_TRUE(typed_data->is_binary());
    ASSERT_EQ(typed_data->get_slice(0).to_string(), "v1");
}

// ColumnHelper::append_column_value
TEST_F(ColumnHelperTest, append_column_value_int32) {
    auto col = Int32Column::create();
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 42);
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 99);
    EXPECT_EQ(col->debug_string(), "[42, 99]");
}

TEST_F(ColumnHelperTest, append_column_value_varchar) {
    auto col = BinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("hello"));
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("world"));
    EXPECT_EQ(col->debug_string(), "['hello', 'world']");
}

TEST_F(ColumnHelperTest, append_column_value_large_binary) {
    auto col = LargeBinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("foo"));
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("bar"));
    EXPECT_EQ(col->debug_string(), "['foo', 'bar']");
}

// ColumnHelper::build_slices
TEST_F(ColumnHelperTest, build_slices_binary_column) {
    auto col = ColumnTestHelper::build_column<Slice>({"foo", "bar", "baz"});

    Buffer<Slice> slices;
    ColumnHelper::build_slices(col.get(), slices);
    ASSERT_EQ(slices.size(), 3);
    EXPECT_EQ(slices[0].to_string(), "foo");
    EXPECT_EQ(slices[1].to_string(), "bar");
    EXPECT_EQ(slices[2].to_string(), "baz");
}

TEST_F(ColumnHelperTest, build_slices_large_binary_column) {
    auto col = LargeBinaryColumn::create();
    col->append_string("hello");
    col->append_string("world");

    Buffer<Slice> slices;
    ColumnHelper::build_slices(col.get(), slices);
    ASSERT_EQ(slices.size(), 2);
    EXPECT_EQ(slices[0].to_string(), "hello");
    EXPECT_EQ(slices[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, build_slices_nullable_column) {
    auto nullable = ColumnTestHelper::build_nullable_column<Slice>({"a", "bb"});

    Buffer<Slice> slices;
    ColumnHelper::build_slices(nullable.get(), slices);
    ASSERT_EQ(slices.size(), 2);
    EXPECT_EQ(slices[0].to_string(), "a");
    EXPECT_EQ(slices[1].to_string(), "bb");
}

TEST_F(ColumnHelperTest, build_slices_const_column) {
    auto inner = BinaryColumn::create();
    inner->append(Slice("const"));
    ColumnPtr const_col = ConstColumn::create(std::move(inner), 3);

    Buffer<Slice> slices;
    ColumnHelper::build_slices(const_col.get(), slices);
    ASSERT_EQ(slices.size(), 1);
    EXPECT_EQ(slices[0].to_string(), "const");
}

// GetStorageContainer::get_data
TEST_F(ColumnHelperTest, get_storage_container_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    auto data = GetStorageContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST_F(ColumnHelperTest, get_storage_container_nullable) {
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    auto data = GetStorageContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 10);
    EXPECT_EQ(data[1], 20);
    EXPECT_EQ(data[2], 30);
}

TEST_F(ColumnHelperTest, get_storage_container_const_column) {
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto const_col = ConstColumn::create(std::move(inner), 3);
    auto data = GetStorageContainer<TYPE_INT>::get_data(const_col.get());
    ASSERT_EQ(data.size(), 1);
    EXPECT_EQ(data[0], 42);
}

TEST_F(ColumnHelperTest, get_storage_container_varchar) {
    auto col = ColumnTestHelper::build_column<Slice>({"hello", "world"});
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "hello");
    EXPECT_EQ(data[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, get_storage_container_large_binary) {
    auto col = LargeBinaryColumn::create();
    col->append_string("abc");
    col->append_string("def");
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "abc");
    EXPECT_EQ(data[1].to_string(), "def");
}

TEST_F(ColumnHelperTest, get_storage_container_nullable_varchar) {
    auto col = ColumnTestHelper::build_nullable_column<Slice>({"foo", "bar"});
    auto data = GetStorageContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "foo");
    EXPECT_EQ(data[1].to_string(), "bar");
}

TEST_F(ColumnHelperTest, get_storage_container_mutable_column_ptr) {
    auto col = ColumnTestHelper::build_column<int32_t>({5, 6, 7});
    MutableColumnPtr mutable_col = std::move(col);
    auto data = GetStorageContainer<TYPE_INT>::get_data(mutable_col);
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 5);
    EXPECT_EQ(data[1], 6);
    EXPECT_EQ(data[2], 7);
}

// GetStorageContainer::get_data(column, row)
TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_const_column) {
    auto inner = ColumnTestHelper::build_column<int32_t>({99});
    auto const_col = ConstColumn::create(std::move(inner), 5);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(const_col.get(), 0), 99);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(const_col.get(), 4), 99);
}

TEST_F(ColumnHelperTest, get_storage_container_get_data_with_row_nullable) {
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetStorageContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

// convert_german_string_to_binary_column

TEST_F(ColumnHelperTest, convert_german_string_to_binary_column_basic) {
    auto gs = GermanStringColumn::create();
    gs->append(Slice("tiny"));                                                    // inline, len=4
    gs->append(Slice(""));                                                        // empty inline
    gs->append(Slice("abcdefghijkl"));                                            // inline, len=12 (boundary)
    gs->append(Slice("this-is-a-long-string-forced-out-of-line-representation")); // long (> 12)

    ColumnPtr src = std::move(gs);
    ColumnPtr converted = ColumnHelper::convert_german_string_to_binary_column(src);

    ASSERT_NE(converted.get(), src.get()) << "GermanStringColumn must be materialized into a fresh BinaryColumn";
    ASSERT_TRUE(converted->is_binary());
    ASSERT_FALSE(converted->is_nullable());
    const auto* bin = down_cast<const BinaryColumn*>(converted.get());
    ASSERT_EQ(bin->size(), 4);
    EXPECT_EQ(bin->get_slice(0).to_string(), "tiny");
    EXPECT_EQ(bin->get_slice(1).to_string(), "");
    EXPECT_EQ(bin->get_slice(2).to_string(), "abcdefghijkl");
    EXPECT_EQ(bin->get_slice(3).to_string(), "this-is-a-long-string-forced-out-of-line-representation");
}

TEST_F(ColumnHelperTest, convert_german_string_to_binary_column_nullable) {
    auto gs = GermanStringColumn::create();
    gs->append(Slice("row0"));
    gs->append(Slice("row1-long-payload-goes-here"));
    gs->append(Slice("row2"));

    auto null = NullColumn::create();
    null->append(0);
    null->append(1); // null row
    null->append(0);

    ColumnPtr src = NullableColumn::create(std::move(gs), std::move(null));
    ColumnPtr converted = ColumnHelper::convert_german_string_to_binary_column(src);

    ASSERT_NE(converted.get(), src.get());
    ASSERT_TRUE(converted->is_nullable());
    const auto* nullable = down_cast<const NullableColumn*>(converted.get());
    const auto* bin = down_cast<const BinaryColumn*>(nullable->data_column().get());
    ASSERT_EQ(bin->size(), 3);
    EXPECT_EQ(bin->get_slice(0).to_string(), "row0");
    EXPECT_EQ(bin->get_slice(2).to_string(), "row2");
    EXPECT_TRUE(nullable->is_null(1));
    EXPECT_FALSE(nullable->is_null(0));
    EXPECT_FALSE(nullable->is_null(2));
}

TEST_F(ColumnHelperTest, convert_german_string_to_binary_column_passthrough) {
    // BinaryColumn input: returned alias, no conversion performed.
    auto bin_src = BinaryColumn::create();
    bin_src->append_string("hello");
    bin_src->append_string("world");
    ColumnPtr src = std::move(bin_src);
    ColumnPtr converted = ColumnHelper::convert_german_string_to_binary_column(src);
    EXPECT_EQ(converted.get(), src.get());

    // Non-string column: returned alias.
    auto int_src = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    ColumnPtr int_ptr = std::move(int_src);
    ColumnPtr int_conv = ColumnHelper::convert_german_string_to_binary_column(int_ptr);
    EXPECT_EQ(int_conv.get(), int_ptr.get());

    // Nullable(BinaryColumn): returned alias.
    auto bin_data = BinaryColumn::create();
    bin_data->append_string("abc");
    auto null_col = NullColumn::create();
    null_col->append(0);
    ColumnPtr nullable_bin = NullableColumn::create(std::move(bin_data), std::move(null_col));
    ColumnPtr nullable_conv = ColumnHelper::convert_german_string_to_binary_column(nullable_bin);
    EXPECT_EQ(nullable_conv.get(), nullable_bin.get());
}

TEST_F(ColumnHelperTest, convert_german_string_to_binary_column_owns_bytes) {
    // Ensure converted bytes are independent of the source arena.
    auto gs = GermanStringColumn::create();
    std::string long_str(128, 'z'); // out-of-line
    gs->append(Slice(long_str));

    ColumnPtr src = std::move(gs);
    ColumnPtr converted = ColumnHelper::convert_german_string_to_binary_column(src);
    src.reset(); // drop the source column and its arena

    const auto* bin = down_cast<const BinaryColumn*>(converted.get());
    ASSERT_EQ(bin->size(), 1);
    EXPECT_EQ(bin->get_slice(0).to_string(), long_str);
}

} // namespace starrocks
