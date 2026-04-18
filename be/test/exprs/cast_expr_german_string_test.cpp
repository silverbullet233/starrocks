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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/german_string.h"
#include "column/german_string_column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "types/logical_type.h"

namespace starrocks {

class CastExprGermanStringTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::CAST;
        expr_node.node_type = TExprNodeType::CAST_EXPR;
        expr_node.num_children = 1;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);
    }

protected:
    TExprNode expr_node;
};

// ---------- VARCHAR -> GERMAN_STRING ---------------------------------------

TEST_F(CastExprGermanStringTest, VarcharInlineToGermanString) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    const std::string s = "hi";
    MockVectorizedExpr<TYPE_VARCHAR> col(child_node, 5, Slice(s));
    expr->_children.push_back(&col);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    auto* gs = ColumnHelper::cast_to_raw<TYPE_GERMAN_STRING>(out);
    ASSERT_NE(nullptr, gs);
    ASSERT_EQ(5, gs->size());
    for (size_t i = 0; i < 5; ++i) {
        ASSERT_EQ(s, gs->get_slice(i).to_string());
    }
}

TEST_F(CastExprGermanStringTest, VarcharLongToGermanString) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    // Force non-inline (>12 bytes) so the arena copy path is exercised.
    const std::string s = "this is a long-ish german string payload";
    ASSERT_GT(s.size(), 12u);
    MockVectorizedExpr<TYPE_VARCHAR> col(child_node, 4, Slice(s));
    expr->_children.push_back(&col);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    auto* gs = ColumnHelper::cast_to_raw<TYPE_GERMAN_STRING>(out);
    ASSERT_NE(nullptr, gs);
    ASSERT_EQ(4, gs->size());
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_EQ(s, gs->get_slice(i).to_string());
    }
}

// ---------- GERMAN_STRING -> VARCHAR ---------------------------------------

TEST_F(CastExprGermanStringTest, GermanStringToVarcharRoundTrip) {
    expr_node.child_type = TPrimitiveType::GERMAN_STRING;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    // Build a GermanStringColumn with both inline and long entries.
    auto src = GermanStringColumn::create();
    src->append(Slice("short"));
    src->append(Slice("this is a long-ish german string payload"));
    src->append(Slice(""));

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);
    MockColumnExpr child(child_node, std::move(src));
    expr->_children.push_back(&child);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    ASSERT_TRUE(out->is_binary());
    auto binary = BinaryColumn::static_pointer_cast(out);
    ASSERT_EQ(3, binary->size());
    ASSERT_EQ("short", binary->get_slice(0).to_string());
    ASSERT_EQ("this is a long-ish german string payload", binary->get_slice(1).to_string());
    ASSERT_EQ("", binary->get_slice(2).to_string());
}

// ---------- INT <-> GERMAN_STRING ------------------------------------------

TEST_F(CastExprGermanStringTest, IntToGermanString) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::INT);
    MockVectorizedExpr<TYPE_INT> col(child_node, 3, 1234567);
    expr->_children.push_back(&col);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    auto* gs = ColumnHelper::cast_to_raw<TYPE_GERMAN_STRING>(out);
    ASSERT_NE(nullptr, gs);
    ASSERT_EQ(3, gs->size());
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_EQ("1234567", gs->get_slice(i).to_string());
    }
}

TEST_F(CastExprGermanStringTest, GermanStringToInt) {
    expr_node.child_type = TPrimitiveType::GERMAN_STRING;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    auto src = GermanStringColumn::create();
    src->append(Slice("42"));
    src->append(Slice("-7"));

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);
    MockColumnExpr child(child_node, std::move(src));
    expr->_children.push_back(&child);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    auto* int_col = ColumnHelper::cast_to_raw<TYPE_INT>(out);
    ASSERT_NE(nullptr, int_col);
    ASSERT_EQ(2, int_col->size());
    ASSERT_EQ(42, int_col->get_data()[0]);
    ASSERT_EQ(-7, int_col->get_data()[1]);
}

// ---------- GERMAN_STRING -> BOOLEAN ---------------------------------------

TEST_F(CastExprGermanStringTest, GermanStringToBoolean) {
    expr_node.child_type = TPrimitiveType::GERMAN_STRING;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    auto src = GermanStringColumn::create();
    src->append(Slice("true"));
    src->append(Slice("false"));
    src->append(Slice("1"));
    src->append(Slice("0"));

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);
    MockColumnExpr child(child_node, std::move(src));
    expr->_children.push_back(&child);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    const Column* data = ColumnHelper::get_data_column(out.get());
    const auto* bool_col = down_cast<const BooleanColumn*>(data);
    ASSERT_EQ(4, bool_col->size());
    EXPECT_TRUE(bool_col->get_data()[0]);
    EXPECT_FALSE(bool_col->get_data()[1]);
    EXPECT_TRUE(bool_col->get_data()[2]);
    EXPECT_FALSE(bool_col->get_data()[3]);
}

// ---------- Null propagation ----------------------------------------------

TEST_F(CastExprGermanStringTest, NullVarcharToGermanString) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    // MockNullVectorizedExpr marks row i as null if ((flag + i) % 2) != 0;
    // default flag == 0 so odd rows are null and even rows carry "abc".
    MockNullVectorizedExpr<TYPE_VARCHAR> col(child_node, 6, Slice("abc"));
    expr->_children.push_back(&col);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    ASSERT_TRUE(out->is_nullable());
    const auto* nullable = down_cast<const NullableColumn*>(out.get());
    const auto* gs = down_cast<const GermanStringColumn*>(nullable->data_column().get());
    ASSERT_EQ(6, gs->size());
    for (size_t i = 0; i < 6; ++i) {
        if (i % 2 == 1) {
            EXPECT_TRUE(out->is_null(i));
        } else {
            EXPECT_FALSE(out->is_null(i));
            EXPECT_EQ("abc", gs->get_slice(i).to_string());
        }
    }
}

TEST_F(CastExprGermanStringTest, OnlyNullGermanStringToInt) {
    expr_node.child_type = TPrimitiveType::GERMAN_STRING;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    ObjectPool pool;
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, expr_node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);

    TExprNode child_node = expr_node;
    child_node.type = gen_type_desc(TPrimitiveType::GERMAN_STRING);
    MockNullVectorizedExpr<TYPE_GERMAN_STRING> col(child_node, 4, GermanString(), /*only_null=*/true);
    expr->_children.push_back(&col);

    ColumnPtr out = expr->evaluate(nullptr, nullptr);
    ASSERT_NE(nullptr, out.get());
    ASSERT_EQ(4, out->size());
    for (size_t i = 0; i < out->size(); ++i) {
        EXPECT_TRUE(out->is_null(i));
    }
}

} // namespace starrocks
