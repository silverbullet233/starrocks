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

#include "exprs/literal.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gutil/port.h"
#include "gutil/strings/fastmem.h"
#include "runtime/decimalv3.h"
#include "runtime/memory/allocator_v2.h"
#include "types/constexpr.h"
#include "util/string_parser.hpp"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/ir_helper.h"
#endif

namespace starrocks {

#define CASE_TYPE_COLUMN(NODE_TYPE, CHECK_TYPE, LITERAL_VALUE)                              \
    case NODE_TYPE: {                                                                       \
        DCHECK_EQ(node.node_type, TExprNodeType::CHECK_TYPE);                               \
        DCHECK(node.__isset.LITERAL_VALUE);                                                 \
        datum.set(node.LITERAL_VALUE.value);                                                \
        break;                                                                              \
    }

template <LogicalType LT>
static RunTimeCppType<LT> unpack_decimal(const std::string& s) {
    static_assert(lt_is_decimal<LT>);
    RunTimeCppType<LT> value;
#ifdef IS_LITTLE_ENDIAN
    strings::memcpy_inlined(&value, &s.front(), sizeof(value));
#else
    std::copy(s.rbegin(), s.rend(), (char*)&value);
#endif
    return value;
}

template <LogicalType DecimalType, typename = DecimalLTGuard<DecimalType>>
static Datum decimal_datum_from_literal(const TExprNode& node, int precision, int scale) {
    using CppType = RunTimeCppType<DecimalType>;
    CppType value;
    DCHECK(node.__isset.decimal_literal);
    // using TDecimalLiteral::integer_value take precedence over using TDecimalLiteral::value
    if (node.decimal_literal.__isset.integer_value) {
        const std::string& s = node.decimal_literal.integer_value;
        value = unpack_decimal<DecimalType>(s);
        return Datum(value);
    }
    auto& literal_value = node.decimal_literal.value;
    auto fail =
            DecimalV3Cast::from_string<CppType>(&value, precision, scale, literal_value.c_str(), literal_value.size());
    if (fail) {
        return kNullDatum;
    }
    return Datum(value);
}

static Datum build_literal_datum(const TExprNode& node, const TypeDescriptor& type, std::string* string_value) {
    Datum datum;
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return datum;
    }

    switch (type.type) {
        CASE_TYPE_COLUMN(TYPE_BOOLEAN, BOOL_LITERAL, bool_literal)
        CASE_TYPE_COLUMN(TYPE_TINYINT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_SMALLINT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_INT, INT_LITERAL, int_literal);
        CASE_TYPE_COLUMN(TYPE_BIGINT, INT_LITERAL, int_literal);
    case TYPE_FLOAT: {
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        datum.set_float(static_cast<float>(node.float_literal.value));
        break;
    }
    case TYPE_DOUBLE: {
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        datum.set_double(node.float_literal.value);
        break;
    }
    case TYPE_LARGEINT: {
        DCHECK_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);

        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto data = StringParser::string_to_int<__int128>(node.large_int_literal.value.c_str(),
                                                          node.large_int_literal.value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            data = MAX_INT128;
        }
        datum.set_int128(data);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        // @IMPORTANT: build slice though get_data, else maybe will cause multi-thread crash in scanner
        *string_value = node.string_literal.value;
        datum.set_slice(Slice(*string_value));
        break;
    }
    case TYPE_TIME: {
        datum.set_double(node.float_literal.value);
        break;
    }
    case TYPE_DATE: {
        DateValue v;
        if (v.from_string(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            datum.set_date(v);
        } else {
            datum.set_null();
        }
        break;
    }
    case TYPE_DATETIME: {
        TimestampValue v;
        if (v.from_string(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            datum.set_timestamp(v);
        } else {
            datum.set_null();
        }
        break;
    }
    case TYPE_DECIMALV2: {
        datum.set_decimal(DecimalV2Value(node.decimal_literal.value));
        break;
    }
    case TYPE_DECIMAL32: {
        datum = decimal_datum_from_literal<TYPE_DECIMAL32>(node, type.precision, type.scale);
        break;
    }
    case TYPE_DECIMAL64: {
        datum = decimal_datum_from_literal<TYPE_DECIMAL64>(node, type.precision, type.scale);
        break;
    }
    case TYPE_DECIMAL128: {
        datum = decimal_datum_from_literal<TYPE_DECIMAL128>(node, type.precision, type.scale);
        break;
    }
    case TYPE_DECIMAL256: {
        datum = decimal_datum_from_literal<TYPE_DECIMAL256>(node, type.precision, type.scale);
        break;
    }
    case TYPE_VARBINARY: {
        // @IMPORTANT: build slice though get_data, else maybe will cause multi-thread crash in scanner
        *string_value = node.binary_literal.value;
        datum.set_slice(Slice(*string_value));
        break;
    }
    default:
        DCHECK(false) << "Vectorized engine not implement type: " << type.type;
        break;
    }

    return datum;
}

static ColumnPtr build_literal_value(const Datum& datum, const TypeDescriptor& type, memory::Allocator* allocator) {
    if (datum.is_null()) {
        return ColumnHelper::create_const_null_column(allocator, 1);
    }

    switch (type.type) {
    case TYPE_BOOLEAN:
        return ColumnHelper::create_const_column<TYPE_BOOLEAN>(allocator, datum.get<bool>(), 1);
    case TYPE_TINYINT:
        return ColumnHelper::create_const_column<TYPE_TINYINT>(allocator, datum.get_int8(), 1);
    case TYPE_SMALLINT:
        return ColumnHelper::create_const_column<TYPE_SMALLINT>(allocator, datum.get_int16(), 1);
    case TYPE_INT:
        return ColumnHelper::create_const_column<TYPE_INT>(allocator, datum.get_int32(), 1);
    case TYPE_BIGINT:
        return ColumnHelper::create_const_column<TYPE_BIGINT>(allocator, datum.get_int64(), 1);
    case TYPE_FLOAT:
        return ColumnHelper::create_const_column<TYPE_FLOAT>(allocator, datum.get_float(), 1);
    case TYPE_DOUBLE:
        return ColumnHelper::create_const_column<TYPE_DOUBLE>(allocator, datum.get_double(), 1);
    case TYPE_LARGEINT:
        return ColumnHelper::create_const_column<TYPE_LARGEINT>(allocator, datum.get_int128(), 1);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return ColumnHelper::create_const_column<TYPE_VARCHAR>(allocator, datum.get_slice(), 1);
    case TYPE_TIME:
        return ColumnHelper::create_const_column<TYPE_TIME>(allocator, datum.get_double(), 1);
    case TYPE_DATE:
        return ColumnHelper::create_const_column<TYPE_DATE>(allocator, datum.get_date(), 1);
    case TYPE_DATETIME:
        return ColumnHelper::create_const_column<TYPE_DATETIME>(allocator, datum.get_timestamp(), 1);
    case TYPE_DECIMALV2:
        return ColumnHelper::create_const_column<TYPE_DECIMALV2>(allocator, datum.get_decimal(), 1);
    case TYPE_DECIMAL32:
        return ColumnHelper::create_const_decimal_column<TYPE_DECIMAL32>(allocator, datum.get_int32(), type.precision,
                                                                         type.scale, 1);
    case TYPE_DECIMAL64:
        return ColumnHelper::create_const_decimal_column<TYPE_DECIMAL64>(allocator, datum.get_int64(), type.precision,
                                                                         type.scale, 1);
    case TYPE_DECIMAL128:
        return ColumnHelper::create_const_decimal_column<TYPE_DECIMAL128>(allocator, datum.get_int128(), type.precision,
                                                                          type.scale, 1);
    case TYPE_DECIMAL256:
        return ColumnHelper::create_const_decimal_column<TYPE_DECIMAL256>(allocator, datum.get_int256(), type.precision,
                                                                          type.scale, 1);
    case TYPE_VARBINARY:
        return ColumnHelper::create_const_column<TYPE_VARBINARY>(allocator, datum.get_slice(), 1);
    default:
        DCHECK(false) << "Vectorized engine not implement type: " << type.type;
        break;
    }

    return ColumnHelper::create_const_null_column(allocator, 1);
}

VectorizedLiteral::VectorizedLiteral(const TExprNode& node) : Expr(node) {
    _datum = build_literal_datum(node, _type, &_string_value);
}

VectorizedLiteral::VectorizedLiteral(const VectorizedLiteral& other)
        : Expr(other), _value(other._value), _datum(other._datum), _string_value(other._string_value) {
    if (!_datum.is_null() &&
        (_type.type == TYPE_CHAR || _type.type == TYPE_VARCHAR || _type.type == TYPE_VARBINARY)) {
        _datum.set_slice(Slice(_string_value));
    }
}

VectorizedLiteral::VectorizedLiteral(ColumnPtr&& value, const TypeDescriptor& type)
        : Expr(type, false), _value(std::move(value)) {
    DCHECK(_value->is_constant());
}

Status VectorizedLiteral::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_value == nullptr) {
        _value = build_literal_value(_datum, _type, context->get_allocator());
    }
    return Status::OK();
}

#undef CASE_TYPE_COLUMN

StatusOr<ColumnPtr> VectorizedLiteral::evaluate_checked(ExprContext* context, Chunk* ptr) {
    DCHECK(_value != nullptr);
    MutableColumnPtr column = _value->clone_empty();
    column->append(*_value, 0, 1);
    if (ptr != nullptr) {
        column->resize(ptr->num_rows());
    }
    return column;
}

#ifdef STARROCKS_JIT_ENABLE

bool VectorizedLiteral::is_compilable(RuntimeState* state) const {
    return IRHelper::support_jit(_type.type);
}

JitScore VectorizedLiteral::compute_jit_score(RuntimeState* state) const {
    return {0, 0};
}

std::string VectorizedLiteral::jit_func_name_impl(RuntimeState* state) const {
    return "{" + type().debug_string() + "[" + _value->debug_string() + "]}";
}

StatusOr<LLVMDatum> VectorizedLiteral::generate_ir_impl(ExprContext* context, JITContext* jit_ctx) {
    bool only_null = _value->only_null();
    LLVMDatum datum(jit_ctx->builder, only_null);
    if (only_null) {
        ASSIGN_OR_RETURN(datum.value, IRHelper::create_ir_number(jit_ctx->builder, _type.type, 0));
    } else {
        ASSIGN_OR_RETURN(datum.value, IRHelper::load_ir_number(jit_ctx->builder, _type.type, _value->raw_data()));
    }
    return datum;
}
#endif

std::string VectorizedLiteral::debug_string() const {
    std::stringstream out;
    if (_value == nullptr) {
        out << "VectorizedLiteral(type=" << this->type().debug_string() << ", value=uninitialized)";
        return out.str();
    }
    out << "VectorizedLiteral("
        << "type=" << this->type().debug_string() << ", value=" << _value->debug_string() << ")";
    return out.str();
}

VectorizedLiteral::~VectorizedLiteral() = default;

} // namespace starrocks
