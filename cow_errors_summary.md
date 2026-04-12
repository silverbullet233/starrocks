
# COW.H 改动影响分析报告

## 📊 总体统计

- **总错误数**: 625 个
- **受影响文件**: 177 个
- **受影响模块**: 11 个

## 🔍 错误类型分布

### 1. const对象调用非const方法 (const_this_non_const_function)
- **数量**: 481 个 (77.0%)
- **原因**: ImmutPtr 返回 const Column*，但代码尝试调用非 const 成员函数
- **示例**: 
  ```
  'this' argument to member function 'append_nulls' has type 'const starrocks::Column', but function is not marked const
  位置: exprs/agg/group_concat.h:311
  ```

### 2. const初始化非const变量 (cannot_init_const)
- **数量**: 130 个 (20.8%)
- **原因**: 尝试用 const Column* 初始化或传递给需要 Column* 的参数
- **示例**:
  ```
  cannot initialize a parameter of type 'Column *' with an rvalue of type 'const starrocks::Column *'
  位置: exec/dict_decode_node.cpp:156
  ```

### 3. 赋值丢失const限定符 (discards_qualifiers)
- **数量**: 14 个 (2.2%)
- **原因**: 从 const Column* 赋值给 Column* 时丢失 const
- **示例**:
  ```
  assigning to 'Column *' from 'const starrocks::Column *' discards qualifiers
  位置: storage/persistent_index.cpp:3506
  ```

## 📦 受影响模块排名

1. **column**: 147 个错误 (23.5%)
2. **exec**: 140 个错误 (22.4%)
3. **exprs**: 133 个错误 (21.3%)
4. **storage**: 83 个错误 (13.3%)
5. **formats**: 79 个错误 (12.6%)
6. **serde**: 13 个错误 (2.1%)
7. **runtime**: 11 个错误 (1.8%)
8. **udf**: 6 个错误 (1.0%)
9. **util**: 5 个错误 (0.8%)
10. **connector**: 4 个错误 (0.6%)


## 📁 受影响最严重的文件 (Top 10)

1. `formats/parquet/column_converter.cpp`: 18 个错误
2. `formats/parquet/scalar_column_reader.cpp`: 17 个错误
3. `column/array_column.cpp`: 15 个错误
4. `exprs/math_functions.cpp`: 15 个错误
5. `column/nullable_column.h`: 15 个错误
6. `formats/orc/column_reader.cpp`: 15 个错误
7. `column/adaptive_nullable_column.h`: 15 个错误
8. `column/map_column.cpp`: 13 个错误
9. `column/struct_column.cpp`: 13 个错误
10. `storage/convert_helper.cpp`: 12 个错误


## 🔧 受影响最严重的函数 (Top 10)

1. `Status::OK`: 42 次
2. `constexpr`: 27 次
3. `Status::InternalError`: 17 次
4. `Status::RuntimeError`: 13 次
5. `resize`: 10 次
6. `max_serialized_size`: 9 次
7. `serialize_to_column`: 7 次
8. `reserve`: 7 次
9. `convert_materialized`: 7 次
10. `TypeDescriptor::from_logical_type`: 7 次


## 💡 修复建议

根据错误类型，需要采取不同的修复策略：

### 1. 对于 const_this_non_const_function 错误
- 需要在调用非const方法前获取可变引用
- 使用 `try_mutate()` 触发 COW 获取可变副本（如果对象可能被共享）
- 使用 `as_mutable_ptr()` 或 `as_mutable_raw_ptr()` （如果确定对象未被共享）

### 2. 对于 cannot_init_const 错误  
- 修改函数签名接受 const Column* 参数
- 或在传递前使用 `try_mutate()` 获取可变副本

### 3. 对于 discards_qualifiers 错误
- 不能直接赋值，需要使用 COW 机制
- 使用 `try_mutate()` 或 `as_mutable_ptr()`

## 📄 生成的文件

所有错误信息已保存到以下文件：

1. **cow_all_related_errors.json** - 所有625个错误的完整JSON数据
2. **cow_errors_categorized.json** - 按类别分类的错误
3. **cow_errors_detailed_report.json** - 详细统计报告
4. **cow_errors_final_report.json** - 最终汇总报告（包含示例）
5. **cow_errors_summary.md** - 本报告（Markdown格式）

---
生成时间: 2025-10-08
