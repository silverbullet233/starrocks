# COW Column 重构 - 修改文件列表

## 📊 总体统计
- **修改文件总数**: 90 个文件
- **代码变更**: 576 行插入, 619 行删除
- **Clangd 错误**: 从 2810 降至 1451 (减少 48%)
- **Column 相关错误**: 从 427 降至 226 (减少 47%)

---

## 🔧 核心 Column 接口重构 (7个文件)

### 1. NullableColumn
- `be/src/column/nullable_column.h` - 统一 data_column 和 null_column 访问接口

### 2. AdaptiveNullableColumn  
- `be/src/column/adaptive_nullable_column.h` - 继承 NullableColumn 的新接口

### 3. ConstColumn
- `be/src/column/const_column.h` - 统一 data_column 访问接口

### 4. ArrayColumn
- `be/src/column/array_column.h` - 统一 elements 和 offsets 访问接口

### 5. MapColumn
- `be/src/column/map_column.h` - 统一 keys, values, offsets 访问接口

### 6. StructColumn
- `be/src/column/struct_column.h` - 统一 field 访问接口
- `be/src/column/struct_column.cpp` - 实现新的批量访问方法

### 7. ArrayViewColumn
- `be/src/column/array_view_column.h` - 统一 elements, offsets, lengths 访问接口
- `be/src/column/array_view_column.cpp` - 相关实现更新

---

## 🛠️ 核心工具类 (2个文件)

### ColumnHelper
- `be/src/column/column_helper.h` - 更新所有调用到新的统一 API
- `be/src/column/column_helper.cpp` - 实现更新

---

## 📁 调用方文件 (81个文件)

### 存储层 (Storage Layer)
- `be/src/storage/rowset/array_column_iterator.cpp`
- `be/src/storage/rowset/map_column_iterator.cpp`
- `be/src/storage/rowset/struct_column_iterator.cpp`
- `be/src/storage/rowset/dictcode_column_iterator.cpp`
- `be/src/storage/rowset/column_decoder.cpp`
- `be/src/storage/rowset/json_column_compactor.cpp`
- `be/src/storage/rowset/json_column_iterator.cpp`
- `be/src/storage/rowset/parsed_page.cpp`
- `be/src/storage/column_aggregate_func.cpp`
- `be/src/storage/column_aggregator.h`
- `be/src/storage/convert_helper.cpp`
- `be/src/storage/meta_reader.cpp`

### 表达式层 (Expression Layer)
- `be/src/exprs/array_functions.cpp`
- `be/src/exprs/array_map_expr.cpp`
- `be/src/exprs/map_element_expr.cpp`
- `be/src/exprs/map_functions.cpp`
- `be/src/exprs/struct_functions.cpp`
- `be/src/exprs/dictionary_get_expr.cpp`
- `be/src/exprs/math_functions.cpp`
- `be/src/exprs/runtime_filter_bank.cpp`
- `be/src/exprs/binary_function.h`

### 聚合函数 (Aggregate Functions)
- `be/src/exprs/agg/approx_top_k.h`
- `be/src/exprs/agg/array_agg.h`
- `be/src/exprs/agg/boolor.h`
- `be/src/exprs/agg/combinator/agg_state_combine.h`
- `be/src/exprs/agg/combinator/agg_state_if.h`
- `be/src/exprs/agg/distinct.h`
- `be/src/exprs/agg/group_concat.h`
- `be/src/exprs/agg/java_udaf_function.h`
- `be/src/exprs/agg/map_agg.h`
- `be/src/exprs/agg/maxmin_by.h`
- `be/src/exprs/agg/nullable_aggregate.h`
- `be/src/exprs/agg/retention.h`
- `be/src/exprs/agg/window.h`
- `be/src/exprs/agg/window_funnel.h`

### 执行引擎 (Execution Engine)
- `be/src/exec/aggregate/agg_hash_map.h`
- `be/src/exec/aggregate/agg_hash_set.h`
- `be/src/exec/aggregate/compress_serializer.cpp`
- `be/src/exec/aggregator.cpp`
- `be/src/exec/arrow_to_starrocks_converter.cpp`
- `be/src/exec/es/es_scroll_parser.cpp`
- `be/src/exec/file_scanner/parquet_scanner.cpp`
- `be/src/exec/hash_join_node.cpp`
- `be/src/exec/hash_joiner.cpp`
- `be/src/exec/hash_joiner.h`
- `be/src/exec/hdfs_scanner/hdfs_scanner_orc.cpp`
- `be/src/exec/hdfs_scanner/jni_scanner.cpp`
- `be/src/exec/schema_scanner/schema_helper.h`
- `be/src/exec/sorted_streaming_aggregator.cpp`
- `be/src/exec/sorting/sort_permute.cpp`
- `be/src/exec/tablet_info.cpp`
- `be/src/exec/tablet_sink.cpp`

### 格式转换层 (Format Conversion)
- `be/src/formats/avro/cpp/complex_column_reader.cpp`
- `be/src/formats/avro/cpp/nullable_column_reader.cpp`
- `be/src/formats/avro/nullable_column.cpp`
- `be/src/formats/csv/array_converter.cpp`
- `be/src/formats/csv/map_converter.cpp`
- `be/src/formats/csv/nullable_converter.cpp`
- `be/src/formats/json/map_column.cpp`
- `be/src/formats/json/nullable_column.cpp`
- `be/src/formats/json/struct_column.cpp`
- `be/src/formats/orc/column_reader.cpp`
- `be/src/formats/orc/orc_min_max_decoder.cpp`
- `be/src/formats/parquet/column_converter.cpp`
- `be/src/formats/parquet/complex_column_reader.cpp`
- `be/src/formats/parquet/encoding.cpp`
- `be/src/formats/parquet/encoding_bss.h`
- `be/src/formats/parquet/encoding_delta.h`
- `be/src/formats/parquet/encoding_dict.h`
- `be/src/formats/parquet/encoding_plain.h`

### 其他模块
- `be/src/bench/parquet_dict_decode_bench.cpp`
- `be/src/connector/mysql_connector.cpp`
- `be/src/runtime/global_dict/decoder.cpp`
- `be/src/serde/column_array_serde.cpp`
- `be/src/udf/java/java_data_converter.cpp`
- `be/src/udf/udf_call_stub.cpp`
- `be/src/util/arrow/starrocks_column_to_arrow.cpp`
- `be/src/util/json_flattener.cpp`
- `build.sh`

---

## 📚 Review 文档 (2个文件)

### 详细文档
- `COW_REFACTOR_CHANGES_SUMMARY.md` - 详细改动摘要
- `COW_REFACTOR_REVIEW_GUIDE.md` - Review 指南

---

## 🎯 建议的 Review 顺序

### 1. 核心接口 (必须 Review)
1. `be/src/column/nullable_column.h` - 查看新的统一 API 设计
2. `be/src/column/array_column.h` - 查看 elements/offsets 访问接口
3. `be/src/column/map_column.h` - 查看 keys/values/offsets 访问接口
4. `be/src/column/struct_column.h` - 查看 field 访问接口

### 2. 工具类 (必须 Review)
5. `be/src/column/column_helper.h` - 查看调用更新
6. `be/src/column/column_helper.cpp` - 查看实现更新

### 3. 典型调用方 (建议 Review)
7. `be/src/exprs/array_functions.cpp` - 表达式层调用示例
8. `be/src/storage/rowset/array_column_iterator.cpp` - 存储层调用示例
9. `be/src/formats/parquet/complex_column_reader.cpp` - 格式转换层调用示例

### 4. Review 文档
10. `COW_REFACTOR_REVIEW_GUIDE.md` - 完整的 Review 指南

---

## 🔍 如何查看具体改动

### 使用 Git 命令
```bash
# 查看核心 Column 头文件改动
git diff be/src/column/nullable_column.h
git diff be/src/column/array_column.h
git diff be/src/column/map_column.h
git diff be/src/column/struct_column.h

# 查看关键调用方改动
git diff be/src/column/column_helper.h
git diff be/src/exprs/array_functions.cpp
git diff be/src/storage/rowset/array_column_iterator.cpp

# 查看所有修改的文件
git status --short
```

### 使用 Cursor
所有修改的文件现在都应该显示在 Cursor 的 "Chat Files" 区域中，您可以：
1. 点击文件名查看具体改动
2. 使用 "Keep All" 应用所有改动
3. 使用 "Undo All" 撤销所有改动
4. 选择性应用部分改动

---

## ⚠️ 当前状态

### ✅ 已完成
- 7个 Column 头文件接口重构
- 1个 Column 实现文件 (struct_column.cpp)
- column_helper.h/cpp 核心工具类更新
- 批量修复 83 个调用文件的旧 API

### 🔄 进行中
- **226 个 column 相关错误**需要继续修复
- 主要剩余问题:
  - 13个 `offsets()` 引用访问
  - 6个 `elements()` 引用访问  
  - 6个 `fields()` 引用访问
  - 一些复杂的 down_cast 和方法调用问题

### 📋 下一步
1. Review 当前改动
2. 继续修复剩余的 226 个错误
3. 运行 `./clangd_check.sh` 验证
4. 运行 `./build.sh --be` 编译验证
