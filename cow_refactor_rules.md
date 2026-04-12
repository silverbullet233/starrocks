# 🔧 Cursor Task Rules

## 🧭 项目目标
修复COW接口重构引入的编译错误，确保所有地方都以正确的方式使用COW

---

## ✅ 执行顺序（TODO）

按照下面的顺序逐个修复每个文件

1. /home/xujia/starrocks/be/src/exec/analytor.cpp
2. /home/xujia/starrocks/be/src/exec/chunks_sorter_heap_sort.cpp
3. /home/xujia/starrocks/be/src/exec/es/es_scroll_parser.cpp
4. /home/xujia/starrocks/be/src/exprs/map_functions.cpp
5. /home/xujia/starrocks/be/src/exprs/math_functions.cpp
6. /home/xujia/starrocks/be/src/exprs/time_functions.cpp
7. /home/xujia/starrocks/be/src/formats/avro/cpp/nullable_column_reader.cpp
8. /home/xujia/starrocks/be/src/formats/avro/nullable_column.cpp
9. /home/xujia/starrocks/be/src/formats/csv/map_converter.cpp
10. /home/xujia/starrocks/be/src/formats/csv/nullable_converter.cpp
11. /home/xujia/starrocks/be/src/formats/json/nullable_column.cpp
12. /home/xujia/starrocks/be/src/formats/json/struct_column.cpp
13. /home/xujia/starrocks/be/src/formats/parquet/column_converter.cpp
14. /home/xujia/starrocks/be/src/storage/column_in_predicate.cpp
15. /home/xujia/starrocks/be/src/storage/column_not_in_predicate.cpp
16. /home/xujia/starrocks/be/src/storage/column_operator_predicate.h
17. /home/xujia/starrocks/be/src/storage/column_predicate_cmp.cpp
18. /home/xujia/starrocks/be/src/storage/rowset/array_column_writer.cpp
19. /home/xujia/starrocks/be/src/storage/rowset/column_writer.cpp
20. /home/xujia/starrocks/be/src/storage/rowset/json_column_writer.cpp
21. /home/xujia/starrocks/be/src/storage/rowset/map_column_writer.cpp
22. /home/xujia/starrocks/be/src/storage/rowset/struct_column_iterator.cpp
23. /home/xujia/starrocks/be/src/storage/rowset/struct_column_writer.cpp
24. /home/xujia/starrocks/be/src/udf/java/java_data_converter.cpp
25. /home/xujia/starrocks/be/src/udf/java/java_native_method.cpp
26. /home/xujia/starrocks/be/src/util/arrow/starrocks_column_to_arrow.cpp
27. /home/xujia/starrocks/be/src/util/json_flattener.cpp
---

## 🧩 通用规则

- column的调用方，访问子列的数据优先使用`*_column()`接口获取Column的原始指针
- 尽可能避免使用`*_column_ptr()->as_mutable_ptr()`来获取MutableColumnPtr
- 修改前后保持接口兼容。
- 提交前必须通过 `clang-tidy` 与 `unit tests`。
- 修复每个文件之前必须调用`./clangd_check.sh ${file}`获取详细的linter错误
- 修复完整必须调用`./clangd_check.sh ${file}`验证自己的修改
- 执行`./clangd_check.sh ${file}`的时候，不要随意过滤输出结果，避免漏掉错误

---

## 💡 提示
> 每个任务在独立 Chat 执行时，**必须先读取本文件**，然后执行对应的 “执行顺序” 项。
> 如果任务规模过大，请拆分为多个子阶段（Part 1 / Part 2）。