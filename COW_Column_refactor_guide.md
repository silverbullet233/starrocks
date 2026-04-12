# COW Column Refactor - Implementation Guide

## 目标

重构Column相关的接口，使其更好地满足COW的语义

## 基本原则

- 不改动 `be/src/common/cow.h` 接口，保持严格 COW 语义。
- 在 `column/*_column.h` 内部，子列统一以 `Column::WrappedPtr` 保存；访问子列的对外 API 严禁返回 `WrappedPtr`和`MutablePtr`，只暴露 `Ptr`/`const Column*`/`Column*`。
- `column/*_column.h`下相关Column的接口，访问子列的API要统一命名，记得删除冗余的接口, 每个子列提供两套接口，以`NullableColumn`为例:
    - 访问_data_column: `ColumnPtr& data_column_ptr() const`, `const ColumnPtr& data_column_ptr() const`, `Column* data_column() const`, `const Column* data_column() const`
    - 访问_null_column: `NullColumnPtr& null_column_ptr() const`, `const NullColumnPtr& null_column_ptr()`, `NullColumn* null_column() const`, `const NullColumn* null_column() const`
- column的调用方，如果要修改子列的话，优先考虑调用`*_column()`接口，而不是`*_column_ptr()->as_mutable_ptr()`
## 验证方式
- 按照以下的流程验证你的改动
    - 每次修改完，运行`./clangd_check.sh`来检查clangd发现的linter错误，并修复你的改动引入的错误
    - linter错误修复完成后，运行`./build.sh --be`来确保编译通过
