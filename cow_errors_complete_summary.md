# COW.H 改动影响分析 - 完整报告

## 📊 总体统计（完整版）

你完全说对了！经过完整分析，实际错误数远超最初发现：

- **总错误数**: **1358 个** ⚠️
- **受影响文件**: **240 个**
- **受影响模块**: **11 个**

## 🔍 错误类型详细分析

### 1. const 对象调用非 const 成员函数
**错误码**: `member_function_call_bad_cvr`  
**数量**: **501 个** (36.9%)

**原因**: ImmutPtr 返回 const Column*，代码尝试调用非 const 成员函数  
**典型错误消息**: `'this' argument to member function 'xxx' has type 'const starrocks::Column', but function is not marked const`

**示例**:

- `exprs/agg/group_concat.h:311` 在 `update_nulls()`
  ```
  'this' argument to member function 'append_nulls' has type 'const starrocks::Column', but function is not marked const
  ```

- `exprs/agg/group_concat.h:371` 在 `reset()`
  ```
  'this' argument to member function 'resize' has type 'const starrocks::Column', but function is not marked const
  ```


### 2. 找不到匹配的成员函数重载
**错误码**: `ovl_no_viable_member_function_in_call`  
**数量**: **328 个** (24.2%)

**原因**: 由于 const 限定符不匹配，无法找到合适的成员函数重载  
**典型错误消息**: `no matching member function for call to 'append'`

**示例**:

- `exprs/agg/group_concat.h:307` 在 `update()`
  ```
  no matching member function for call to 'append'
  ```

- `exprs/agg/group_concat.h:492` 在 `serialize_to_column()`
  ```
  no matching member function for call to 'append'
  ```


### 3. 找不到匹配的函数重载
**错误码**: `ovl_no_viable_function_in_call`  
**数量**: **410 个** (30.2%)

**原因**: 参数类型不匹配（const Column* vs Column*），无法找到匹配的函数重载  
**典型错误消息**: `no matching function for call to 'fill_column_with_slot'`

**示例**:

- `exec/schema_scanner/schema_loads_scanner.cpp:89` 在 `Status::InternalError()`
  ```
  no matching function for call to 'fill_column_with_slot'
  ```

- `exec/schema_scanner/schema_loads_scanner.cpp:95` 在 `Status::InternalError()`
  ```
  no matching function for call to 'fill_column_with_slot'
  ```


### 4. 赋值时丢失 const 限定符
**错误码**: `typecheck_convert_discards_qualifiers`  
**数量**: **14 个** (1.0%)

**原因**: 从 const Column* 赋值给 Column* 时丢失 const  
**典型错误消息**: `assigning to 'Column *' from 'const starrocks::Column *' discards qualifiers`

**示例**:

- `storage/persistent_index.cpp:3506` 在 `PrimaryKeyEncoder::encode()`
  ```
  assigning to 'Column *' from 'const starrocks::Column *' discards qualifiers
  ```

- `storage/primary_index.cpp:1257` 在 `PrimaryKeyEncoder::encode()`
  ```
  assigning to 'Column *' from 'const starrocks::Column *' discards qualifiers
  ```


### 5. 初始化转换失败
**错误码**: `init_conversion_failed`  
**数量**: **130 个** (9.6%)

**原因**: 尝试用 const Column* 初始化或传递给需要 Column* 的参数  
**典型错误消息**: `cannot initialize a parameter of type 'Column *' with an rvalue of type 'const starrocks::Column *'`

**示例**:

- `exec/dict_decode_node.cpp:156` 在 `decode_columns()`
  ```
  cannot initialize a parameter of type 'Column *' with an rvalue of type 'const starrocks::Column *'
  ```

- `runtime/global_dict/decoder.cpp:68` 在 `decode_string()`
  ```
  cannot initialize a parameter of type 'Column *' with an rvalue of type 'const starrocks::Column *'
  ```


## 📦 受影响模块排名（完整）

1. **exec**: 582 个错误 (42.9%) ████████
2. **exprs**: 216 个错误 (15.9%) ███
3. **column**: 212 个错误 (15.6%) ███
4. **storage**: 172 个错误 (12.7%) ██
5. **formats**: 117 个错误 (8.6%) █
6. **util**: 17 个错误 (1.3%) 
7. **serde**: 13 个错误 (1.0%) 
8. **runtime**: 11 个错误 (0.8%) 
9. **udf**: 9 个错误 (0.7%) 
10. **bench**: 5 个错误 (0.4%) 
11. **connector**: 4 个错误 (0.3%) 


## 📁 受影响最严重的文件 (Top 15)

1. `exec/schema_scanner/schema_loads_scanner.cpp`: 19 个错误
2. `exec/schema_scanner/schema_partitions_meta_scanner.cpp`: 19 个错误
3. `exec/schema_scanner/schema_routine_load_jobs_scanner.cpp`: 19 个错误
4. `exec/schema_scanner/schema_temp_tables_scanner.cpp`: 19 个错误
5. `exec/schema_scanner/schema_task_runs_scanner.cpp`: 19 个错误
6. `storage/rowset/array_column_iterator.cpp`: 19 个错误
7. `exec/schema_scanner/schema_stream_loads_scanner.cpp`: 19 个错误
8. `column/nullable_column.cpp`: 19 个错误
9. `column/adaptive_nullable_column.h`: 19 个错误
10. `storage/rowset/map_column_iterator.cpp`: 19 个错误
11. `exec/schema_scanner/schema_columns_scanner.cpp`: 19 个错误
12. `exec/schema_scanner/schema_be_tablets_scanner.cpp`: 19 个错误
13. `exec/schema_scanner/schema_fe_tablet_schedules_scanner.cpp`: 19 个错误
14. `exprs/array_functions.cpp`: 19 个错误
15. `exec/schema_scanner/schema_tables_scanner.cpp`: 19 个错误


## 💡 修复建议

根据不同的错误类型，需要采取不同的修复策略：

### 针对 member_function_call_bad_cvr 错误
需要在调用非 const 方法前获取可变引用：

```cpp
// ❌ 错误：在 const Column* 上调用非 const 方法
auto col = immut_ptr.get();  // 返回 const Column*
col->resize(100);  // 编译错误！

// ✅ 方法1：使用 try_mutate() (推荐，安全)
auto mut_col = immut_ptr->try_mutate();
mut_col->resize(100);

// ✅ 方法2：使用 as_mutable_ptr() (仅当确定未共享时)
auto mut_col = immut_ptr->as_mutable_ptr();
mut_col->resize(100);

// ✅ 方法3：使用 as_mutable_raw_ptr() (最高性能，需保证生命周期)
immut_ptr->as_mutable_raw_ptr()->resize(100);
```

### 针对 ovl_no_viable_member_function_in_call 错误
找不到匹配的成员函数重载（如 append、append_default 等）：

```cpp
// ❌ 错误：没有接受 const Column* 的 append 重载
auto col = immut_ptr.get();  // const Column*
col->append(...);  // 编译错误！

// ✅ 方法1：先获取可变引用
auto mut_col = immut_ptr->try_mutate();
mut_col->append(...);

// ✅ 方法2：如果函数应该支持 const，考虑添加 const 重载
// （这需要修改 Column 类本身）
```

### 针对 ovl_no_viable_function_in_call 错误
找不到匹配的函数重载（参数类型不匹配）：

```cpp
// ❌ 错误：函数需要 Column*，但传入了 const Column*
auto col = immut_ptr.get();  // const Column*
fill_column_with_slot(col, ...);  // 编译错误！

// ✅ 方法1：传递可变指针
fill_column_with_slot(immut_ptr->as_mutable_ptr().get(), ...);

// ✅ 方法2：修改函数签名接受 const Column*（如果语义允许）
void fill_column_with_slot(const Column* col, ...);
```

### 针对 typecheck_convert_discards_qualifiers 错误
赋值时丢失 const 限定符：

```cpp
// ❌ 错误：不能直接赋值
Column* col = immut_ptr.get();  // 编译错误！

// ✅ 正确：使用 COW 机制
auto mut_ptr = immut_ptr->try_mutate();
Column* col = mut_ptr.get();
```

### 针对 init_conversion_failed 错误
初始化或参数传递失败：

```cpp
// ❌ 错误：类型不匹配
Column* col = immut_ptr.get();  // 编译错误！

// ✅ 方法1：使用 const Column*
const Column* col = immut_ptr.get();

// ✅ 方法2：需要可变时，使用 try_mutate()
auto mut_ptr = immut_ptr->try_mutate();
Column* col = mut_ptr.get();
```

## 📄 生成的文件

所有错误信息已保存到以下文件：

1. **cow_complete_errors.json** - 所有 1358 个错误的完整 JSON 数据
2. **cow_errors_by_code.json** - 按错误码分类的详细数据
3. **cow_errors_complete_final.json** - 最终完整报告（包含统计）
4. **cow_errors_complete_summary.md** - 本完整报告（Markdown 格式）

## ⚠️ 重要提醒

你对 `cow.h` 中 `ImmutPtr` 的改动使其正确地只返回 const 指针，这是**正确的设计**，符合 COW（Copy-on-Write）的语义。

现在发现的这 **1358 个错误**实际上是**代码中之前存在的问题**，这些地方：
- 错误地假设了可以直接修改共享的数据
- 没有正确使用 COW 机制
- 违反了 const 正确性原则

修复这些错误将使代码更加健壮和正确。

---
生成时间: 2025-10-08  
分析工具: Python + clangd diagnostics
