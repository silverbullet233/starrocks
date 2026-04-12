# STRING_V2 Type Design Specification

## Overview

Add a new user-visible data type `STRING_V2` to StarRocks. It behaves identically to `STRING`/`VARCHAR` at the SQL and storage levels, but uses a German-string memory layout (`GermanString` + Arena) in the compute layer for improved comparison and sorting performance.

**Initial Scope:**
- Duplicate Key tables only
- CREATE TABLE, data write (INSERT/Stream Load), data read (SELECT/scan)
- Core string functions registered with independent implementations
- STRING and STRING_V2 can implicitly cast to each other

**Out of Scope (initial):**
- Primary Key / Unique Key / Aggregate Key tables
- Full string function library (added incrementally later)
- ALTER TABLE type change from STRING to STRING_V2

---

## 1. Type Definition Pipeline

### 1.1 Thrift Layer

**File:** `gensrc/thrift/Types.thrift`

Add `STRING_V2` to the `TPrimitiveType` enum (after `VARIANT`):

```thrift
enum TPrimitiveType {
  // ... existing values ...
  VARIANT,
  STRING_V2
}
```

### 1.2 BE LogicalType Enum

**File:** `be/src/types/logical_type.h`

```cpp
TYPE_VARIANT = 55,
TYPE_STRING_V2 = 56,

TYPE_MAX_VALUE = 57  // bumped from 56
```

### 1.3 BE Type Predicate Functions

**File:** `be/src/types/logical_type.h`

Update `is_string_type()`:
```cpp
constexpr bool is_string_type(LogicalType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING_V2;
}
```

Update `is_type_compatible()` to treat STRING_V2 like VARCHAR for compatibility checks.

Update `is_scalar_logical_type()`, `support_column_expr_predicate()`, `type_estimated_overhead_bytes()` to include `TYPE_STRING_V2`.

### 1.4 BE Type Conversion Functions

**File:** `be/src/types/logical_type.cpp`

- `string_to_logical_type()`: add `"STRING_V2"` mapping
- `logical_type_to_string()`: add `TYPE_STRING_V2 -> "STRING_V2"` case
- `thrift_to_type()` / `to_thrift()`: covered by `APPLY_FOR_SCALAR_THRIFT_TYPE` macro update
- `ScalarFieldTypeToLogicalTypeMapping`: add `TYPE_STRING_V2` entry

### 1.5 BE Type Dispatch Macros

**File:** `be/src/types/logical_type_infra.h`

```cpp
// Add TYPE_STRING_V2 to these macros:
#define APPLY_FOR_ALL_STRING_TYPE(M) \
    M(TYPE_VARCHAR)                  \
    M(TYPE_CHAR)                     \
    M(TYPE_BINARY)                   \
    M(TYPE_VARBINARY)                \
    M(TYPE_STRING_V2)

#define APPLY_FOR_ALL_SCALAR_TYPE(M) \
    // ... existing entries ...      \
    M(TYPE_STRING_V2)

#define APPLY_FOR_SCALAR_THRIFT_TYPE(M) \
    // ... existing entries ...         \
    M(STRING_V2)
```

All `type_dispatch_*` functions that use these macros will automatically pick up the new type.

### 1.6 BE Type Guards

**File:** `be/src/types/logical_type.h`

Update the `StringLTGuard` value guard:
```cpp
VALUE_GUARD(LogicalType, StringLTGuard, lt_is_string, TYPE_CHAR, TYPE_VARCHAR, TYPE_STRING_V2)
```

This ensures `lt_is_string<TYPE_STRING_V2>` is `true`, which propagates to `StringOrBinaryGuard`, `AggregateLTGuard`, etc.

### 1.7 BE Runtime Type Traits

**File:** `be/src/column/runtime_type_traits.h`

```cpp
template <>
struct RunTimeTypeTraits<TYPE_STRING_V2> {
    using CppType = GermanString;
    using ColumnType = GermanStringColumn;
    using LargeColumnType = GermanStringColumn;  // No large variant; needed by GetContainer template
    using ImmContainerType = ColumnType::ImmContainer;
};
```

Also add:
```cpp
template <>
inline constexpr bool isArithmeticLT<TYPE_STRING_V2> = false;

// GermanString is NOT Slice, so isSliceLT stays false (the default).
// This is a KEY DIFFERENCE from TYPE_VARCHAR.
```

**Important:** `isSliceLT<TYPE_STRING_V2>` is `false` (unlike VARCHAR). The `RunTimeCppType<TYPE_STRING_V2>` is `GermanString`, not `Slice`. This has two key implications:

1. **GetContainer dispatch**: Since TYPE_STRING_V2 is in `StringLTGuard`, `GetContainer<TYPE_STRING_V2>::get_data()` takes the string branch, calling `immutable_data()` which returns `GermanStringImmContainer`. The `LargeColumnType = GermanStringColumn` avoids compilation errors; `is_large_binary()` returns false so the large-column path is never taken.

2. **Function OP templates**: For functions using `UnaryFunction`/`BinaryFunction` framework, the OP struct receives `GermanString` (not `Slice`) as input. Each STRING_V2 function OP needs to handle `GermanString` explicitly. This is by design — STRING_V2 function implementations are independent.

### 1.8 FE PrimitiveType

**File:** `fe/fe-type/src/main/java/com/starrocks/type/PrimitiveType.java`

Add enum value:
```java
STRING_V2("STRING_V2", 16),  // slot size 16, same as VARCHAR
```

Update lists and methods:
- Add to `STRING_TYPE_LIST`: `ImmutableList.of(CHAR, VARCHAR, STRING_V2)` — this makes `Type.isStringType()` (which checks `STRING_TYPE_LIST.contains()`) work automatically
- Update `PrimitiveType.isStringType()` (line 456): add `|| this == STRING_V2` — this is a **separate** hardcoded check from `Type.isStringType()`
- Update `PrimitiveType.isCharFamily()` (line 476): add `|| this == STRING_V2` — used by 48+ optimizer rules for string-specific logic
- Add to `BASIC_TYPE_LIST` (via STRING_TYPE_LIST, automatic)
- Add to `IMPLICIT_CAST_MAP`: STRING_V2 can cast to/from all types that VARCHAR can
- `getTypeSize()`: return 16 (same as VARCHAR)

### 1.9 FE ScalarType

**File:** `fe/fe-type/src/main/java/com/starrocks/type/ScalarType.java`

- `toSql()`: `case STRING_V2: return "STRING_V2"` (or `"STRING_V2(" + len + ")"` if variable length)
- `toString()`: similar
- `matchesType()`: already handled by `isStringType()` check
- `isFullyCompatible()`: already handled by `isStringType()` check

### 1.10 FE TypeFactory

**File:** `fe/fe-type/src/main/java/com/starrocks/type/TypeFactory.java`

Add factory method:
```java
public static ScalarType createStringV2Type(int len) {
    ScalarType type = new ScalarType(PrimitiveType.STRING_V2);
    type.len = len;
    return type;
}
```

### 1.11 FE FunctionSet

**File:** `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`

Update `STRING_TYPES` to include STRING_V2:
```java
ImmutableList.of(CharType.CHAR, VarcharType.VARCHAR, StringV2Type.STRING_V2)
```

All string function signatures that iterate over `STRING_TYPES` will automatically register STRING_V2 variants. Additional manual registration may be needed for functions that don't use this loop.

### 1.12 FE DDL Validation

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/CreateTableAnalyzer.java`

- Allow STRING_V2 as column type in Duplicate Key tables
- For initial scope, reject STRING_V2 in Primary Key / Unique Key / Aggregate Key tables

---

## 2. GermanStringColumn Implementation

### 2.1 Core Design

**New files:**
- `be/src/column/german_string_column.h`
- `be/src/column/german_string_column.cpp`

```
GermanStringColumn
├── Buffer<GermanString> _german_strings   // 每行一个 GermanString (16 bytes each)
├── MemPool _arena                         // 长字符串 (>12 bytes) 的 backing store
└── Column base class virtual methods       // ~40+ methods
```

**Memory Layout:**

For a column with N rows:
- `_german_strings`: N x 16 bytes = 16N bytes
- `_arena` (MemPool): total bytes of all long strings (those > 12 bytes)
- Short strings (<=12 bytes): data stored inline in `GermanString.short_rep.str`, zero _arena usage

**Pointer Stability — 使用 MemPool:**

GermanString for long strings stores a raw pointer (`long_rep.ptr`) to the string data. StarRocks BE 已有 `MemPool`（`be/src/runtime/mem_pool.h`）是一个页式内存池，天然保证指针稳定性：
- 分配的内存块永不移动（`clear()` 只重置偏移量，不释放页）
- 页从 4KB 开始，倍增至 512KB 上限
- 已集成 StarRocks 内存追踪指标
- 不支持单独释放（适合 arena 场景）

直接使用 MemPool 而非自建 PageArena，减少重复实现。

**Lazy Arena Compaction（延迟压缩）:**

`_arena` 中的内存在 column 操作中遵循"写入不删除"策略：

- **filter/select 操作**：只更新 `_german_strings`（移除/重排 GermanString 元素），不调整 `_arena`。被过滤掉的行的长字符串数据仍留在 `_arena` 中，成为"死数据"。这避免了 filter 时的内存拷贝开销。
- **空间浪费**：经过多次 filter 后，`_arena` 中可能有大量死数据。
- **压缩时机**：提供 `compact()` 方法，在合适的时机（如 arena 利用率低于阈值）重建 GermanStringColumn，只拷贝存活数据到新的 MemPool。
- **触发条件**：可在以下场景触发 compact：
  - `arena_memory_usage() > 2 * live_data_bytes()`（利用率低于 50%）
  - Column 即将序列化（网络传输或写入磁盘前）
  - 显式调用（如算子结束时）
- **`live_data_bytes()`** 计算方式：遍历 `_german_strings`，累加所有长字符串的长度。

```cpp
class GermanStringColumn final : public CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn> {
    friend class CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn>;

public:
    using ValueType = GermanString;
    using Container = Buffer<GermanString>;
    using ImmContainer = GermanStringImmContainer;

    GermanStringColumn() = default;
    explicit GermanStringColumn(size_t size);

    // Core data access
    size_t size() const override { return _german_strings.size(); }
    GermanString get_german_string(size_t idx) const { return _german_strings[idx]; }
    Slice get_slice(size_t idx) const;  // Construct Slice from GermanString for compatibility

    // Append operations
    void append(const Slice& str);
    void append(const GermanString& gs);
    void append_datum(const Datum& datum) override;
    bool append_strings(const Slice* data, size_t size) override;
    bool append_continuous_strings(const Slice* data, size_t size) override;
    void append_default() override;  // Empty string
    void append_default(size_t count) override;

    // Filter — only updates _german_strings, arena untouched (lazy compaction)
    size_t filter_range(const Filter& filter, size_t start, size_t to) override;

    // Arena compaction
    void compact();  // Rebuild column with only live data
    size_t arena_memory_usage() const { return _arena.total_allocated_bytes(); }
    size_t live_arena_bytes() const;  // Sum of long string lengths in _german_strings
    bool needs_compaction() const { return arena_memory_usage() > 2 * live_arena_bytes(); }

    // Conversion
    ColumnPtr to_binary_column() const;  // For write path reuse

    // Immutable data access
    ImmContainer immutable_data() const { return ImmContainer(*this); }
    const Container& get_german_strings_container() const { return _german_strings; }

    // Column visitor integration
    Status accept(ColumnVisitor* visitor) const override;
    Status accept(ColumnVisitorMutable* visitor) override;

    // ... all other Column virtual methods ...

private:
    Container _german_strings;
    MemPool _arena;  // Page-based arena (be/src/runtime/mem_pool.h), pointers stable across growth
};
```

### 2.2 GermanStringImmContainer

Immutable view for the template-based function dispatch framework:

```cpp
class GermanStringImmContainer {
public:
    GermanStringImmContainer() = default;
    explicit GermanStringImmContainer(const GermanStringColumn& column);

    GermanString operator[](size_t index) const;
    size_t size() const;

private:
    const GermanStringColumn* _column = nullptr;
};
```

### 2.3 compact() — Arena Compaction

```cpp
void GermanStringColumn::compact() {
    MemPool new_arena;
    Container new_gs;
    new_gs.reserve(_german_strings.size());
    for (size_t i = 0; i < _german_strings.size(); ++i) {
        const auto& gs = _german_strings[i];
        if (gs.is_inline()) {
            new_gs.push_back(gs);  // Inline data, no arena involved
        } else {
            // Allocate in new arena and copy data
            auto* ptr = new_arena.allocate(gs.len);
            memcpy(ptr, gs.get_data(), gs.len);
            new_gs.emplace_back(ptr, gs.len, ptr);
        }
    }
    _german_strings = std::move(new_gs);
    _arena.free_all();
    _arena = std::move(new_arena);
}
```

### 2.4 to_binary_column() — Write Path Conversion

```cpp
ColumnPtr GermanStringColumn::to_binary_column() const {
    auto bc = BinaryColumn::create();
    bc->reserve(size());
    for (size_t i = 0; i < size(); ++i) {
        const auto& gs = _german_strings[i];
        bc->append(Slice(gs.get_data(), gs.len));
    }
    return bc;
}
```

This is the key bridge for the write path. When the storage layer needs to persist data, GermanStringColumn is first converted to BinaryColumn, then the existing write pipeline handles it.

### 2.5 Column Visitor Integration

**Files to modify:**
- `be/src/column/column_visitor.h`: Add `virtual Status visit(const GermanStringColumn& column);`
- `be/src/column/column_visitor_mutable.h`: Add `virtual Status visit(GermanStringColumn* column);`
- `be/src/column/column_visitor_adapter.h`: Add adapter entries for both
- `be/src/column/vectorized_fwd.h`: Add `class GermanStringColumn;` forward declaration

### 2.6 Forward Declarations

**File:** `be/src/column/vectorized_fwd.h`

Add:
```cpp
class GermanStringColumn;
```

---

## 3. Storage Layer Adaptation

### 3.1 Read Path — Direct Construction from Page

**Key insight:** The storage layer's page decoders populate columns through the `Column` virtual interface (`append_strings()`, `append_continuous_strings()`, etc.). As long as GermanStringColumn implements these methods, storage code needs minimal changes.

**Changes needed:**

1. **ColumnHelper::create_column()** (`be/src/column/column_helper.cpp`):
   - When `type_desc.type == TYPE_STRING_V2`, the `type_dispatch_column()` function will resolve to `GermanStringColumn::create()` through the `RunTimeTypeTraits<TYPE_STRING_V2>::ColumnType` mapping.
   - No special-case code needed — the existing ColumnBuilder functor in column_helper.cpp handles this automatically.

2. **ScalarColumnIterator** (`be/src/storage/rowset/scalar_column_iterator.cpp`):
   - TYPE_STRING_V2 uses the same on-disk encoding as TYPE_VARCHAR (binary pages with dict encoding support)
   - Add `TYPE_STRING_V2` to the conditions that enable dictionary encoding
   - Page decoder calls `column->append_strings()` / `column->append_continuous_strings()` which are virtual — GermanStringColumn's implementations are called directly

3. **Storage schema** (`be/src/storage/tablet_schema.h`, `be/src/storage/rowset/column_reader.h`):
   - Recognize TYPE_STRING_V2 as a string-like type for encoding selection
   - Map TYPE_STRING_V2 to the same storage encoding as TYPE_VARCHAR

### 3.2 Write Path — Convert Then Reuse

**Strategy:** At the column writer entry point, if the input column is GermanStringColumn, convert it to BinaryColumn via `to_binary_column()`, then proceed with the existing write pipeline.

**Changes needed:**

1. **ColumnWriter** (`be/src/storage/rowset/column_writer.cpp`):
   - Before writing, check if column is GermanStringColumn
   - If so, call `to_binary_column()` and write the resulting BinaryColumn
   - All existing encoding logic (dictionary, plain binary, LZ4, etc.) works unchanged

2. **This conversion happens at the boundary only** — no changes needed in the page encoder, compressor, or segment writer internals.

### 3.3 Network Serialization

**File:** `be/src/serde/column_array_serde.cpp`

Add visitor branches for GermanStringColumn. The wire format is identical to BinaryColumn:

```
[uint32_t: total_bytes] [bytes_data] [uint32_t[N+1]: offsets]
```

**Serialization:** Iterate GermanString array, write each string's (data, len) as bytes+offsets format.

**Deserialization:** Read bytes+offsets format, construct GermanString for each string, storing long strings in the arena.

---

## 4. FE DDL Support

### 4.1 SQL Syntax

```sql
CREATE TABLE test_string_v2 (
    id INT,
    name STRING_V2
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;
```

### 4.2 Column Type Parser

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java` (or similar)

Add parsing for `STRING_V2` type keyword, creating `ScalarType(PrimitiveType.STRING_V2)`.

### 4.3 DDL Analyzer

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/CreateTableAnalyzer.java`

- Allow STRING_V2 columns in Duplicate Key tables
- Reject STRING_V2 in PK/UK/AGG tables with clear error message: "STRING_V2 is only supported in Duplicate Key tables"

### 4.4 Implicit Cast Rules

STRING_V2 and VARCHAR are storage-compatible. Define bidirectional implicit casts:
- `STRING_V2 -> VARCHAR`: always safe (for compatibility)
- `VARCHAR -> STRING_V2`: always safe (for mixed expressions)

In the FE's `IMPLICIT_CAST_MAP`, STRING_V2 should have the same cast targets as VARCHAR.

---

## 5. Core Function Support

### 5.1 Function Registration Strategy

Each STRING_V2 function is registered as an independent FE signature with its own BE implementation. The BE implementation operates directly on `GermanStringColumn`.

**FE registration** (`FunctionSet.java`):
```java
// Automatically registered via STRING_TYPES loop:
// length(STRING_V2) -> INT
// concat(STRING_V2, STRING_V2) -> STRING_V2
// substr(STRING_V2, INT, INT) -> STRING_V2
// etc.
```

**BE implementation**: New file `be/src/exprs/string_v2_functions.h` / `.cpp` with implementations optimized for GermanStringColumn.

### 5.2 Minimum Viable Function Set

For the initial implementation to support basic queries:

| Function | Priority | Reason |
|----------|----------|--------|
| Comparison (`=`, `!=`, `<`, `>`, `<=`, `>=`) | P0 | Predicate pushdown, sorting, join |
| Hash (fnv, crc32, murmur) | P0 | Hash join, hash aggregate, bucketing |
| `length()` | P0 | Basic function test |
| `concat()` | P1 | Common string operation |
| `substr()` / `substring()` | P1 | Common string operation |
| `upper()` / `lower()` | P1 | Common string operation |
| `like` / `regexp` | P2 | Pattern matching |
| `put_mysql_row_buffer()` | P0 | Return results to MySQL client |

### 5.3 Comparison Implementation

GermanString already has optimized `compare()`, `operator==`, `operator<` etc. The column-level `compare_at()` delegates directly:

```cpp
int GermanStringColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& r = down_cast<const GermanStringColumn&>(rhs);
    return _german_strings[left].compare(r._german_strings[right]);
}
```

This is the primary performance advantage: comparison on GermanString can short-circuit using the 4-byte prefix without dereferencing the full string pointer.

### 5.4 Hash Implementation

**File:** `be/src/column/column_hash/column_hash.cpp`

Add visitor specialization for GermanStringColumn:
```cpp
Status do_visit(const GermanStringColumn& column) {
    for each idx:
        hash = GermanString.fnv_hash(seed)  // or crc32_hash
}
```

GermanString already implements `fnv_hash()` and `crc32_hash()`.

---

## 6. 核心模板数据结构适配

### 6.1 问题本质

`ColumnPredicate`、`AggHashVariant`、`JoinHashMap` 这三个核心数据结构都按 LogicalType 进行模板实例化。它们当前对 VARCHAR 的处理模式是：

```cpp
// 通用模式：down_cast 到 BinaryColumn，提取 Slice
const auto* column = down_cast<const BinaryColumn*>(key_column);
auto key = column->get_slice(i);
```

TYPE_STRING_V2 的 ColumnType 是 GermanStringColumn（非 BinaryColumn），所以这些 `down_cast<BinaryColumn*>` 会崩溃。需要为 TYPE_STRING_V2 添加分支。

### 6.2 适配策略：Slice 桥接

**关键观察：** 这三个数据结构的核心操作都是基于 Slice 的 hash 和 memequal：

| 数据结构 | Hash 方式 | Compare 方式 | Key 存储 |
|----------|----------|-------------|---------|
| AggHashVariant | CRC-32 on Slice bytes | `memequal_padded()` | 拷贝到 MemPool |
| JoinHashMap | CRC-32 on Slice bytes | `memequal()` | 拷贝到 MemPool 或固定长度序列化 |
| ColumnPredicate | N/A | `Slice::compare()` | 谓词自身持有 Slice |

GermanString 的优势在于**有序比较**（前缀短路），而非等值比较或 hash。在 hash-based 场景（join、aggregate），GermanString 没有额外优势。因此初始实现采用 **Slice 桥接**：从 GermanStringColumn 提取 Slice，复用现有的 Slice hash/compare 基础设施。

### 6.3 统一 Slice 提取辅助函数

```cpp
// be/src/column/column_helper.h or a new utility header
template <LogicalType LT>
inline Slice get_string_column_slice(const Column* column, size_t idx) {
    if constexpr (std::is_same_v<RunTimeColumnType<LT>, GermanStringColumn>) {
        return down_cast<const GermanStringColumn*>(column)->get_slice(idx);
    } else {
        return down_cast<const BinaryColumn*>(column)->get_slice(idx);
    }
}
```

所有需要从 string 列提取 Slice 的模板代码都使用此辅助函数，避免散落的 `if constexpr` 分支。

### 6.4 AggHashVariant 适配

**文件：** `be/src/exec/aggregate/agg_hash_variant.h`, `agg_hash_variant.cpp`, `agg_hash_map.h`

**变更：**
1. 在 `ADD_VARIANT_PHASE1_TYPE` 注册中添加：
   ```cpp
   ADD_VARIANT_PHASE1_TYPE(TYPE_STRING_V2, string);  // 复用 "string" 变体
   ```
   这将 TYPE_STRING_V2 映射到与 TYPE_VARCHAR 相同的 `OneStringAggHashMap<SliceAggHashMap<seed>>`。

2. 在 `AggHashMapWithOneStringKeyWithNullable::compute_agg_states()` 中，将：
   ```cpp
   const auto* column = down_cast<const BinaryColumn*>(key_column);
   ```
   替换为基于类型的分派，或使用统一的 Slice 提取方式。由于该类不按 LogicalType 模板化（只按 HashMap 类型），需要在运行时判断列类型：
   ```cpp
   auto get_key = [&](size_t i) -> Slice {
       if (key_column->is_binary()) {
           return down_cast<const BinaryColumn*>(key_column)->get_slice(i);
       } else {
           return down_cast<const GermanStringColumn*>(key_column)->get_slice(i);
       }
   };
   ```
   或者让 GermanStringColumn 提供 `is_german_string()` 虚方法用于判断。

### 6.5 JoinHashMap 适配

**文件：** `be/src/exec/join/join_type_traits.h`, `join_key_constructor.hpp`

**变更：**
1. 注册 TYPE_STRING_V2 的 key constructor 和 method type：
   ```cpp
   REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_STRING_V2, KeyConstructorForOneKey<TYPE_STRING_V2>, ONE_KEY_STRING_V2)
   REGISTER_KEY_CONSTRUCTOR(SERIALIZED, TYPE_STRING_V2, KeyConstructorForSerialized, SERIALIZED_STRING_V2)
   ```

2. 在 `join_key_constructor.hpp` 的 `lt_is_string<LT>` 分支中，STRING_V2 已通过 `StringLTGuard` 包含。但 `build_slices()` 调用需要适配 GermanStringColumn：
   ```cpp
   if constexpr (lt_is_string<LT>) {
       // 已有的 BinaryColumn 分支
       if constexpr (std::is_same_v<RunTimeColumnType<LT>, GermanStringColumn>) {
           // 从 GermanStringColumn 构建 Slice 缓存
       } else {
           column->build_slices(slices);  // 现有 BinaryColumn 路径
       }
   }
   ```

3. 在 `join_hash_table.cpp` 的 `_determine_key_constructor()` 中，TYPE_STRING_V2 需要与 TYPE_VARCHAR 走相同的路径（短字符串固定长度优化等）。

### 6.6 ColumnPredicate 适配

**文件：** `be/src/storage/olap_type_infra.h`, `be/src/storage/column_predicate_cmp.cpp`, `be/src/storage/column_in_predicate.cpp`

**变更：**
1. 在 `APPLY_FOR_COLUMN_PREDICATE_TYPE` 宏中添加 `M(TYPE_STRING_V2)`

2. 在 `storage_type_traits.h` 中添加：
   ```cpp
   template <>
   struct StorageTypeTraits<TYPE_STRING_V2> {
       using CppType = Slice;  // 存储层仍使用 Slice 作为比较类型
   };
   ```

3. 在 `BinaryColumnPredicateCmpBase::t_evaluate()` 中，列类型判断需要兼容 GermanStringColumn：
   ```cpp
   // 现有代码：
   auto* binary_column = down_cast<BinaryColumn*>(column);
   Slice value = binary_column->get_slice(i);
   // 需要适配为：
   Slice value = column->is_binary()
       ? down_cast<BinaryColumn*>(column)->get_slice(i)
       : down_cast<GermanStringColumn*>(column)->get_slice(i);
   ```

4. 在谓词工厂函数的 switch 中添加 TYPE_STRING_V2 case，映射到 `BinaryColumnPredicate` 系列（复用 Slice 比较逻辑）。

### 6.7 长期优化方向（当前不实现）

初始实现通过 Slice 桥接复用现有基础设施。后续可以在以下场景引入 GermanString 原生优化：

| 场景 | 优化方式 | 收益 |
|------|---------|------|
| 排序 Merge | GermanStringColumn 的 `compare_at()` 使用前缀短路 | ORDER BY 性能提升 |
| Range 谓词 | GermanString 原生比较替代 Slice::compare | `WHERE col > 'xxx'` 性能提升 |
| 排序聚合 | 有序 GROUP BY 使用 GermanString 比较 | 有序聚合场景性能提升 |
| Join (Merge Join) | GermanString 原生比较 | Sort-Merge Join 性能提升 |

Hash-based 操作（Hash Join、Hash Aggregate）不会从 GermanString 获益，因为其核心操作是 CRC hash + memequal，不涉及有序比较。

---

## 7. Testing Plan

### 6.1 Unit Tests (BE)

**New test file:** `be/test/column/german_string_column_test.cpp`

| Test | Description |
|------|-------------|
| `test_create_empty` | Empty column creation, size=0 |
| `test_append_short_string` | Append strings <= 12 bytes, verify inline storage |
| `test_append_long_string` | Append strings > 12 bytes, verify arena storage |
| `test_append_mixed` | Mix of short and long strings |
| `test_append_empty_string` | Empty string handling |
| `test_get_slice` | Verify get_slice() returns correct Slice for both inline and long strings |
| `test_get_german_string` | Verify direct GermanString access |
| `test_compare_at` | Column-level comparison |
| `test_serialize_deserialize` | Round-trip serialization |
| `test_clone` | clone() and clone_empty() |
| `test_filter_range` | Filter with bitmask |
| `test_append_selective` | Selective append with index array |
| `test_to_binary_column` | Conversion to BinaryColumn preserves data |
| `test_append_strings_from_slice` | append_strings(Slice*, size) works correctly |
| `test_append_continuous_strings` | append_continuous_strings works correctly |
| `test_arena_stability` | Verify pointers remain valid after many appends (page arena) |
| `test_large_column` | Column with 100K+ rows, mixed sizes |
| `test_hash_functions` | fnv_hash, crc32_hash consistency with BinaryColumn |
| `test_swap_column` | swap_column correctness |
| `test_reset_column` | reset_column frees arena memory |

### 6.2 Type System Tests (BE)

**File:** `be/test/types/logical_type_test.cpp` (or new file)

| Test | Description |
|------|-------------|
| `test_is_string_type` | `is_string_type(TYPE_STRING_V2) == true` |
| `test_type_dispatch` | `type_dispatch_column` resolves TYPE_STRING_V2 correctly |
| `test_thrift_conversion` | Round-trip thrift_to_type / to_thrift |
| `test_string_conversion` | string_to_logical_type("STRING_V2") == TYPE_STRING_V2 |
| `test_runtime_traits` | RunTimeCppType / RunTimeColumnType resolve correctly |

### 6.3 Storage Round-Trip Tests (BE)

| Test | Description |
|------|-------------|
| `test_write_read_roundtrip` | Write GermanStringColumn → read back → verify data |
| `test_serde_roundtrip` | Network serialize → deserialize → verify data |
| `test_storage_encoding` | Verify on-disk format matches VARCHAR format |

### 6.4 SQL Integration Tests

**New test directory:** `test/sql/test_string_v2/`

```sql
-- T1: Basic DDL
CREATE TABLE test_sv2 (id INT, val STRING_V2) DUPLICATE KEY(id) ...;
SHOW CREATE TABLE test_sv2;  -- verify STRING_V2 type shown
DESC test_sv2;

-- T2: Insert and Select
INSERT INTO test_sv2 VALUES (1, 'hello'), (2, 'world'), (3, '');
SELECT * FROM test_sv2;
SELECT * FROM test_sv2 WHERE val = 'hello';
SELECT * FROM test_sv2 WHERE val != 'hello';
SELECT * FROM test_sv2 WHERE val < 'world';
SELECT * FROM test_sv2 ORDER BY val;

-- T3: Long strings (> 12 bytes, triggers arena storage)
INSERT INTO test_sv2 VALUES (4, 'this is a long string that exceeds twelve bytes');
SELECT val FROM test_sv2 WHERE id = 4;

-- T4: Basic functions
SELECT length(val) FROM test_sv2;
SELECT concat(val, '_suffix') FROM test_sv2;
SELECT substr(val, 1, 3) FROM test_sv2;

-- T5: NULL handling
CREATE TABLE test_sv2_null (id INT, val STRING_V2 NULL) DUPLICATE KEY(id) ...;
INSERT INTO test_sv2_null VALUES (1, NULL), (2, 'abc');
SELECT * FROM test_sv2_null WHERE val IS NULL;
SELECT * FROM test_sv2_null WHERE val IS NOT NULL;

-- T6: Implicit cast between STRING and STRING_V2
SELECT * FROM test_sv2 WHERE val = CAST('hello' AS VARCHAR);

-- T7: Aggregate operations
SELECT COUNT(DISTINCT val) FROM test_sv2;
SELECT val, COUNT(*) FROM test_sv2 GROUP BY val;

-- T8: Join
CREATE TABLE test_sv2_join (id INT, key STRING_V2) DUPLICATE KEY(id) ...;
INSERT INTO test_sv2_join VALUES (1, 'hello'), (2, 'xyz');
SELECT a.id, b.id FROM test_sv2 a JOIN test_sv2_join b ON a.val = b.key;

-- T9: Reject non-Duplicate Key table
CREATE TABLE test_sv2_pk (id INT, val STRING_V2, PRIMARY KEY(id)) ...;
-- Expected: error "STRING_V2 is only supported in Duplicate Key tables"
```

### 6.5 Performance Benchmark (Manual)

After implementation is complete, compare sorting and comparison performance:
```sql
-- Create two tables with identical data, one STRING and one STRING_V2
-- Compare ORDER BY performance
-- Compare GROUP BY performance
-- Compare JOIN performance
```

---

## 7. Implementation Phases

### Phase 1: Type Pipeline (Foundation)

**Goal:** TYPE_STRING_V2 recognized end-to-end, compiles successfully, no runtime support yet.

1. Thrift: add STRING_V2 to TPrimitiveType
2. BE: LogicalType enum, type predicates, dispatch macros, guards, conversion functions
3. FE: PrimitiveType enum, ScalarType, TypeFactory, implicit cast rules
4. FE: DDL validation — allow in Duplicate Key tables
5. BE: RunTimeTypeTraits specialization (stub GermanStringColumn forward declaration)
6. Build and verify compilation

### Phase 2: GermanStringColumn Core

**Goal:** GermanStringColumn fully functional as a Column.

1. Implement PageArena
2. Implement GermanStringColumn with all Column virtual methods
3. Implement GermanStringImmContainer
4. Add column visitor integration
5. Add to vectorized_fwd.h
6. Unit tests: creation, append, access, clone, filter, serialize/deserialize

### Phase 3: Storage Integration

**Goal:** Write and read STRING_V2 data to/from disk.

1. ColumnHelper::create_column() wiring (automatic via type dispatch)
2. Storage schema recognition of TYPE_STRING_V2
3. Read path: ScalarColumnIterator dictionary encoding support for TYPE_STRING_V2
4. Write path: GermanStringColumn → BinaryColumn conversion at writer boundary
5. Network serde: serialize/deserialize visitor for GermanStringColumn
6. Storage round-trip tests

### Phase 4: FE DDL End-to-End

**Goal:** CREATE TABLE with STRING_V2 and INSERT/SELECT work.

1. SQL parser: STRING_V2 type keyword
2. FE DDL analyzer: validation rules
3. FE plan generation: correct type propagation
4. End-to-end: CREATE TABLE → INSERT → SELECT
5. SQL integration tests T1-T3

### Phase 5: Core Template Data Structures

**Goal:** AggHashVariant, JoinHashMap, ColumnPredicate 支持 STRING_V2.

1. 实现 `get_string_column_slice<LT>()` 辅助函数
2. AggHashVariant: 注册 TYPE_STRING_V2 → string 变体，适配列访问
3. JoinHashMap: 注册 key constructor 和 method type，适配列访问
4. ColumnPredicate: 注册 TYPE_STRING_V2，适配谓词评估
5. storage_type_traits.h: 添加 TYPE_STRING_V2 映射
6. SQL integration tests T7-T8 (aggregate + join)

### Phase 6: Core Functions

**Goal:** Basic string functions work with STRING_V2.

1. Comparison operators (P0): column compare_at, predicate evaluation
2. Hash functions (P0): column_hash visitor
3. put_mysql_row_buffer (P0): result output to client
4. length(), concat(), substr() (P1)
5. Register FE function signatures
6. SQL integration tests T4-T6

### Phase 7: Optimizer Adaptation & Validation

**Goal:** Full test suite passes, optimizer treats STRING_V2 like STRING, edge cases handled.

1. FE optimizer: update hardcoded VARCHAR/CHAR checks (ConstantOperator, PruneSubfieldRule)
2. FE optimizer: verify isStringType() / isCharFamily() propagation for all 48+ rule locations
3. NULL handling in NullableColumn wrapper
4. Implicit cast implementation (STRING <-> STRING_V2)
5. Error handling: reject STRING_V2 in PK/UK/AGG tables
6. Optimizer UT: duplicate VARCHAR test cases for STRING_V2, verify consistent behavior
7. All SQL integration tests pass
8. Performance benchmark

---

## 8. Key Files Changed

### New Files

| File | Purpose |
|------|---------|
| `be/src/column/german_string_column.h` | GermanStringColumn class definition (uses MemPool for arena) |
| `be/src/column/german_string_column.cpp` | GermanStringColumn implementation |
| `be/src/exprs/string_v2_functions.h` | STRING_V2 function declarations |
| `be/src/exprs/string_v2_functions.cpp` | STRING_V2 function implementations |
| `be/test/column/german_string_column_test.cpp` | Unit tests |
| `test/sql/test_string_v2/` | SQL integration tests |

### Modified Files

| File | Change |
|------|--------|
| `gensrc/thrift/Types.thrift` | Add STRING_V2 to TPrimitiveType |
| `be/src/types/logical_type.h` | Enum, predicates, guards |
| `be/src/types/logical_type.cpp` | Conversion functions |
| `be/src/types/logical_type_infra.h` | Dispatch macros |
| `be/src/column/runtime_type_traits.h` | RunTimeTypeTraits specialization |
| `be/src/column/vectorized_fwd.h` | Forward declaration |
| `be/src/column/column_visitor.h` | Visit method for GermanStringColumn |
| `be/src/column/column_visitor_mutable.h` | Mutable visit method |
| `be/src/column/column_visitor_adapter.h` | Adapter entries |
| `be/src/column/column_helper.cpp` | Column creation (automatic via dispatch) |
| `be/src/column/column_hash/column_hash.cpp` | Hash visitor |
| `be/src/serde/column_array_serde.cpp` | Network serde visitors |
| `be/src/storage/rowset/scalar_column_iterator.cpp` | String type recognition |
| `be/src/storage/rowset/column_writer.cpp` | Write path conversion |
| `be/src/storage/olap_type_infra.h` | APPLY_FOR_COLUMN_PREDICATE_TYPE macro |
| `be/src/storage/column_predicate_cmp.cpp` | Predicate factory + eval dispatch |
| `be/src/storage/column_in_predicate.cpp` | IN predicate dispatch |
| `be/src/storage/storage_type_traits.h` | StorageTypeTraits for STRING_V2 |
| `be/src/exec/aggregate/agg_hash_variant.h/.cpp` | Variant registration + column access |
| `be/src/exec/aggregate/agg_hash_map.h` | String key extraction |
| `be/src/exec/join/join_type_traits.h` | Key constructor + method type registration |
| `be/src/exec/join/join_key_constructor.hpp` | Key building GermanStringColumn branch |
| `be/src/exec/join/join_hash_table.cpp` | Key type determination |
| `fe/fe-type/.../PrimitiveType.java` | Enum, type lists, cast map |
| `fe/fe-type/.../ScalarType.java` | Type display and matching |
| `fe/fe-type/.../TypeFactory.java` | Factory method |
| `fe/fe-core/.../FunctionSet.java` | STRING_TYPES list, function registration |
| `fe/fe-core/.../CreateTableAnalyzer.java` | DDL validation |
| `fe/fe-core/.../AstBuilder.java` (or parser) | SQL type keyword |
| `fe/fe-core/.../ConstantOperator.java` | Hardcoded VARCHAR/CHAR check |
| `fe/fe-core/.../PruneSubfieldRule.java` | Hardcoded VARCHAR/CHAR to jsonString map |

---

## 9. FE Optimizer Rules Audit

FE 优化器中有 48+ 处针对 STRING 类型的特殊判断。审计结果如下：

### 9.1 自动兼容（通过 isStringType() / isCharFamily()）

以下规则通过 `Type.isStringType()`（查 `STRING_TYPE_LIST`）或 `PrimitiveType.isCharFamily()` 判断，只要 STRING_V2 加入这两个入口即自动生效：

| 分类 | 规则/文件 | 行为 |
|------|----------|------|
| 类型强转 | `ImplicitCastRule.java` | STRING→数值/日期/布尔 的隐式转换 |
| 类型强转 | `ReduceCastRule.java` | 级联 CAST 优化 |
| Range 提取 | `RangeExtractor.java` | 非 EQ 的 STRING 谓词不做 range 提取 |
| Range Join | `DeriveRangeJoinPredicateRule.java` | STRING 排除在 range join 推导之外 |
| 统计信息 | `PredicateStatisticsCalculator.java` | STRING IN 谓词用 NDV 估算选择率 |
| 统计信息 | `HistogramStatisticsUtils.java` | STRING 类型使用简化行估算 |
| 统计信息 | `ExpressionStatisticCalculator.java` | STRING 常量估算 |
| 低基数字典 | `DecodeUtil/DecodeContext/DecodeCollector/DecodeRewriter` | STRING 列字典编码优化 |
| MetaScan | `RewriteSimpleAggToMetaScanRule.java` | STRING 列不做 MetaScan 重写 |
| TopN | `SplitTopNAggregateRule.java` | STRING 列不做 TopN 聚合优化 |
| 分区裁剪 | `ExternalTablePredicateExtractor.java` | STRING 分区列仅支持等值裁剪 |
| 列比较 | `ColumnRefOperator.java` | STRING 列匹配只看 isStringType() |
| JSON 子字段 | `SubfieldExpressionCollector/PruneSubfieldRule` | JSON/Variant 路径提取需要 STRING 参数 |

### 9.2 需要手动适配

以下位置直接硬编码了 `PrimitiveType.VARCHAR` 或 `PrimitiveType.CHAR`，需要额外添加 `PrimitiveType.STRING_V2`：

| 文件 | 位置 | 变更 |
|------|------|------|
| `ConstantOperator.java` (line 482) | `t == PrimitiveType.CHAR \|\| t == PrimitiveType.VARCHAR` | 添加 `\|\| t == PrimitiveType.STRING_V2` |
| `PruneSubfieldRule.java` (lines 174-175) | `.put(PrimitiveType.VARCHAR, jsonString).put(PrimitiveType.CHAR, jsonString)` | 添加 `.put(PrimitiveType.STRING_V2, jsonString)` |

### 9.3 验证策略

在 Phase 6 中，运行现有 FE 优化器的 UT，将其中涉及 VARCHAR 的测试用例复制一份并替换为 STRING_V2，确保优化行为一致。重点验证：
- 谓词下推（= / != / IN 对 STRING_V2 列生效）
- 常量折叠（STRING_V2 常量表达式能正确折叠）
- 统计信息估算（STRING_V2 列的选择率估算与 VARCHAR 一致）
- 低基数字典编码（STRING_V2 列能被字典编码优化）

---

## 10. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Arena memory waste after repeated filter operations | Lazy compaction: `compact()` when `arena_usage > 2 * live_bytes`. Also compact before serialization (write/serde) to avoid transmitting dead data. |
| MemPool memory tracking (embedded in Column) | MemPool already integrates with `memory_pool_bytes_total` metric. Monitor via `container_memory_usage()` override on GermanStringColumn. |
| `isSliceLT<TYPE_STRING_V2>` is false, code expecting Slice may break | Audit all `isSliceLT` usage. STRING_V2 functions are separate implementations, so existing code paths don't encounter GermanString. |
| Column visitor not implemented for all visitors | Initial implementation covers serde and hash visitors. Other visitors fall through to `Status::NotSupported`, which surfaces as clear errors. |
| FE function resolution complexity | STRING_V2 added to STRING_TYPES ensures automatic registration. Manual audit for functions not in the loop. |
| Optimizer hardcoded VARCHAR/CHAR checks | Audit identified 2 locations requiring manual update (ConstantOperator, PruneSubfieldRule). Verify with optimizer UT. |
| GermanStringColumn append performance | MemPool allocation is O(1) amortized (page-based, 4KB→512KB growth). Benchmark against BinaryColumn to ensure no regression. |
