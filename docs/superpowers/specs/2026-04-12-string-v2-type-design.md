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

Update lists:
- Add to `STRING_TYPE_LIST`: `ImmutableList.of(CHAR, VARCHAR, STRING_V2)`
- Add to `BASIC_TYPE_LIST` (via STRING_TYPE_LIST, automatic)
- Add to `IMPLICIT_CAST_MAP`: STRING_V2 can cast to/from all types that VARCHAR can
- `isStringType()`: returns true (automatic via STRING_TYPE_LIST membership)
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
├── RawVectorPad16<uint8_t> _arena         // 长字符串 (>12 bytes) 的 backing store
└── Column base class virtual methods       // ~40+ methods
```

**Memory Layout:**

For a column with N rows:
- `_german_strings`: N x 16 bytes = 16N bytes
- `_arena`: total bytes of all long strings (those > 12 bytes)
- Short strings (<=12 bytes): data stored inline in `GermanString.short_rep.str`, zero _arena usage

**Pointer Stability:**

GermanString for long strings stores a raw pointer (`long_rep.ptr`) to the string data. A resizable buffer (like `std::vector`) would invalidate these pointers on reallocation. To avoid this, use a **page-based arena** (linked list of fixed-size pages, e.g., 64KB each). Long string data is allocated sequentially within pages. Pages are never moved or freed until the column is destroyed, so GermanString pointers remain valid throughout the column's lifetime.

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
    PageArena _arena;  // Page-based arena, pointers stable across growth
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

### 2.3 PageArena Design

A simple page-based memory arena for stable pointers:

```cpp
class PageArena {
public:
    static constexpr size_t DEFAULT_PAGE_SIZE = 65536;  // 64KB pages

    PageArena() = default;
    ~PageArena() = default;

    // Allocate n bytes, returns pointer that remains valid for arena lifetime
    char* allocate(size_t n);

    // Total bytes allocated
    size_t memory_usage() const;

    void reset();

private:
    struct Page {
        std::unique_ptr<char[]> data;
        size_t size;
        size_t offset;
    };
    std::vector<Page> _pages;
    // Strings larger than DEFAULT_PAGE_SIZE get their own dedicated page
};
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

## 6. Testing Plan

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

### Phase 5: Core Functions

**Goal:** Basic string functions work with STRING_V2.

1. Comparison operators (P0): column compare_at, predicate evaluation
2. Hash functions (P0): column_hash visitor, join/aggregate support
3. put_mysql_row_buffer (P0): result output to client
4. length(), concat(), substr() (P1)
5. Register FE function signatures
6. SQL integration tests T4-T8

### Phase 6: Validation and Cleanup

**Goal:** Full test suite passes, edge cases handled.

1. NULL handling in NullableColumn wrapper
2. Implicit cast implementation (STRING <-> STRING_V2)
3. Error handling: reject STRING_V2 in PK/UK/AGG tables
4. All SQL integration tests pass
5. Performance benchmark

---

## 8. Key Files Changed

### New Files

| File | Purpose |
|------|---------|
| `be/src/column/german_string_column.h` | GermanStringColumn class definition |
| `be/src/column/german_string_column.cpp` | GermanStringColumn implementation |
| `be/src/column/page_arena.h` | Page-based arena allocator |
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
| `fe/fe-type/.../PrimitiveType.java` | Enum, type lists, cast map |
| `fe/fe-type/.../ScalarType.java` | Type display and matching |
| `fe/fe-type/.../TypeFactory.java` | Factory method |
| `fe/fe-core/.../FunctionSet.java` | STRING_TYPES list, function registration |
| `fe/fe-core/.../CreateTableAnalyzer.java` | DDL validation |
| `fe/fe-core/.../AstBuilder.java` (or parser) | SQL type keyword |

---

## 9. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| PageArena memory fragmentation with many small long strings | Pages are 64KB; internal fragmentation bounded at ~64KB per page. Monitor memory_usage() in tests. |
| `isSliceLT<TYPE_STRING_V2>` is false, code expecting Slice may break | Audit all `isSliceLT` usage. STRING_V2 functions are separate implementations, so existing code paths don't encounter GermanString. |
| Column visitor not implemented for all visitors | Initial implementation covers serde and hash visitors. Other visitors fall through to `Status::NotSupported`, which surfaces as clear errors. |
| FE function resolution complexity | STRING_V2 added to STRING_TYPES ensures automatic registration. Manual audit for functions not in the loop. |
| GermanStringColumn append performance (arena allocation overhead) | Page arena is O(1) amortized. Benchmark against BinaryColumn to ensure no regression. |
