# STRING_V2 Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a user-visible STRING_V2 data type to StarRocks that uses GermanString (inline/pointer hybrid) as the compute-layer memory representation, with the same storage format as STRING/VARCHAR.

**Architecture:** 7 phases — type pipeline, column implementation, storage integration, FE DDL, template data structures (GermanString native specialization), core functions, optimizer validation. GermanStringColumn uses MemPool for pointer-stable arena. Lazy compaction on filter. Datum carries GermanString natively. AggHashVariant/JoinHashMap/ColumnPredicate get independent GermanString specializations.

**Tech Stack:** C++ (BE), Java (FE), Thrift (gensrc), GTest (BE tests), SQL (integration tests)

**Design Spec:** `docs/superpowers/specs/2026-04-12-string-v2-type-design.md`

## Commit & Review Policy

**Git Commit:** 每个 Task 完成并通过 Acceptance Criteria 后，必须创建一个 git commit。commit message 格式：`[Feature] STRING_V2: Task N.M — <简要描述>`。确保每个 commit 是可编译、可验证的自洽单元。

**Code Review:** 每个 Phase 完成后，使用 `superpowers:code-reviewer` subagent 对该 Phase 的所有变更进行审查，对照 design spec 和 coding standards 验证实现完整性和正确性。

---

## Phase 1: Type Pipeline (Foundation) -- COMPLETED

**Review:** Code review passed. C1 (module boundary) fixed — moved german_string.h to types/. C2 (incomplete type) deferred to Task 2.1.

### Task 1.1: Thrift — Add STRING_V2 to TPrimitiveType

**Goal:** STRING_V2 exists as a Thrift primitive type for FE-BE communication.

**Files:**
- Modify: `gensrc/thrift/Types.thrift`

**Steps:**

- [x] **Step 1:** In `gensrc/thrift/Types.thrift`, add `STRING_V2` after `VARIANT` in `enum TPrimitiveType`:
  ```thrift
  VARIANT,
  STRING_V2
  ```

- [x] **Step 2:** Regenerate Thrift code by running the BE build (CMake regenerates automatically):
  ```bash
  cd gensrc && make thrift
  ```

**Acceptance Criteria:**
- `STRING_V2` appears in generated `gen_cpp/Types_types.h` as `TPrimitiveType::STRING_V2`
- `grep STRING_V2 be/build_Release/gensrc/gen_cpp/Types_types.h` returns a match (path may vary by build type)

---

### Task 1.2: BE LogicalType Enum and Type Predicates

**Goal:** `TYPE_STRING_V2` recognized by all BE type infrastructure — enum, predicates, dispatch macros, guards, conversion functions.

**Files:**
- Modify: `be/src/types/logical_type.h`
- Modify: `be/src/types/logical_type.cpp`
- Modify: `be/src/types/logical_type_infra.h`

**Steps:**

- [x] **Step 1:** In `be/src/types/logical_type.h`, add enum value and bump max:
  ```cpp
  TYPE_VARIANT = 55,
  TYPE_STRING_V2 = 56,
  TYPE_MAX_VALUE = 57
  ```

- [x] **Step 2:** In the same file, update `is_string_type()` to include TYPE_STRING_V2:
  ```cpp
  constexpr bool is_string_type(LogicalType type) {
      return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING_V2;
  }
  ```

- [x] **Step 3:** In the same file, update `is_type_compatible()` — add STRING_V2 case alongside VARCHAR:
  ```cpp
  if (lhs == TYPE_STRING_V2) {
      return rhs == TYPE_CHAR || rhs == TYPE_VARCHAR || rhs == TYPE_STRING_V2 || rhs == TYPE_HLL || rhs == TYPE_OBJECT;
  }
  ```
  Also add TYPE_STRING_V2 to existing VARCHAR and CHAR cases as compatible type.

- [x] **Step 4:** In the same file, add TYPE_STRING_V2 to `is_scalar_logical_type()`, `support_column_expr_predicate()`, `type_estimated_overhead_bytes()` (return 128, same as VARCHAR).

- [x] **Step 5:** In the same file, update `StringLTGuard`:
  ```cpp
  VALUE_GUARD(LogicalType, StringLTGuard, lt_is_string, TYPE_CHAR, TYPE_VARCHAR, TYPE_STRING_V2)
  ```

- [x] **Step 6:** In `be/src/types/logical_type_infra.h`, add TYPE_STRING_V2 to macros:
  - `APPLY_FOR_ALL_STRING_TYPE`: add `M(TYPE_STRING_V2)`
  - `APPLY_FOR_ALL_SCALAR_TYPE`: add `M(TYPE_STRING_V2)`
  - `APPLY_FOR_SCALAR_THRIFT_TYPE`: add `M(STRING_V2)`

- [x] **Step 7:** In `be/src/types/logical_type.cpp`:
  - `string_to_logical_type()`: add `if (upper_type_str == "STRING_V2") return TYPE_STRING_V2;`
  - `logical_type_to_string()`: add `case TYPE_STRING_V2: return "STRING_V2";`
  - `ScalarFieldTypeToLogicalTypeMapping` constructor: add `_data[TYPE_STRING_V2] = TYPE_STRING_V2;`
  - `thrift_to_type()` / `to_thrift()`: handled automatically by APPLY_FOR_SCALAR_THRIFT_TYPE macro update

**Acceptance Criteria:**
- `is_string_type(TYPE_STRING_V2)` returns true
- `lt_is_string<TYPE_STRING_V2>` is true at compile time
- `logical_type_to_string(TYPE_STRING_V2)` returns `"STRING_V2"`
- `string_to_logical_type("STRING_V2")` returns `TYPE_STRING_V2`
- BE compiles with no errors (run `./build.sh --be` or cmake build)

---

### Task 1.3: BE Datum — Add GermanString to Variant

**Goal:** Datum can hold and return GermanString values natively.

**Files:**
- Modify: `be/src/types/datum.h`

**Steps:**

- [x] **Step 1:** Add `#include "column/german_string.h"` to datum.h includes.

- [x] **Step 2:** Add `GermanString` to the `Variant` type:
  ```cpp
  using Variant = std::variant<std::monostate, int8_t, uint8_t, int16_t, uint16_t, uint24_t, int32_t, uint32_t,
                               int64_t, uint64_t, int96_t, int128_t, int256_t, Slice, GermanString, decimal12_t,
                               DecimalV2Value, float, double, DatumArray, DatumMap, HyperLogLog*, BitmapValue*,
                               PercentileValue*, JsonValue*, VariantRowValue*>;
  ```

- [x] **Step 3:** Add `GermanString` to `DatumKey`:
  ```cpp
  using DatumKey = std::variant<std::monostate, int8_t, uint8_t, int16_t, uint16_t, uint24_t, int32_t, uint32_t,
                                int64_t, uint64_t, int96_t, int128_t, int256_t, Slice, GermanString, decimal12_t,
                                DecimalV2Value, float, double>;
  ```

- [x] **Step 4:** Add accessors:
  ```cpp
  const GermanString& get_german_string() const { return get<GermanString>(); }
  void set_german_string(const GermanString& v) { set<decltype(v)>(v); }
  ```

- [x] **Step 5:** In `convert2DatumKey()`, add GermanString visitor:
  ```cpp
  [](const GermanString& arg) { return DatumKey(arg); },
  ```

**Acceptance Criteria:**
- `Datum d(GermanString("hello")); d.get_german_string().to_string() == "hello"` compiles and works
- BE compiles with no errors

---

### Task 1.4: BE RunTimeTypeTraits — TYPE_STRING_V2 Specialization

**Goal:** The template type system maps TYPE_STRING_V2 to GermanString/GermanStringColumn.

**Files:**
- Modify: `be/src/column/runtime_type_traits.h`
- Modify: `be/src/column/vectorized_fwd.h`

**Steps:**

- [x] **Step 1:** In `be/src/column/vectorized_fwd.h`, add forward declaration:
  ```cpp
  class GermanStringColumn;
  ```

- [x] **Step 2:** In `be/src/column/runtime_type_traits.h`, add include for german_string.h if not already present, and add the specialization after the TYPE_VARBINARY block:
  ```cpp
  template <>
  struct RunTimeTypeTraits<TYPE_STRING_V2> {
      using CppType = GermanString;
      using ColumnType = GermanStringColumn;
      using LargeColumnType = GermanStringColumn;
      using ImmContainerType = ColumnType::ImmContainer;
  };
  ```

- [x] **Step 3:** In the same file, add:
  ```cpp
  template <>
  inline constexpr bool isArithmeticLT<TYPE_STRING_V2> = false;
  ```

**Acceptance Criteria:**
- `RunTimeCppType<TYPE_STRING_V2>` is `GermanString`
- `RunTimeColumnType<TYPE_STRING_V2>` is `GermanStringColumn`
- `isArithmeticLT<TYPE_STRING_V2>` is false
- `isSliceLT<TYPE_STRING_V2>` is false (default)
- Note: Full compilation requires a stub GermanStringColumn header (Task 2.1). This task may need to be combined with the stub creation.

---

### Task 1.5: FE PrimitiveType and ScalarType

**Goal:** FE type system recognizes STRING_V2 as a string type with correct slot size, cast rules, and display.

**Files:**
- Modify: `fe/fe-type/src/main/java/com/starrocks/type/PrimitiveType.java`
- Modify: `fe/fe-type/src/main/java/com/starrocks/type/ScalarType.java`
- Modify: `fe/fe-type/src/main/java/com/starrocks/type/TypeFactory.java`

**Steps:**

- [x] **Step 1:** In `PrimitiveType.java`, add enum value after VARIANT:
  ```java
  STRING_V2("STRING_V2", 16),
  ```

- [x] **Step 2:** Update `STRING_TYPE_LIST`:
  ```java
  public static final ImmutableList<PrimitiveType> STRING_TYPE_LIST =
          ImmutableList.of(CHAR, VARCHAR, STRING_V2);
  ```

- [x] **Step 3:** Update `isStringType()` (the PrimitiveType version, line ~456):
  ```java
  public boolean isStringType() {
      return (this == VARCHAR || this == CHAR || this == HLL || this == STRING_V2);
  }
  ```

- [x] **Step 4:** Update `isCharFamily()` (line ~476):
  ```java
  public boolean isCharFamily() {
      return (this == VARCHAR || this == CHAR || this == STRING_V2);
  }
  ```

- [x] **Step 5:** In the `IMPLICIT_CAST_MAP` static initializer, add STRING_V2 with the same cast targets as VARCHAR. Also add STRING_V2 as a valid cast target for existing types that can cast to VARCHAR.

- [x] **Step 6:** Update `getTypeSize()` — add case returning 16 for STRING_V2. Update `getSlotSize()` similarly if needed.

- [x] **Step 7:** In `ScalarType.java`, add STRING_V2 handling in `toSql()`, `toString()`, `toMysqlDataTypeString()`, `toMysqlColumnTypeString()` methods — return `"STRING_V2"`.

- [x] **Step 8:** In `TypeFactory.java`, add factory method:
  ```java
  public static ScalarType createStringV2Type(int len) {
      ScalarType type = new ScalarType(PrimitiveType.STRING_V2);
      type.len = len;
      return type;
  }
  ```

**Acceptance Criteria:**
- `PrimitiveType.STRING_V2.isStringType()` returns true
- `PrimitiveType.STRING_V2.isCharFamily()` returns true
- `PrimitiveType.STRING_V2.getTypeSize()` returns 16
- `Type.isStringType()` returns true for STRING_V2 ScalarType (via STRING_TYPE_LIST)
- FE compiles: `./build.sh --fe`

---

### Task 1.6: FE DDL Validation — Parser and Analyzer

**Goal:** `CREATE TABLE ... (col STRING_V2)` parses and is accepted for Duplicate Key tables, rejected for PK/UK/AGG tables.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java` (or StarRocksParser.g4 / type mapping)
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/CreateTableAnalyzer.java`

**Steps:**

- [x] **Step 1:** In the SQL parser layer, add STRING_V2 as a recognized type keyword. Find where VARCHAR/STRING are mapped to PrimitiveType in the parser (likely in AstBuilder.java or a type resolution helper). Add a STRING_V2 mapping:
  ```java
  case "STRING_V2":
      return TypeFactory.createStringV2Type(ScalarType.DEFAULT_STRING_LENGTH);
  ```

- [x] **Step 2:** In `CreateTableAnalyzer.java`, add validation that rejects STRING_V2 in non-Duplicate Key tables. Find the key type validation section and add:
  ```java
  if (column.getType().getPrimitiveType() == PrimitiveType.STRING_V2) {
      if (keysType != KeysType.DUP_KEYS) {
          throw new SemanticException("STRING_V2 type is only supported in Duplicate Key tables");
      }
  }
  ```

**Acceptance Criteria:**
- FE compiles
- FE unit test: parsing `CREATE TABLE t (id INT, val STRING_V2) DUPLICATE KEY(id) ...` succeeds
- FE unit test: parsing `CREATE TABLE t (id INT, val STRING_V2) PRIMARY KEY(id) ...` throws "STRING_V2 type is only supported in Duplicate Key tables"

---

## Phase 2: GermanStringColumn Core -- COMPLETED

### Task 2.1: GermanStringColumn — Skeleton and Basic Operations

**Goal:** GermanStringColumn class with constructor, append(Slice), append(GermanString), get_german_string(), get_slice(), size(), append_default(), clone_empty().

**Files:**
- Create: `be/src/column/german_string_column.h`
- Create: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Create `be/src/column/german_string_column.h` with the class skeleton as defined in spec Section 2.1. Include:
  - Class inheriting from `CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn>`
  - `Container _german_strings` (Buffer<GermanString>)
  - `MemPool _arena`
  - `ValueType`, `Container`, `ImmContainer` type aliases
  - Constructor, size(), capacity(), type_size() (return sizeof(GermanString) = 16)
  - `get_german_string(idx)`, `get_slice(idx)`
  - `append(const Slice&)`, `append(const GermanString&)`
  - `append_default()`, `append_default(size_t count)`
  - `clone_empty()`, `clone()`
  - `get_name()` returning `"german-string"`

- [x] **Step 2:** Create `be/src/column/german_string_column.cpp` implementing:
  - `append(const Slice& str)`: if str.size <= 12, construct inline GermanString; else allocate in `_arena`, construct long GermanString with ptr
  - `get_slice(idx)`: return `Slice(gs.get_data(), gs.len)`
  - Other basic methods

- [x] **Step 3:** Verify compilation: `./build.sh --be` (may need stub implementations for pure virtual methods returning Status::NotSupported initially)

**Note:** TYPE_STRING_V2 was removed from APPLY_FOR_ALL_SCALAR_TYPE and APPLY_FOR_ALL_STRING_TYPE macros because GermanString is not Slice-compatible. Created _WITH_STRING_V2 macro variants for incremental integration. Also added visitor stubs, std::hash<GermanString>, and fixed multiple switch fallthrough cases across the codebase.

**Acceptance Criteria:**
- `GermanStringColumn::create()` produces an empty column with size() == 0
- `append(Slice("hello"))` (short) → `get_german_string(0).is_inline() == true`, `get_slice(0) == Slice("hello")`
- `append(Slice("this is a long string"))` (long) → `get_german_string(0).is_inline() == false`, `get_slice(0)` returns correct data
- `clone_empty()` returns empty column, `clone()` returns deep copy
- BE compiles

---

### Task 2.2: GermanStringColumn — Column Virtual Methods (Append Family)

**Goal:** All append-related Column virtual methods implemented: append_datum, append_strings, append_continuous_strings, append_numbers (return -1), append_value_multiple_times, append_selective, append_nulls (return false).

**Files:**
- Modify: `be/src/column/german_string_column.h`
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Implement `append_datum(const Datum& datum)`:
  ```cpp
  void append_datum(const Datum& datum) override {
      append(datum.get_german_string());
  }
  ```

- [x] **Step 2:** Implement `append_strings(const Slice* data, size_t size)` — iterate and call `append(Slice)` for each. Return true.

- [x] **Step 3:** Implement `append_continuous_strings(const Slice* data, size_t size)` — same as append_strings but the caller guarantees contiguous memory. Can optimize by bulk-copying the contiguous bytes to arena, then constructing GermanStrings pointing into it.

- [x] **Step 4:** Implement `append(const Column& src, size_t offset, size_t count)` — down_cast to GermanStringColumn, iterate and append each GermanString.

- [x] **Step 5:** Implement `append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size)` — similar, using index array.

- [x] **Step 6:** Implement `append_value_multiple_times(const Column& src, uint32_t index, uint32_t size)`.

- [x] **Step 7:** Implement `append_numbers()` returning -1, `append_nulls()` returning false.

**Acceptance Criteria:**
- `append_strings()` with mixed short/long strings preserves data: roundtrip via get_slice()
- `append(const Column& src, offset, count)` copies correct subset
- `append_selective()` with index array [2, 0, 1] reorders correctly
- BE compiles

---

### Task 2.3: GermanStringColumn — Serialize / Deserialize

**Goal:** Row-level serialize() and deserialize_and_append() working (used by hash join/aggregate).

**Files:**
- Modify: `be/src/column/german_string_column.h`
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Implement `serialize(size_t idx, uint8_t* pos)` — write [uint32_t len][bytes] format (same as BinaryColumn):
  ```cpp
  uint32_t serialize(size_t idx, uint8_t* pos) const override {
      const auto& gs = _german_strings[idx];
      auto len = gs.len;
      memcpy(pos, &len, sizeof(uint32_t));
      memcpy(pos + sizeof(uint32_t), gs.get_data(), len);
      return sizeof(uint32_t) + len;
  }
  ```

- [x] **Step 2:** Implement `deserialize_and_append(const uint8_t* pos)`:
  ```cpp
  const uint8_t* deserialize_and_append(const uint8_t* pos) override {
      uint32_t len;
      memcpy(&len, pos, sizeof(uint32_t));
      append(Slice(reinterpret_cast<const char*>(pos + sizeof(uint32_t)), len));
      return pos + sizeof(uint32_t) + len;
  }
  ```

- [x] **Step 3:** Implement `serialize_size(idx)`, `serialize_default()`, `max_one_element_serialize_size()`, `serialize_batch()`, `deserialize_and_append_batch()`, `serialize_batch_with_null_masks()`, `deserialize_and_append_batch_nullable()`.

**Acceptance Criteria:**
- Serialize row → deserialize → get_slice() matches original data
- Roundtrip works for empty strings, short strings (≤12), long strings (>12)
- BE compiles

---

### Task 2.4: GermanStringColumn — Filter, Compare, Assign, Other Operations

**Goal:** filter_range (lazy compaction), compare_at, assign, remove_first_n_values, update_rows, fill_default, resize, reserve, byte_size, raw_data, get (Datum).

**Files:**
- Modify: `be/src/column/german_string_column.h`
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Implement `filter_range(const Filter& filter, size_t start, size_t to)` — only filter `_german_strings`, do NOT touch `_arena` (lazy compaction). Use the same algorithm as FixedLengthColumn::filter_range but on _german_strings buffer.

- [x] **Step 2:** Implement `compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint)`:
  ```cpp
  int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override {
      const auto& r = down_cast<const GermanStringColumn&>(rhs);
      return _german_strings[left].compare(r._german_strings[right]);
  }
  ```

- [x] **Step 3:** Implement `assign(size_t n, size_t idx)` — fill n copies of element at idx.

- [x] **Step 4:** Implement `remove_first_n_values(size_t count)` — erase first count elements from _german_strings. Arena untouched (lazy).

- [x] **Step 5:** Implement `update_rows(const Column& src, const uint32_t* indexes)`.

- [x] **Step 6:** Implement `fill_default(const Filter& filter)` — set filtered positions to empty GermanString.

- [x] **Step 7:** Implement `byte_size()`, `byte_size(from, size)`, `byte_size(idx)`, `raw_data()`, `reserve()`, `resize()`, `capacity()`.

- [x] **Step 8:** Implement `get(size_t n)` returning `Datum(_german_strings[n])`.

- [x] **Step 9:** Implement `xor_checksum()`, `debug_item()`, `debug_string()`, `capacity_limit_reached()`.

- [x] **Step 10:** Implement `swap_column()`, `reset_column()`.

- [x] **Step 11:** Implement `upgrade_if_overflow()` (return nullptr — no large variant), `downgrade()` (return nullptr), `has_large_column()` (return false).

**Acceptance Criteria:**
- `filter_range` with filter [1,0,1,0] on 4-row column produces 2-row column; arena size unchanged
- `compare_at` correctly orders strings lexicographically
- `get(n)` returns Datum with GermanString that matches the stored value
- BE compiles

---

### Task 2.5: GermanStringColumn — Compact and to_binary_column

**Goal:** Arena compaction and BinaryColumn conversion working.

**Files:**
- Modify: `be/src/column/german_string_column.h`
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Implement `compact()` as defined in spec Section 2.3.

- [x] **Step 2:** Implement `live_arena_bytes()`:
  ```cpp
  size_t live_arena_bytes() const {
      size_t total = 0;
      for (const auto& gs : _german_strings) {
          if (!gs.is_inline()) total += gs.len;
      }
      return total;
  }
  ```

- [x] **Step 3:** Implement `needs_compaction()`: `return arena_memory_usage() > 2 * live_arena_bytes();`

- [x] **Step 4:** Implement `to_binary_column()` as defined in spec Section 2.4.

- [x] **Step 5:** Implement `container_memory_usage()`:
  ```cpp
  size_t container_memory_usage() const override {
      return _german_strings.capacity() * sizeof(GermanString) + _arena.total_allocated_bytes();
  }
  ```

**Acceptance Criteria:**
- After filter that removes half the rows, `needs_compaction()` returns true
- After `compact()`, `arena_memory_usage()` approximately equals `live_arena_bytes()`
- `to_binary_column()` produces BinaryColumn with identical data (verify via get_slice comparison)
- BE compiles

---

### Task 2.6: GermanStringImmContainer

**Goal:** Immutable container view for template function dispatch.

**Files:**
- Modify: `be/src/column/german_string_column.h`

**Steps:**

- [x] **Step 1:** Define `GermanStringImmContainer` class (can be in german_string_column.h):
  ```cpp
  class GermanStringImmContainer {
  public:
      GermanStringImmContainer() = default;
      explicit GermanStringImmContainer(const GermanStringColumn& column)
          : _column(&column) {}
      GermanString operator[](size_t index) const {
          return _column->get_german_string(index);
      }
      size_t size() const { return _column ? _column->size() : 0; }
  private:
      const GermanStringColumn* _column = nullptr;
  };
  ```

- [x] **Step 2:** Ensure `GermanStringColumn::immutable_data()` returns `GermanStringImmContainer(*this)`.

**Acceptance Criteria:**
- `ImmContainer ic(col); ic[0]` returns same GermanString as `col.get_german_string(0)`
- BE compiles

---

### Task 2.7: Column Visitor Integration

**Goal:** GermanStringColumn integrates with the visitor pattern.

**Files:**
- Modify: `be/src/column/column_visitor.h`
- Modify: `be/src/column/column_visitor.cpp` (if exists, for default implementation)
- Modify: `be/src/column/column_visitor_mutable.h`
- Modify: `be/src/column/column_visitor_adapter.h`
- Modify: `be/src/column/german_string_column.h`
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** In `column_visitor.h`, add:
  ```cpp
  virtual Status visit(const GermanStringColumn& column);
  ```

- [x] **Step 2:** In `column_visitor_mutable.h`, add:
  ```cpp
  virtual Status visit(GermanStringColumn* column);
  ```

- [x] **Step 3:** In `column_visitor_adapter.h`, add to `ColumnVisitorAdapter`:
  ```cpp
  Status visit(const GermanStringColumn& column) override { return _impl->do_visit(column); }
  ```
  And to `ColumnVisitorMutableAdapter`:
  ```cpp
  Status visit(GermanStringColumn* column) override { return _impl->do_visit(column); }
  ```

- [x] **Step 4:** In `german_string_column.cpp`, implement:
  ```cpp
  Status GermanStringColumn::accept(ColumnVisitor* visitor) const { return visitor->visit(*this); }
  Status GermanStringColumn::accept(ColumnVisitorMutable* visitor) { return visitor->visit(this); }
  ```

- [x] **Step 5:** Add default implementation in column_visitor.cpp (return Status::NotSupported) for the new visit methods.

**Acceptance Criteria:**
- `column->accept(&visitor)` dispatches to the correct `visit(const GermanStringColumn&)` overload
- BE compiles

---

### Task 2.8: GermanStringColumn Unit Tests

**Goal:** Comprehensive unit tests for all GermanStringColumn operations.

**Files:**
- Create: `be/test/column/german_string_column_test.cpp`

**Steps:**

- [x] **Step 1:** Write tests covering all cases listed in spec Section 7.1:
  - test_create_empty, test_append_short_string, test_append_long_string, test_append_mixed, test_append_empty_string
  - test_get_slice, test_get_german_string, test_compare_at
  - test_serialize_deserialize, test_clone, test_filter_range
  - test_append_selective, test_to_binary_column
  - test_append_strings_from_slice, test_append_continuous_strings
  - test_arena_stability, test_large_column, test_hash_functions
  - test_swap_column, test_reset_column
  - test_compact, test_needs_compaction

- [x] **Step 2:** Add test to CMakeLists.txt (or the appropriate build target).

- [x] **Step 3:** Run tests:
  ```bash
  ./run-be-ut.sh --build-target column_test --module column_test --without-java-ext
  ```

**Acceptance Criteria:**
- All tests pass
- Test coverage includes: empty string, short string (≤12 bytes), long string (>12 bytes), 100K+ rows, filter with compaction check, serialize/deserialize roundtrip

---

## Phase 3: Storage & Network Integration -- COMPLETED

### Task 3.1: Storage Read Path — Column Creation and Dictionary Encoding

**Goal:** `ColumnHelper::create_column()` creates GermanStringColumn for TYPE_STRING_V2. ScalarColumnIterator enables dictionary encoding for TYPE_STRING_V2.

**Files:**
- Modify: `be/src/storage/rowset/scalar_column_iterator.cpp` (add TYPE_STRING_V2 to dict encoding conditions)
- Verify: `be/src/column/column_helper.cpp` (should work automatically via type dispatch)

**Steps:**

- [x] **Step 1:** Verify `ColumnHelper::create_column()` works automatically: the `type_dispatch_column()` resolves TYPE_STRING_V2 to `GermanStringColumn::create()` via RunTimeTypeTraits. No code change needed — verify by reading the code path.

- [x] **Step 2:** In `scalar_column_iterator.cpp`, find the conditions that enable dictionary encoding for TYPE_VARCHAR (look for `type == TYPE_VARCHAR` or `is_string_type(type)`). Add TYPE_STRING_V2 to the same conditions. If `is_string_type()` is already used, this may already work.

- [x] **Step 3:** In storage schema recognition code (tablet_schema.h or similar), ensure TYPE_STRING_V2 is treated as a variable-length string type for encoding selection. Find where TYPE_VARCHAR encoding is determined and ensure TYPE_STRING_V2 follows the same path.

**Acceptance Criteria:**
- `ColumnHelper::create_column(TypeDescriptor(TYPE_STRING_V2), false)` returns a GermanStringColumn
- ScalarColumnIterator with TYPE_STRING_V2 column uses dictionary encoding when appropriate
- BE compiles

---

### Task 3.2: Storage Write Path — GermanStringColumn to BinaryColumn Conversion

**Goal:** Column writer converts GermanStringColumn to BinaryColumn at the write boundary.

**Files:**
- Modify: `be/src/storage/rowset/column_writer.cpp` (or the appropriate entry point)

**Steps:**

- [x] **Step 1:** Find the column writer's `append_data()` or equivalent method where column data is consumed. Add a check: if the input column is GermanStringColumn (check via `dynamic_cast` or a new `is_german_string_column()` virtual method), call `to_binary_column()` and proceed with the BinaryColumn.

- [x] **Step 2:** This should be a one-point conversion — all downstream encoding (dict, plain, LZ4) works on BinaryColumn unchanged.

**Acceptance Criteria:**
- Writing a GermanStringColumn to disk produces the same byte format as writing a BinaryColumn with the same data
- BE compiles

---

### Task 3.3: Network Serialization — column_array_serde Visitors

**Goal:** GermanStringColumn can be serialized/deserialized across network with the same wire format as BinaryColumn.

**Files:**
- Modify: `be/src/serde/column_array_serde.cpp`

**Steps:**

- [x] **Step 1:** Add `do_visit(const GermanStringColumn& column)` to `ColumnSerializedSizeVisitor`:
  - Compute size: sum of all string lengths + offsets overhead (same formula as BinaryColumn)

- [x] **Step 2:** Add `do_visit(const GermanStringColumn& column)` to `ColumnSerializingVisitor`:
  - Write bytes+offsets format: iterate GermanStrings, write data sequentially, track offsets

- [x] **Step 3:** Add `do_visit(GermanStringColumn* column)` to `ColumnDeserializingVisitor`:
  - Read bytes+offsets format: for each string, call `column->append(Slice(data, len))`

- [x] **Step 4:** Add visitor adapter entries in the adapter classes for GermanStringColumn.

**Acceptance Criteria:**
- Serialize GermanStringColumn → deserialize to new GermanStringColumn → data matches
- Wire format is byte-identical to serializing a BinaryColumn with the same data
- BE compiles

---

### Task 3.4: Storage and Serde Round-Trip Tests

**Goal:** End-to-end verification of read/write and network serialization.

**Files:**
- Create or modify: `be/test/storage/german_string_column_storage_test.cpp` (or add to existing test)
- Create or modify: `be/test/serde/german_string_column_serde_test.cpp` (or add to existing test)

**Steps:**

- [x] **Step 1:** Write storage roundtrip test: create GermanStringColumn with mixed short/long strings → write to segment → read back → verify data matches.

- [x] **Step 2:** Write serde roundtrip test: create GermanStringColumn → serialize → deserialize → verify data matches.

- [x] **Step 3:** Write cross-type test: create GermanStringColumn, convert to BinaryColumn, serialize BinaryColumn → deserialize as GermanStringColumn → verify data matches. (This validates wire format compatibility.)

**Acceptance Criteria:**
- All roundtrip tests pass with empty, short, long, and mixed strings

---

## Phase 4: FE DDL End-to-End

### Task 4.1: put_mysql_row_buffer — Result Output to Client

**Goal:** GermanStringColumn can output results to MySQL protocol client.

**Files:**
- Modify: `be/src/column/german_string_column.cpp`

**Steps:**

- [x] **Step 1:** Implement `put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol)`:
  ```cpp
  void GermanStringColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const override {
      auto slice = get_slice(idx);
      if (is_binary_protocol) {
          buf->push_string(slice.data, slice.size);
      } else {
          buf->push_string(slice.data, slice.size);
      }
  }
  ```

**Acceptance Criteria:**
- SELECT on a STRING_V2 column returns correct string values to the MySQL client
- BE compiles

---

### Task 4.2: SQL Integration Tests — DDL, Insert, Select

**Goal:** CREATE TABLE, INSERT, SELECT work end-to-end with STRING_V2 columns.

**Files:**
- Create: `test/sql/test_string_v2/R/test_basic.result`
- Create: `test/sql/test_string_v2/T/test_basic.sql`

**Steps:**

- [ ] **Step 1:** Write SQL test file with test cases T1-T3 from spec Section 7.4.

- [ ] **Step 2:** Run the integration test:
  ```bash
  cd test && python3 run.py -v test_string_v2
  ```

**Acceptance Criteria:**
- CREATE TABLE with STRING_V2 column succeeds on Duplicate Key table
- CREATE TABLE with STRING_V2 on Primary Key table fails with expected error
- INSERT VALUES and SELECT return correct results
- Long strings (>12 bytes) round-trip correctly
- SHOW CREATE TABLE displays STRING_V2 type

---

## Phase 5: Core Template Data Structures (GermanString Native Specialization)

### Task 5.1: GermanStringHash and GermanStringEqual Infrastructure

**Goal:** Hash and equality primitives for GermanString, usable by AggHashVariant, JoinHashMap, ColumnPredicate.

**Files:**
- Modify: `be/src/column/column_hash.h` (or create `be/src/column/german_string_hash.h`)
- Modify: `be/src/base/hash/hash.h`

**Steps:**

- [x] **Step 1:** Define `GermanStringEqual`:
  ```cpp
  struct GermanStringEqual {
      bool operator()(const GermanString& lhs, const GermanString& rhs) const {
          return lhs == rhs;
      }
  };
  ```

- [x] **Step 2:** Define `GermanStringHashWithSeed` templates (following SliceHashWithSeed pattern):
  ```cpp
  template <PhmapSeed seed>
  struct GermanStringHashWithSeed;

  template <>
  struct GermanStringHashWithSeed<PhmapSeed1> {
      std::size_t operator()(const GermanString& gs) const {
          return gs.crc32_hash(CRC_HASH_SEEDS::CRC_HASH_SEED1);
      }
  };
  // ... similar for PhmapSeed2
  ```

**Acceptance Criteria:**
- `GermanStringHashWithSeed<PhmapSeed1>()(GermanString("hello"))` returns a consistent hash
- `GermanStringEqual()(GermanString("abc"), GermanString("abc"))` returns true
- `GermanStringEqual()(GermanString("abc"), GermanString("def"))` returns false
- BE compiles

---

### Task 5.2: Column Hash Visitor for GermanStringColumn

**Goal:** Hash computation visitor works with GermanStringColumn.

**Files:**
- Modify: `be/src/column/column_hash/column_hash.cpp`

**Steps:**

- [x] **Step 1:** Add `do_visit(const GermanStringColumn& column)` to each hash visitor class (CrcHashVisitor, FnvHashVisitor, etc.):
  ```cpp
  Status do_visit(const GermanStringColumn& column) {
      const auto column_size = column.size();
      _selector.for_each([&](uint32_t idx) {
          if (idx >= column_size) return;
          uint32_t* slot_ptr = slot(idx);
          auto gs = column.get_german_string(idx);
          *slot_ptr = HashFunction::hash_german_string(gs, *slot_ptr);
      });
      return Status::OK();
  }
  ```
  Where `hash_german_string` delegates to `gs.crc32_hash(seed)` or `gs.fnv_hash(seed)`.

**Acceptance Criteria:**
- Hash computation on GermanStringColumn produces consistent results
- Hash values for STRING_V2 match hash values for STRING with the same data (important for join/aggregate correctness)
- BE compiles

---

### Task 5.3: AggHashVariant — GermanString Specialization

**Goal:** Hash aggregate with STRING_V2 key uses GermanString-native hash map.

**Files:**
- Modify: `be/src/exec/aggregate/agg_hash_map.h`
- Modify: `be/src/exec/aggregate/agg_hash_variant.h`
- Modify: `be/src/exec/aggregate/agg_hash_variant.cpp`

**Steps:**

- [ ] **Step 1:** In `agg_hash_map.h`, create `AggHashMapWithOneGermanStringKeyWithNullable` class, following the pattern of `AggHashMapWithOneStringKeyWithNullable` but using GermanString keys:
  - Key type: GermanString
  - Key extraction: `down_cast<const GermanStringColumn*>(key_column)->get_german_string(i)`
  - Key insertion: `make_hash_key(gs, pool)` — inline for short, MemPool copy for long

- [ ] **Step 2:** Define hash map type aliases:
  ```cpp
  template <PhmapSeed seed>
  using OneGermanStringAggHashMap = AggHashMapWithOneGermanStringKey<
      phmap::flat_hash_map<GermanString, AggDataPtr, GermanStringHashWithSeed<seed>, GermanStringEqual>>;
  ```

- [ ] **Step 3:** In `agg_hash_variant.h`, add enum entries: `phase1_german_string`, `phase1_null_german_string`, `phase2_german_string`, `phase2_null_german_string`, and two-level variants.

- [ ] **Step 4:** In `agg_hash_variant.cpp`:
  - Add `DEFINE_MAP_TYPE` entries for all german_string variants
  - Add `ADD_VARIANT_PHASE1_TYPE(TYPE_STRING_V2, german_string)` and phase2
  - Add to `APPLY_FOR_AGG_VARIANT_ALL` macro

**Acceptance Criteria:**
- `SELECT val, COUNT(*) FROM test_sv2 GROUP BY val` works correctly with STRING_V2 column
- Short string keys (≤12 bytes) do not allocate from MemPool (verify via MemPool::total_allocated_bytes() being 0 when all keys are short)
- BE compiles

---

### Task 5.4: JoinHashMap — GermanString Specialization

**Goal:** Hash join with STRING_V2 key uses GermanString-native operations.

**Files:**
- Modify: `be/src/exec/join/join_type_traits.h`
- Modify: `be/src/exec/join/join_key_constructor.hpp`
- Modify: `be/src/exec/join/join_hash_map_helper.h`
- Modify: `be/src/exec/join/join_hash_table.cpp`

**Steps:**

- [ ] **Step 1:** In `join_hash_map_helper.h`, add `JoinKeyHash<GermanString>` specialization.

- [ ] **Step 2:** In `join_type_traits.h`, register:
  ```cpp
  REGISTER_KEY_CONSTRUCTOR(ONE_KEY, TYPE_STRING_V2, KeyConstructorForOneKey<TYPE_STRING_V2>, ONE_KEY_STRING_V2)
  REGISTER_KEY_CONSTRUCTOR(SERIALIZED, TYPE_STRING_V2, KeyConstructorForSerialized, SERIALIZED_STRING_V2)
  ```
  Add to APPLY macros for dispatch.

- [ ] **Step 3:** In `join_key_constructor.hpp`, add GermanStringColumn branch:
  - In `build_slices()` equivalent, extract GermanString from GermanStringColumn instead of Slice from BinaryColumn
  - Use `if constexpr (std::is_same_v<RunTimeColumnType<LT>, GermanStringColumn>)` to dispatch

- [ ] **Step 4:** In `join_hash_table.cpp`, add TYPE_STRING_V2 to `_determine_key_constructor()` — follow the same logic as TYPE_VARCHAR but note that GermanString is already 16 bytes fixed.

**Acceptance Criteria:**
- `SELECT ... FROM a JOIN b ON a.sv2_col = b.sv2_col` works correctly
- BE compiles

---

### Task 5.5: ColumnPredicate — GermanString Specialization

**Goal:** Storage-layer predicates (EQ, NE, LT, LE, GT, GE, IN) work natively with GermanString.

**Files:**
- Modify: `be/src/storage/olap_type_infra.h`
- Modify: `be/src/storage/storage_type_traits.h`
- Modify: `be/src/storage/column_predicate_cmp.cpp`
- Modify: `be/src/storage/column_in_predicate.cpp`
- Modify: `be/src/storage/column_not_in_predicate.cpp`

**Steps:**

- [ ] **Step 1:** In `storage_type_traits.h`, add:
  ```cpp
  template <>
  struct StorageTypeTraits<TYPE_STRING_V2> {
      using CppType = GermanString;
  };
  ```

- [ ] **Step 2:** In `olap_type_infra.h`, add `M(TYPE_STRING_V2)` to `APPLY_FOR_COLUMN_PREDICATE_TYPE`.

- [ ] **Step 3:** In `column_predicate_cmp.cpp`, create `GermanStringColumnPredicateCmpBase` (parallel to BinaryColumnPredicateCmpBase) that:
  - Stores comparison value as GermanString
  - Evaluates by down_casting to GermanStringColumn and comparing GermanStrings

- [ ] **Step 4:** Create GermanStringColumnEqPredicate, GermanStringColumnNePredicate, GermanStringColumnLtPredicate, etc. — following the BinaryColumn predicate pattern.

- [ ] **Step 5:** In the predicate factory functions, add TYPE_STRING_V2 cases that create GermanString predicates.

- [ ] **Step 6:** In `column_in_predicate.cpp`, create GermanStringColumnInPredicate using `phmap::flat_hash_set<GermanString, GermanStringHash, GermanStringEqual>`.

**Acceptance Criteria:**
- `SELECT * FROM test_sv2 WHERE val = 'hello'` uses GermanString predicate
- `SELECT * FROM test_sv2 WHERE val IN ('a', 'b', 'c')` uses GermanString IN predicate
- All comparison predicates return correct results
- BE compiles

---

### Task 5.6: SQL Integration Tests — Aggregate and Join

**Goal:** GROUP BY and JOIN with STRING_V2 columns produce correct results.

**Files:**
- Create: `test/sql/test_string_v2/R/test_aggregate_join.result`
- Create: `test/sql/test_string_v2/T/test_aggregate_join.sql`

**Steps:**

- [ ] **Step 1:** Write SQL test cases T7-T8 from spec Section 7.4.

**Acceptance Criteria:**
- `SELECT val, COUNT(*) FROM test_sv2 GROUP BY val` returns correct counts
- `SELECT COUNT(DISTINCT val) FROM test_sv2` returns correct count
- JOIN between two STRING_V2 tables returns correct matched rows

---

## Phase 6: Core Functions

### Task 6.1: FE Function Registration — STRING_V2 in STRING_TYPES

**Goal:** String function signatures automatically registered for STRING_V2.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`

**Steps:**

- [ ] **Step 1:** Update `STRING_TYPES` to include STRING_V2:
  ```java
  private static final ImmutableList<ScalarType> STRING_TYPES =
      ImmutableList.of(CharType.CHAR, VarcharType.VARCHAR, TypeFactory.createStringV2Type(ScalarType.DEFAULT_STRING_LENGTH));
  ```

- [ ] **Step 2:** Audit functions that register STRING_TYPES in a loop — ensure they handle STRING_V2 correctly.

**Acceptance Criteria:**
- `length(STRING_V2) -> INT` is a valid function signature in the FE catalog
- FE compiles

---

### Task 6.2: BE String Functions — length, concat, substr

**Goal:** Core string functions implemented for STRING_V2, operating directly on GermanStringColumn.

**Files:**
- Create: `be/src/exprs/string_v2_functions.h`
- Create: `be/src/exprs/string_v2_functions.cpp`

**Steps:**

- [ ] **Step 1:** Implement `length()` for STRING_V2:
  ```cpp
  struct StringV2LengthImpl {
      template <typename T, typename ResultT>
      static inline ResultT apply(const T& v) {
          return v.len;  // GermanString.len is directly accessible
      }
  };
  ```

- [ ] **Step 2:** Implement `concat()` for STRING_V2 — concatenate GermanStrings, produce result GermanStringColumn.

- [ ] **Step 3:** Implement `substr()` / `substring()` for STRING_V2.

- [ ] **Step 4:** Implement `upper()` / `lower()` for STRING_V2.

- [ ] **Step 5:** Register these implementations in the BE function dispatch (connect FE function signatures to BE implementations).

**Acceptance Criteria:**
- `SELECT length(val) FROM test_sv2` returns correct lengths
- `SELECT concat(val, '_suffix') FROM test_sv2` returns correct concatenation
- `SELECT substr(val, 1, 3) FROM test_sv2` returns correct substrings
- BE compiles

---

### Task 6.3: SQL Integration Tests — Functions, NULL, Cast

**Goal:** String functions, NULL handling, and implicit cast work with STRING_V2.

**Files:**
- Create: `test/sql/test_string_v2/R/test_functions.result`
- Create: `test/sql/test_string_v2/T/test_functions.sql`

**Steps:**

- [ ] **Step 1:** Write SQL test cases T4-T6 from spec Section 7.4.

**Acceptance Criteria:**
- `SELECT length(val), concat(val, '_x'), substr(val, 1, 3) FROM test_sv2` returns correct results
- NULL handling: `IS NULL`, `IS NOT NULL` work on STRING_V2 columns
- Implicit cast: `WHERE sv2_col = CAST('hello' AS VARCHAR)` works

---

## Phase 7: Optimizer Adaptation & Validation

### Task 7.1: FE Optimizer — Hardcoded VARCHAR/CHAR Fixes

**Goal:** Optimizer rules that hardcode VARCHAR/CHAR also handle STRING_V2.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperator.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/PruneSubfieldRule.java`

**Steps:**

- [ ] **Step 1:** In `ConstantOperator.java` (line ~482), add STRING_V2:
  ```java
  else if (t == PrimitiveType.CHAR || t == PrimitiveType.VARCHAR || t == PrimitiveType.STRING_V2)
  ```

- [ ] **Step 2:** In `PruneSubfieldRule.java` (lines ~174-175), add:
  ```java
  .put(PrimitiveType.STRING_V2, jsonString)
  ```

**Acceptance Criteria:**
- FE compiles
- Optimizer UT passes (existing VARCHAR tests should not regress)

---

### Task 7.2: Implicit CAST Implementation — STRING <-> STRING_V2

**Goal:** Bidirectional implicit cast between STRING/VARCHAR and STRING_V2.

**Files:**
- Modify: BE cast expression factory (find where VARCHAR/CHAR casts are registered)
- Modify: FE cast rules if needed

**Steps:**

- [ ] **Step 1:** In BE cast expression factory, add STRING_V2 <-> VARCHAR cast:
  - STRING_V2 → VARCHAR: extract Slice from GermanString, produce BinaryColumn
  - VARCHAR → STRING_V2: extract Slice from BinaryColumn, produce GermanStringColumn

- [ ] **Step 2:** Ensure FE IMPLICIT_CAST_MAP (from Task 1.5) already covers this.

**Acceptance Criteria:**
- `SELECT sv2_col FROM t WHERE sv2_col = varchar_literal` works via implicit cast
- `SELECT CAST(sv2_col AS VARCHAR) FROM t` works
- `SELECT CAST(varchar_col AS STRING_V2) FROM t` works

---

### Task 7.3: Full SQL Integration Test Suite

**Goal:** All T1-T9 test cases pass, including rejection of non-Duplicate Key tables.

**Files:**
- Create: `test/sql/test_string_v2/R/test_full.result`
- Create: `test/sql/test_string_v2/T/test_full.sql`

**Steps:**

- [ ] **Step 1:** Write comprehensive test file combining all T1-T9 test cases from spec Section 7.4.

- [ ] **Step 2:** Run:
  ```bash
  cd test && python3 run.py -v test_string_v2
  ```

**Acceptance Criteria:**
- All T1-T9 test cases pass
- T9 specifically: `CREATE TABLE ... PRIMARY KEY ...` with STRING_V2 fails with expected error

---

### Task 7.4: Performance Benchmark

**Goal:** Measure STRING_V2 vs STRING performance for sort, aggregate, join.

**Files:**
- Create: `test/sql/test_string_v2/T/test_benchmark.sql` (manual test)

**Steps:**

- [ ] **Step 1:** Create two tables with identical data (10M+ rows), one STRING and one STRING_V2:
  ```sql
  CREATE TABLE bench_string (id INT, val STRING) DUPLICATE KEY(id) ...;
  CREATE TABLE bench_string_v2 (id INT, val STRING_V2) DUPLICATE KEY(id) ...;
  -- Load identical data into both
  ```

- [ ] **Step 2:** Compare:
  - `SELECT val FROM bench_* ORDER BY val LIMIT 100` (sorting)
  - `SELECT val, COUNT(*) FROM bench_* GROUP BY val` (aggregate)
  - `SELECT ... FROM bench_a JOIN bench_b ON a.val = b.val` (join)

**Acceptance Criteria:**
- STRING_V2 should be no slower than STRING on any operation
- STRING_V2 should show measurable improvement on sorting (prefix comparison) and aggregate/join with short strings (inline key storage)
- Document results for future reference
