# GoFrame — Detailed Code Documentation

A pandas-inspired DataFrame library for Go. This document explains every component of the codebase in depth: purpose, design decisions, data structures, and API semantics.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Module & Dependencies](#module--dependencies)
3. [types/value.go — The Value Type](#typesvaluego--the-value-type)
4. [types/index.go — The Index Type](#typesindexgo--the-index-type)
5. [types/types_test.go — Tests for types](#typestypes_testgo--tests-for-types)
6. [series/series.go — The Series Type](#seriesserie sgo--the-series-type)
7. [series/series_test.go & series_extra_test.go — Series Tests](#seriesseries_testgo--seriesseries_extra_testgo--series-tests)
8. [dataframe/dataframe.go — The DataFrame Type](#dataframedataframego--the-dataframe-type)
9. [dataframe/dataframe_test.go — DataFrame Tests](#dataframedataframe_testgo--dataframe-tests)
10. [io/csv.go — CSV I/O](#iocsvgo--csv-io)
11. [io/csv_test.go — CSV Tests](#iocsv_testgo--csv-tests)
12. [ops/join.go — Join & Concatenation](#opsjoingo--join--concatenation)
13. [ops/ops_test.go — Ops Tests](#opsops_testgo--ops-tests)
14. [examples/main.go — Usage Examples](#examplesmaingo--usage-examples)
15. [Design Principles & Trade-offs](#design-principles--trade-offs)
16. [Comparison with Pandas](#comparison-with-pandas)

---

## Project Overview

GoFrame is a learning-focused implementation of the core concepts behind Python's pandas library, written in idiomatic Go. The goal is to understand how a DataFrame library works under the hood — not to compete with production libraries in performance.

The library is organized in layers:

```
types        ← atomic primitives (Value, Index)
series       ← 1D labeled array built on types
dataframe    ← 2D table built on series
io           ← CSV reader/writer built on dataframe
ops          ← multi-dataframe operations (join, concat)
examples     ← runnable demonstrations
```

Each layer only depends on layers below it, keeping the dependency graph acyclic and testable in isolation.

---

## Module & Dependencies

**File**: `go.mod`

```
module github.com/LuizCdosSantos/goframe
go 1.21
```

The module has **zero external dependencies**. Everything — type inference, hashing, statistics — is implemented from scratch using only the Go standard library. This is intentional: it forces you to understand every algorithm rather than delegating to a third-party package.

---

## types/value.go — The Value Type

**Package**: `types`

### Purpose

`Value` is the atomic unit of data. Every cell in a Series or DataFrame is a `Value`. It can hold one of five possible kinds of data:

| Kind | Go backing type | Constant |
|------|----------------|----------|
| Null | — | `KindNull` |
| Integer | `int64` | `KindInt` |
| Float | `float64` | `KindFloat` |
| String | `string` | `KindString` |
| Bool | `bool` | `KindBool` |
| DateTime | `time.Time` | `KindDateTime` |
| Decimal | `Decimal` (scaled int64, no external deps) | `KindDecimal` |

### Why a Tagged Union Instead of `interface{}`

Go's `interface{}` (or `any`) is the idiomatic catch-all type. However, this design deliberately avoids it for several reasons:

1. **No heap allocation per value.** An `interface{}` wrapping a primitive always escapes to the heap. A struct with an explicit discriminant field stays on the stack or in a contiguous array.
2. **Exhaustive switches.** When you switch on `v.Kind`, the compiler can verify you handle every case. With `interface{}` you use type assertions which fail at runtime.
3. **Explicit null.** Go's zero values are not null. A `KindNull` value is unambiguous, whereas a nil `interface{}` is ambiguous (does it mean "not set" or "null data"?).

### Data Structure

```go
type Kind int

const (
    KindNull Kind = iota
    KindInt
    KindFloat
    KindString
    KindBool
    KindDateTime
)

type Value struct {
    Kind    Kind
    intVal  int64
    fltVal  float64
    strVal  string
    boolVal bool
    timeVal time.Time
}
```

Memory layout: approximately 49 bytes per Value on a 64-bit system (1 byte kind + 8 int64 + 8 float64 + 16 string header + 1 bool + padding). For large datasets this is heavier than a typed array, but it enables heterogeneous columns.

### Constructors

```go
func Null() Value                    // KindNull
func Int(v int64) Value              // KindInt
func Float(v float64) Value          // KindFloat
func Str(v string) Value             // KindString
func Bool(v bool) Value              // KindBool
func DateTime(v time.Time) Value     // KindDateTime
func Dec(v Decimal) Value            // KindDecimal
```

### Decimal in Numeric Operations

All statistical operations (`Sum`, `Mean`, `Std`, `Min`, `Max`) work on `KindDecimal` Series because they route through `ToFloat64()`, which returns the value as a float64:

```go
prices := series.New([]types.Value{
    types.Dec(types.NewDecimal(1500, 2)), // 15.00
    types.Dec(types.NewDecimal(2000, 2)), // 20.00
}, "price")

prices.Sum()   // → 35.0
prices.Mean()  // → 17.5
prices.Min()   // → 15.0
prices.Max()   // → 20.0
```

Decimal columns are also included in `DataFrame.Describe()` and `DataFrame.Corr()`. Sorting via `SortValues` and `SortBy` uses the exact `Decimal` comparison (no float conversion). Filtering via `Gt`, `Lt`, etc. compares against a `float64` threshold.

**Limitation**: element-wise arithmetic (`Add`, `Sub`, `Mul`, `Div`) converts operands to `float64` and returns a `Float` series. The decimal type is not preserved through arithmetic — use `Apply` with `Decimal` arithmetic directly if exact decimal arithmetic is required.

Each constructor sets the `kind` field and exactly one storage field; all others remain zero.

### Accessors (Comma-OK Pattern)

```go
func (v Value) AsInt() (int64, bool)
func (v Value) AsFloat() (float64, bool)
func (v Value) AsString() (string, bool)
func (v Value) AsBool() (bool, bool)
func (v Value) AsDateTime() (time.Time, bool)
func (v Value) AsDecimal() (Decimal, bool)
```

The second return value is `true` only when the kind matches. This mirrors how Go's type assertions and map lookups work, making callers check before using the value:

```go
if n, ok := v.AsInt(); ok {
    // safe to use n
}
```

### Numeric Conversion

```go
func (v Value) ToFloat64() (float64, bool)
```

Converts both `KindInt` and `KindFloat` to `float64`. Returns `false` for non-numeric kinds. Used throughout aggregation functions (`Sum`, `Mean`, etc.) so they can operate uniformly on int and float columns.

### Comparison

```go
func (v Value) Equal(other Value) bool
func (v Value) LessThan(other Value) bool
```

- `Equal`: returns `false` whenever either side is `KindNull` (SQL null semantics — null is not equal to anything, including itself).
- `LessThan`: used by `SortValues`. Defines a total order: null < bool < int/float < string. Within numerics, int and float are compared by converting both to float64.

### String Representation

```go
func (v Value) String() string
```

Returns a human-readable string. Null prints as `<null>`. Used by `WriteCSV` to serialize values and by tests for readable failure messages.

### `IsNull()`

```go
func (v Value) IsNull() bool { return v.kind == KindNull }
```

Simple predicate. The `KindNull` constant is zero, so the zero value of `Value{}` is a null — a deliberate choice so uninitialized values are null, not garbage.

---

## types/decimal.go — The Decimal Type

**Package**: `types`

### Purpose

`Decimal` provides exact decimal arithmetic without any external dependencies. It is a first-class `Value` kind (`KindDecimal`), replacing `float64` when exact representation is required (financial, scientific).

### Why Not float64?

```go
0.1 + 0.2 == 0.3  // false — float64 gives 0.30000000000000004
```

`Decimal` stores values as a scaled `int64`:

| Value | Internal representation |
|-------|------------------------|
| `15.99` | `{value: 1599, scale: 2}` |
| `0.001` | `{value: 1, scale: 3}` |
| `100` | `{value: 100, scale: 0}` |

### Constructors

```go
func NewDecimal(value int64, scale uint8) Decimal  // NewDecimal(1599, 2) → 15.99
func ParseDecimal(s string) (Decimal, error)       // ParseDecimal("15.99")
```

### Arithmetic

```go
func (d Decimal) Add(other Decimal) Decimal   // exact
func (d Decimal) Sub(other Decimal) Decimal   // exact
func (d Decimal) Mul(other Decimal) Decimal   // exact; result scale = a.scale + b.scale
// Division is intentionally omitted to avoid unbounded scale growth
```

Operands with different scales are automatically aligned to the higher scale before arithmetic:

```go
// "1.5" + "1.50" → aligned to scale 2 → "1.50" + "1.50" = "3.00"
```

### Comparison

```go
func (d Decimal) Cmp(other Decimal) int   // -1, 0, or 1
func (d Decimal) Equal(other Decimal) bool
func (d Decimal) LessThan(other Decimal) bool
```

Comparison is value-based regardless of scale: `NewDecimal(150, 1).Equal(NewDecimal(1500, 2))` is `true` (both represent 15.0).

### Conversion

```go
func (d Decimal) String() string        // canonical decimal string: "15.99"
func (d Decimal) ToFloat64() float64   // approximate; for display and aggregation only
```

---

## types/index.go — The Index Type

**Package**: `types`

### Purpose

An `Index` labels the rows of a Series or DataFrame. Without an index, rows can only be accessed by integer position. With an index, they can be accessed by meaningful labels (e.g., `"alice"`, `"2024-01-01"`).

### Data Structure

```go
type Index struct {
    labels []Value         // ordered label values
    posMap map[string]int  // label.String() → position
    unique bool            // are all labels distinct?
}
```

The `posMap` key is `label.String()` rather than the `Value` itself because Go maps require comparable keys and `Value` contains a slice-free struct — however, using the string representation is a pragmatic simplification. It means `Int(1)` and `Str("1")` would collide if both appeared, which is an accepted limitation for this learning implementation.

### Constructors

```go
func NewRangeIndex(n int) *Index
```
Creates an index with integer labels 0, 1, 2, …, n−1. This is the default index used when no labels are specified — equivalent to pandas' `RangeIndex`.

```go
func NewIndex(labels []Value) *Index
```
Creates an index from arbitrary `Value` labels. If any two labels have the same string representation, `unique` is set to false.

```go
func NewStringIndex(labels []string) *Index
```
Convenience wrapper that converts `[]string` to `[]Value` via `Str()`.

### Key Methods

```go
func (idx *Index) Len() int
func (idx *Index) Labels() []Value
func (idx *Index) Label(pos int) Value
```

Basic accessors for length and label retrieval.

```go
func (idx *Index) Locate(label Value) (int, bool)
```

O(1) label-to-position lookup via `posMap`. Returns `(position, true)` if found. This is the core of label-based access (`Loc`).

```go
func (idx *Index) Subset(positions []int) *Index
```

Returns a new Index containing only the labels at the given positions. Used internally by `Filter`, `ILocRange`, etc. to produce matching indexes for result Series.

```go
func (idx *Index) Append(other *Index) *Index
```

Concatenates two indexes. Used by `Concat`. The resulting `unique` flag is only true if both inputs are unique and no labels overlap.

---

## types/types_test.go — Tests for types

**Package**: `types`

Tests cover:

- **Value constructors**: Verify that `Int(5).AsInt()` returns `(5, true)` and `Int(5).AsFloat()` returns `(0, false)`.
- **Null semantics**: `Null().Equal(Null())` is `false`; `Null().IsNull()` is `true`.
- **ToFloat64**: Both `Int(3)` and `Float(3.5)` convert successfully; `Str("x")` does not.
- **LessThan ordering**: Nulls come before all other kinds; numeric comparison across int/float works.
- **Index construction**: `NewRangeIndex(3)` produces labels `[0, 1, 2]`; `Locate` returns correct positions.
- **Index.Subset**: Verifies positional subsetting preserves label identity.

---

## series/series.go — The Series Type

**Package**: `series`

### Purpose

A `Series` is a one-dimensional labeled array — the equivalent of a pandas `Series` or a single column of a spreadsheet. It pairs a `[]Value` array with an `*Index` for row labels and a `string` name.

### Data Structure

```go
type Series struct {
    data  []types.Value
    index *types.Index
    name  string
}
```

The data and index always have the same length. This invariant is enforced by constructors and maintained by all operations — no method ever produces a Series where `len(data) != index.Len()`.

### Constructors

```go
func New(data []types.Value, name string) *Series
```
Creates a Series with a default `RangeIndex` (0, 1, 2, …).

```go
func NewWithIndex(data []types.Value, idx *types.Index, name string) *Series
```
Creates a Series with a custom index. Panics if `len(data) != idx.Len()` to catch programmer errors at construction time rather than silently misaligning data.

```go
func FromInts(vals []int64, name string) *Series
func FromFloats(vals []float64, name string) *Series
func FromStrings(vals []string, name string) *Series
```
Convenience constructors that convert Go slices to `[]Value` and create a RangeIndex.

### Positional Access

```go
func (s *Series) ILoc(i int) types.Value
```

Returns the value at integer position `i`. Supports negative indexing: `ILoc(-1)` returns the last element, `ILoc(-2)` the second-to-last, etc. This mirrors Python's list indexing. Panics if the resulting index is out of bounds.

```go
func (s *Series) ILocRange(start, end int) *Series
```

Returns a slice of rows `[start, end)`. Both bounds are clamped to valid range so callers don't need to guard against boundary conditions. Returns a new Series with a subset index.

### Label-Based Access

```go
func (s *Series) Loc(label types.Value) (types.Value, bool)
```

Looks up a value by its row label. Returns `(value, true)` if the label exists in the index, `(Null(), false)` otherwise.

### Metadata

```go
func (s *Series) Len() int
func (s *Series) Name() string
func (s *Series) Index() *types.Index
func (s *Series) Values() []types.Value
```

Simple accessors. `Values()` returns a copy of the underlying slice to prevent mutation of internal state.

```go
func (s *Series) Dtype() types.Kind
```

Infers the "dominant type" of the Series by scanning non-null values and returning the most common `Kind`. If the Series is all-null, returns `KindNull`. Used by `Describe()` to decide which aggregations are meaningful.

```go
func (s *Series) Rename(name string) *Series
```

Returns a new Series with a different name but sharing the same underlying data slice. This is a shallow copy — safe because `Value` is immutable.

### Filtering

```go
func (s *Series) Filter(mask *Series) (*Series, error)
```

Keeps rows where the corresponding element in `mask` is `Bool(true)`. Both Series must have the same length. The result has a new RangeIndex (positions reset to 0, 1, 2, …).

```go
func (s *Series) Where(mask *Series) (*Series, error)
```

Like `Filter`, but preserves the original index labels. Rows where mask is false become `Null()` rather than being removed. Useful when you want to maintain alignment with other Series.

### Transformation

```go
func (s *Series) Map(fn func(types.Value) types.Value) *Series
```

Applies `fn` to every element and returns a new Series. The index is preserved.

```go
func (s *Series) Apply(fn func(types.Value) types.Value) *Series
```

Alias for `Map` with identical semantics. Provided because pandas uses both names for similar operations.

```go
func (s *Series) MapWithIndex(fn func(types.Value, types.Value) types.Value) *Series
```

Like `Map`, but the function also receives the row label. Useful when the transformation depends on identity (e.g., computing age from a birth-date label).

### Element-wise Arithmetic

```go
func (s *Series) Add(other *Series) (*Series, error)
func (s *Series) Sub(other *Series) (*Series, error)
func (s *Series) Mul(other *Series) (*Series, error)
func (s *Series) Div(other *Series) (*Series, error)
```

Element-wise operations between two Series of the same length. The result type is `Float` unless both inputs are integer (in which case `Add`, `Sub`, `Mul` return `Int`; `Div` always returns `Float`). Null propagates: `null + anything = null`.

```go
func (s *Series) AddScalar(v types.Value) *Series
```

Adds a scalar to every element. Similar null and type promotion rules apply.

### Comparison Operators

```go
func (s *Series) Gt(threshold float64) *Series
func (s *Series) Lt(threshold float64) *Series
func (s *Series) Gte(threshold float64) *Series
func (s *Series) Lte(threshold float64) *Series
func (s *Series) Eq(threshold float64) *Series
func (s *Series) EqStr(target string) *Series
```

Each returns a boolean Series (elements are `Bool(true/false)` or `Null()` where input was null). These are designed to be passed directly to `Filter`:

```go
mask := scores.Gt(85)
highScores, _ := scores.Filter(mask)
```

```go
func (s *Series) IsNull() *Series
func (s *Series) IsNotNull() *Series
```

Return boolean masks for null/non-null positions.

### Null Handling

```go
func (s *Series) DropNull() *Series
```

Returns a new Series with all null values removed. The index is reset to a new RangeIndex.

```go
func (s *Series) FillNull(v types.Value) *Series
func (s *Series) FillNullFloat(f float64) *Series
```

Replace nulls with a constant value. The original index is preserved.

```go
func (s *Series) FillNullMean() *Series
```

Replace nulls with the mean of non-null numeric values. If the Series has no non-null numerics, nulls remain unchanged.

### Aggregations

```go
func (s *Series) Sum() float64
func (s *Series) Mean() float64
func (s *Series) Min() float64
func (s *Series) Max() float64
```

Operate over non-null numeric values (those for which `ToFloat64()` succeeds). Return 0 if there are no non-null numerics.

```go
func (s *Series) Std() float64
```

Sample standard deviation with **Bessel's correction** (divides by n−1, not n). This matches pandas' default `ddof=1`. Returns 0 for a Series with fewer than 2 non-null numerics.

```go
func (s *Series) Count() int
```

Returns the count of **non-null** values. This is consistent with SQL's `COUNT(column)` and pandas' default `count()`, which skip nulls. Use `Len()` if you want the total number of rows.

```go
func (s *Series) NullCount() int
```

Number of null values. Equals `Len() - Count()`.

```go
func (s *Series) ValueCounts() map[string]int
```

Returns a frequency map from `value.String()` to count. Null values are excluded.

```go
func (s *Series) Unique() []types.Value
```

Returns deduplicated values in first-seen order. Null values are excluded.

```go
func (s *Series) Describe() map[string]float64
```

Returns a summary statistics map:
- `"count"` — non-null count
- `"mean"`, `"std"`, `"min"`, `"max"` — numeric stats (only for numeric Series)
- `"null_count"` — number of nulls

### Sorting

```go
func (s *Series) SortValues(ascending bool) *Series
```

Returns a sorted Series. Uses `sort.SliceStable` to preserve relative order of equal elements. Nulls always sort to the end regardless of the `ascending` flag. The original index labels move with their corresponding values.

### Head & Tail

```go
func (s *Series) Head(n int) *Series
func (s *Series) Tail(n int) *Series
```

Return the first or last `n` rows. If `n` exceeds the Series length, the full Series is returned.

---

## series/series_test.go & series_extra_test.go — Series Tests

Tests are organized to cover each feature group independently:

- **Construction**: verify length, index, name for all constructors.
- **ILoc**: positive and negative indexing; out-of-bounds panic.
- **Loc**: hit and miss cases with custom index.
- **Filter / Where**: behavior with all-true, all-false, and partial masks; null mask elements.
- **Arithmetic**: int+int=int, int+float=float, null propagation, division-by-zero.
- **Comparison operators**: result is a bool Series; null input gives null output.
- **Aggregations**: Sum/Mean/Std/Count on a mix of int, float, and null values.
- **DropNull / FillNull / FillNullMean**: null handling edge cases.
- **ValueCounts / Unique**: correct deduplication order.
- **SortValues**: ascending and descending; null placement.

---

## dataframe/dataframe.go — The DataFrame Type

**Package**: `dataframe`

### Purpose

A `DataFrame` is a two-dimensional labeled table: rows × columns. Conceptually it is a map of column names to Series, where all Series share the same Index and therefore have the same length.

### Data Structure

```go
type DataFrame struct {
    columns  map[string]*series.Series
    colOrder []string
    index    *types.Index
}
```

- `columns` provides O(1) column lookup by name.
- `colOrder` preserves the insertion order of columns, so `Columns()` returns names in a deterministic order.
- `index` is shared by all columns. When a column is added or replaced with `WithColumn`, its index must match the DataFrame's index.

### Constructors

```go
func New(cols map[string]*series.Series, order []string) (*DataFrame, error)
```

Primary constructor. Validates that all Series have the same length, then adopts the first Series' index as the shared index.

```go
func FromMap(data map[string]interface{}, order []string) (*DataFrame, error)
```

Accepts raw Go slices (`[]int64`, `[]float64`, `[]string`, `[]bool`) and converts them to Series automatically. This is the most convenient way to create a DataFrame from in-memory data without manually constructing Value slices.

### Shape & Metadata

```go
func (df *DataFrame) Shape() (int, int)    // (nRows, nCols)
func (df *DataFrame) Columns() []string    // ordered column names
func (df *DataFrame) Len() int             // number of rows
func (df *DataFrame) HasColumn(name string) bool
```

### Column Access

```go
func (df *DataFrame) Col(name string) (*series.Series, error)
func (df *DataFrame) MustCol(name string) *series.Series
```

`Col` returns an error if the column doesn't exist. `MustCol` panics instead — use it only when you are certain the column exists (e.g., after `HasColumn` check). The panic-on-miss variant is useful in test code and examples where you don't want to clutter every access with error handling.

### Column Manipulation

```go
func (df *DataFrame) Select(names ...string) (*DataFrame, error)
```
Returns a new DataFrame with only the named columns, preserving their relative order.

```go
func (df *DataFrame) Drop(names ...string) (*DataFrame, error)
```
Returns a new DataFrame with the named columns removed.

```go
func (df *DataFrame) WithColumn(name string, s *series.Series) (*DataFrame, error)
```
Returns a new DataFrame with the column added (if `name` is new) or replaced (if it already exists). Validates that the Series length matches the DataFrame. Preserves `colOrder` — new columns are appended.

```go
func (df *DataFrame) Rename(nameMap map[string]string) (*DataFrame, error)
```
Returns a new DataFrame with columns renamed according to `nameMap`. Columns not in the map keep their original names. Returns an error if a key in `nameMap` doesn't exist.

### Row Access

```go
func (df *DataFrame) ILoc(i int) map[string]types.Value
```
Returns a single row as `map[columnName]value`. Supports negative indexing.

```go
func (df *DataFrame) ILocRange(start, end int) (*DataFrame, error)
```
Returns a subset of rows `[start, end)` as a new DataFrame. Each column is independently sliced.

```go
func (df *DataFrame) Head(n int) (*DataFrame, error)
func (df *DataFrame) Tail(n int) (*DataFrame, error)
```
First or last `n` rows.

### Filtering

```go
func (df *DataFrame) Filter(mask *series.Series) (*DataFrame, error)
```
Applies a boolean Series mask to all columns simultaneously, keeping only the rows where mask is `true`. All columns are filtered identically, preserving row alignment. The result has a new RangeIndex.

```go
func (df *DataFrame) Query(fn func(row map[string]types.Value) bool) (*DataFrame, error)
```
Row-wise predicate filtering. The function receives each row as a map and returns `true` to keep it. More flexible than `Filter` since the predicate can reference multiple columns:

```go
seniors, _ := df.Query(func(row map[string]types.Value) bool {
    age, _ := row["age"].AsInt()
    dept, _ := row["dept"].AsString()
    return age > 50 && dept == "Eng"
})
```

### Sorting

```go
func (df *DataFrame) SortBy(colName string, ascending bool) (*DataFrame, error)
```

Sorts rows by the values in a single column. Null values sort to the end. All columns are rearranged together (rows stay aligned). Returns an error if the column doesn't exist.

### GroupBy

```go
func (df *DataFrame) GroupBy(
    groupCol string,
    aggs map[string]func(*series.Series) types.Value,
) (*DataFrame, error)
```

Groups rows by unique values in `groupCol`, then applies aggregation functions to other columns. The aggregation map is `columnName → aggregatorFn`, where the function receives a Series of the group's values and returns a single `Value`.

The result is a new DataFrame with one row per unique group value and one column per entry in `aggs` (plus the group column). Example:

```go
grouped, _ := df.GroupBy("dept", map[string]func(*series.Series) types.Value{
    "salary": func(s *series.Series) types.Value {
        return types.Float(s.Mean())
    },
    "headcount": func(s *series.Series) types.Value {
        return types.Int(int64(s.Count()))
    },
})
```

This design is flexible — you can pass any aggregation logic, including custom ones. The built-in Series aggregation methods (`Mean`, `Sum`, `Max`, etc.) all have the right signature.

### Null Handling

```go
func (df *DataFrame) DropNull(cols ...string) (*DataFrame, error)
```

Drops rows that contain a null in any of the specified columns. If no columns are given, checks all columns.

```go
func (df *DataFrame) FillNull(colName string, v types.Value) (*DataFrame, error)
```

Replaces nulls in a specific column with a constant value.

### Statistics

```go
func (df *DataFrame) Describe() (*DataFrame, error)
```

Returns a DataFrame of summary statistics (count, mean, std, min, max) for all numeric columns. Each column becomes a column in the result, and the rows are labeled with the statistic name.

```go
func (df *DataFrame) Corr() (*DataFrame, error)
```

Computes the **Pearson correlation matrix** across all numeric columns. Returns a square DataFrame where `result[i][j]` is the correlation coefficient between column `i` and column `j`. The diagonal is always 1.0.

Pearson correlation between columns X and Y:
```
r = Σ((xᵢ - x̄)(yᵢ - ȳ)) / sqrt(Σ(xᵢ - x̄)² · Σ(yᵢ - ȳ)²)
```

Only rows where both columns are non-null numerics are included in the calculation.

```go
func (df *DataFrame) Apply(fn func(*series.Series) *series.Series) (*DataFrame, error)
```

Applies a function to every column Series, returning a new DataFrame with the transformed columns. Useful for bulk normalization or type conversion.

---

## dataframe/dataframe_test.go — DataFrame Tests

Tests cover:

- **New / FromMap**: shape validation, column access, error on mismatched lengths.
- **Select / Drop**: correct column sets, error on missing names.
- **WithColumn / Rename**: immutability (original unchanged), column order.
- **ILoc / ILocRange**: boundary conditions, negative indexing.
- **Filter / Query**: multi-column predicates, null rows.
- **SortBy**: ascending and descending; null rows at end.
- **GroupBy**: single-group and multi-group scenarios; custom aggregations.
- **DropNull / FillNull**: whole-row drops vs single-column fills.
- **Describe / Corr**: values verified against hand-calculated results.

---

## io/csv.go — CSV I/O

**Package**: `io`

### Purpose

Reads and writes DataFrames to/from CSV format. The reader performs automatic type inference so that a CSV column containing `"1"`, `"2"`, `"3"` becomes an integer Series rather than a string Series.

### Options

```go
type ReadCSVOptions struct {
    Delimiter  rune            // field separator (default: ',')
    HasHeader  bool            // first row is column names (default: true)
    NullValues map[string]bool // strings treated as null
    InferTypes bool            // enable type detection (default: true)
    MaxRows    int             // 0 means unlimited
}
```

Default null values: `""`, `"NA"`, `"null"`, `"NULL"`, `"N/A"`, `"nan"`, `"NaN"`.

```go
type WriteCSVOptions struct {
    Delimiter rune  // default: ','
    WriteHeader bool // default: true
}
```

### Type Inference Algorithm

When `InferTypes` is true, each column's values are parsed in this priority order:

1. **Null check**: if the string is in `NullValues`, produce `Null()`.
2. **int64**: try `strconv.ParseInt`. If successful, `Int(v)`.
3. **float64**: try `strconv.ParseFloat`. If successful, `Float(v)`.
4. **datetime64**: try parsing with `time.Parse` against several layouts (RFC3339, `"2006-01-02T15:04:05"`, `"2006-01-02 15:04:05"`, `"2006-01-02"`). If successful, `DateTime(v)`. DateTime is probed before bool because date strings would never parse as bool but the reverse is not true.
5. **bool**: try `strconv.ParseBool` (accepts `"true"`, `"false"`, `"1"`, `"0"`, etc.). If successful, `Bool(v)`.
6. **string**: fallback, always succeeds. `Str(v)`.

`KindDecimal` is not auto-inferred from CSV — decimal strings like `"15.99"` are inferred as `float64`. Create `KindDecimal` values explicitly with `types.Dec(types.NewDecimal(...))` or `types.Dec(d)` after calling `types.ParseDecimal`.

This is done per-value, so a column can contain a mix of types if the CSV data is inconsistent. The Series `Dtype()` will then reflect the dominant kind.

### Reading

```go
func ReadCSVFile(path string, opts *ReadCSVOptions) (*dataframe.DataFrame, error)
```

Opens a file and delegates to `ReadCSV`.

```go
func ReadCSV(r io.Reader, opts *ReadCSVOptions) (*dataframe.DataFrame, error)
```

Reads from any `io.Reader`. This design makes it easy to read from HTTP responses, in-memory strings, gzipped streams, etc., without changes to the core logic.

The implementation uses Go's `encoding/csv` package for actual CSV parsing (handles quoted fields, multi-line values, etc.). After parsing:

1. If `HasHeader`, the first row becomes column names; otherwise columns are named `col_0`, `col_1`, etc.
2. Each string value is type-inferred.
3. A Series is created for each column.
4. A DataFrame is assembled from those Series.

### Writing

```go
func WriteCSVFile(df *dataframe.DataFrame, path string, opts *WriteCSVOptions) error
func WriteCSV(df *dataframe.DataFrame, w io.Writer, opts *WriteCSVOptions) error
```

Serializes a DataFrame to CSV. Null values are written as empty strings. Each `Value` is converted using its `.String()` method.

---

## io/csv_test.go — CSV Tests

- **Round-trip**: write a DataFrame to CSV, read it back, verify shape and values are identical.
- **Type inference**: a CSV with int, float, bool, and null columns is correctly typed.
- **Custom delimiter**: tab-separated files.
- **NullValues**: custom null strings (`"N/A"`, `"-"`) are recognized.
- **MaxRows**: reading stops after the specified number of rows.
- **No header**: columns named `col_0`, `col_1`, etc.

---

## ops/join.go — Join & Concatenation

**Package**: `ops`

### Purpose

Implements multi-DataFrame operations that pandas provides as `pd.merge` and `pd.concat`.

### Join Types

```go
type JoinType int

const (
    InnerJoin JoinType = iota
    LeftJoin
    RightJoin
    OuterJoin
)
```

| Type | Rows included |
|------|--------------|
| InnerJoin | Only rows where the key exists in **both** DataFrames |
| LeftJoin | All left rows; unmatched right values become null |
| RightJoin | All right rows; unmatched left values become null |
| OuterJoin | All rows from both; nulls fill unmatched sides |

### Merge (Hash Join)

```go
type MergeOptions struct {
    How         JoinType // default InnerJoin
    LeftSuffix  string   // default "_left"
    RightSuffix string   // default "_right"
}

func Merge(left, right *dataframe.DataFrame, on string, opts *MergeOptions) (*dataframe.DataFrame, error)
```

Joins two DataFrames on a shared key column `on`.

**Algorithm** (O(n+m) hash join):

1. **Build phase**: iterate the right DataFrame and insert each row into a `map[keyString][]rowIndex`.
2. **Probe phase**: iterate the left DataFrame. For each left row, look up the key in the map.
   - **InnerJoin**: emit a combined row for each matching right row.
   - **LeftJoin**: emit combined rows for matches; if no match, emit the left row with null right values.
3. For `RightJoin` and `OuterJoin`, a second pass over unmatched right rows emits them with null left values.

**Null keys never match** (SQL semantics). A null key on the left won't join with a null key on the right.

**Column name conflicts**: if both DataFrames have a non-key column with the same name, the left gets `_left` suffix and the right gets `_right` suffix (configurable via `MergeOptions`).

**One-to-many joins**: if the right DataFrame has multiple rows with the same key, each left row that matches will produce multiple output rows.

### Concat (Vertical Stack)

```go
type ConcatOptions struct {
    IgnoreIndex bool // reset to RangeIndex instead of concatenating indexes
    FillMissing bool // if columns differ, fill missing with null
}

func Concat(dfs []*dataframe.DataFrame, opts *ConcatOptions) (*dataframe.DataFrame, error)
```

Stacks DataFrames vertically (appending rows).

**Algorithm**:

1. Determine the union of all column names across all DataFrames.
2. For each DataFrame, each column is either present (use its values) or missing (produce `Null()` values for those rows, if `FillMissing` is true).
3. Concatenate each column's values across all DataFrames.
4. Assemble a new DataFrame from concatenated columns.

If `FillMissing` is false and DataFrames have differing columns, an error is returned.

If `IgnoreIndex` is false, the row labels from all DataFrames are concatenated into a combined Index (possibly with duplicates). If true, a new RangeIndex is produced.

---

## ops/ops_test.go — Ops Tests

- **InnerJoin**: no rows with unmatched keys; columns from both sides present.
- **LeftJoin**: all left rows present; unmatched rows have null right columns.
- **OuterJoin**: rows from both sides; correct null filling.
- **Null key**: null key never matches.
- **Column conflict**: suffixes applied correctly.
- **One-to-many**: correct row multiplication.
- **Concat basic**: simple stacking of identical schemas.
- **Concat fill missing**: DataFrames with different columns produce null-filled output.
- **Concat ignore index**: result has RangeIndex.

---

## examples/main.go — Usage Examples

**Package**: `main`

A runnable demonstration of the full library API. Covers:

1. **Series basics**: construction, head/tail, arithmetic, aggregation.
2. **Custom index**: label-based access with `Loc`.
3. **DataFrame construction**: `FromMap` with mixed column types.
4. **Filtering**: threshold filter, equality filter, multi-column `Query`.
5. **GroupBy**: average salary and headcount per department.
6. **Merge**: inner join and left join on a key column.
7. **Null handling**: `DropNull`, `FillNull`, `FillNullMean`.
8. **Describe**: summary statistics table.
9. **Corr**: correlation matrix of numeric columns.
10. **CSV round-trip**: write then read a DataFrame.
11. **DateTime**: constructing datetime Series, sorting by timestamp, CSV round-trip with re-inference.
12. **Decimal**: exactness proof (0.10 + 0.20 = "0.30"), Decimal Series construction, all aggregations (Sum/Mean/Std/Min/Max), filtering, sorting, Describe, GroupBy, CSV write.

This file is the best starting point for understanding the API in action.

---

## Design Principles & Trade-offs

### Immutability

Every operation returns a **new** Series or DataFrame rather than mutating in place. This eliminates an entire class of bugs where a shared reference is unexpectedly modified:

```go
// Safe: df2 is a new object; df is unchanged
df2, _ := df.WithColumn("salary_k", salaryK)
```

The trade-off is additional memory allocation per operation. For a learning implementation this is acceptable.

### Columnar Storage

Columns are stored as separate `[]Value` slices. This means:
- Column access is O(1): `df.Col("salary")` returns a pointer immediately.
- Column-wise operations (aggregations, transforms) are cache-friendly.
- Row access requires gathering values from multiple slices.

Pandas/NumPy use columnar storage for the same reason.

### Error Handling

All operations that can fail return `(result, error)`. This is idiomatic Go and forces callers to acknowledge potential failures. The `MustCol` escape hatch (panics instead) is provided for contexts where the caller has already validated existence.

### Hash Join Complexity

Naive nested-loop joins are O(n·m). The hash join implementation is O(n+m) — linear in the total number of rows. For a 1000-row left × 1000-row right join, this is 2,000 operations vs 1,000,000.

### Bessel's Correction in Std

`Std()` divides by `n−1` (sample standard deviation) rather than `n` (population standard deviation). This matches pandas' default and is the statistically correct choice when estimating population variance from a sample.

### Null Semantics

The library follows SQL/pandas null semantics throughout:
- `null == null` → `false` (null is not equal to anything)
- `null + x` → `null` (null propagates through arithmetic)
- Aggregations skip nulls (`Sum`, `Mean`, `Count` all ignore null rows)

---

## Comparison with Pandas

| Feature | goframe | pandas |
|---------|---------|--------|
| Storage | Row-oriented `[]Value` | NumPy columnar arrays |
| Type system | 7 kinds (null, int, float, string, bool, datetime, decimal) | 20+ dtypes (int8…int64, datetime, category, …) |
| Performance | Pure Go loops | SIMD via C/NumPy |
| Index alignment | Manual (not automatic) | Automatic on arithmetic |
| DateTime support | `KindDateTime` (`time.Time`), CSV inference, RFC3339 serialization | Full datetime64 |
| MultiIndex | Not implemented | Supported |
| Plotting | Not implemented | matplotlib integration |
| Missing values | `KindNull` value | `NaN` / `pd.NA` |
| GroupBy | Single-column key | Multi-key, named aggregations |
| Window functions | Not implemented | `rolling`, `expanding`, `ewm` |

GoFrame is a teaching tool. For production Go data work, consider [gota](https://github.com/go-gota/gota) or [polars](https://github.com/pola-rs/polars) with Go bindings.
