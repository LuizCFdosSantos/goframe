# goframe — A pandas-inspired DataFrame library for Go

A complete, production-quality implementation of pandas' core concepts in Go, with **deep documentation explaining every design decision**.

## Project Structure

```
goframe/
├── types/
│   ├── value.go       # Tagged-union Value type (int, float, string, bool, datetime, null)
│   └── index.go       # Row label Index
├── series/
│   └── series.go      # 1D labeled array (equivalent to pd.Series)
├── dataframe/
│   └── dataframe.go   # 2D labeled table (equivalent to pd.DataFrame)
├── io/
│   └── csv.go         # CSV read/write with type inference
├── ops/
│   └── join.go        # Merge/join and concatenation
└── examples/
    └── main.go        # Documented usage examples
```

---

## Core Concepts (with pandas comparisons)

### 1. The `Value` type — Go's answer to dynamic typing

pandas can store *any* Python object in a Series cell. Go is statically typed, so we use a **tagged union** (`types.Value`):

```go
// Creating values of different types
v1 := types.Int(42)
v2 := types.Float(3.14)
v3 := types.Str("hello")
v4 := types.Bool(true)
v5 := types.Null()                              // missing data
v6 := types.DateTime(time.Date(2024, 6, 15,
        12, 30, 0, 0, time.UTC))               // date-time
v7 := types.Dec(types.NewDecimal(1500, 2)) // 15.00 (exact)

// Type-safe access with "comma ok" pattern
if n, ok := v1.AsInt(); ok {
    fmt.Println("Int:", n)  // → 42
}
if ts, ok := v6.AsDateTime(); ok {
    fmt.Println("Year:", ts.Year())  // → 2024
}
if d, ok := v7.AsDecimal(); ok {
    fmt.Println("Decimal:", d)  // → 15.00
}
// Decimal coerces to float64 for numeric aggregations
f, err = v7.ToFloat64()  // → 15.00, nil

// Universal coercion to float64 for numeric operations
f, err := v1.ToFloat64()  // → 42.0, nil
// DateTime coerces to Unix timestamp
f, err = v6.ToFloat64()   // → 1718451000.0, nil
```

**Why not `interface{}`?** Using `interface{}` (Go's `any`) means type assertions everywhere, no compile-time exhaustiveness checking, and GC pressure from heap-allocated boxed values. Our tagged union gives us a closed set of types and fast switch statements.

---

### 2. `Series` — 1D labeled array

```go
// pandas: pd.Series([85, 92, 78], index=["alice","bob","carol"], name="scores")
idx := types.NewStringIndex([]string{"alice", "bob", "carol"})
vals := []types.Value{types.Int(85), types.Int(92), types.Int(78)}
s := series.NewWithIndex(vals, idx, "scores")

// Access by label:    s.loc["alice"]
val, _ := s.Loc(types.Str("alice"))  // → 85

// Access by position: s.iloc[0]
val = s.ILoc(0)   // → 85
val = s.ILoc(-1)  // → 78  (negative indexing supported)

// Aggregations
s.Mean()   // → 85.0
s.Std()    // → 7.0
s.Sum()    // → 255
s.Count()  // → 3 (non-null count, like pandas)

// Element-wise operations
doubled := s.Apply(func(v types.Value) types.Value {
    f, _ := v.ToFloat64()
    return types.Float(f * 2)
})

// Arithmetic between Series (pandas: s1 + s2)
s1.Add(s2)
s1.Sub(s2)
s1.Mul(s2)
s1.Div(s2)
```

---

### 3. `DataFrame` — 2D labeled table

```go
// pandas: pd.DataFrame({"name": [...], "salary": [...]})
df, err := dataframe.FromMap(map[string]interface{}{
    "name":   []string{"Alice", "Bob", "Carol"},
    "dept":   []string{"Eng", "Sales", "Eng"},
    "salary": []int64{95000, 72000, 88000},
}, []string{"name", "dept", "salary"})

// Column access: df["salary"]
salaryCol := df.MustCol("salary")

// Select columns: df[["name", "salary"]]
subset, _ := df.Select("name", "salary")

// Add computed column: df["salary_k"] = df["salary"] / 1000
salaryK := salaryCol.Apply(func(v types.Value) types.Value {
    f, _ := v.ToFloat64()
    return types.Float(f / 1000)
}).Rename("salary_k")
df2, _ := df.WithColumn("salary_k", salaryK)

// Slicing
df.Head(5)           // first 5 rows
df.Tail(5)           // last 5 rows
df.ILocRange(10, 20) // rows 10–19

// Sort: df.sort_values("salary", ascending=False)
sorted, _ := df.SortBy("salary", false)

nRows, nCols := df.Shape()  // → (3, 3)
```

---

### 4. Filtering

```go
// Simple numeric filter: df[df["salary"] > 80000]
mask := df.MustCol("salary").Gt(80000)
highEarners, _ := df.Filter(mask)

// String equality: df[df["dept"] == "Eng"]
engineers, _ := df.Filter(df.MustCol("dept").EqStr("Eng"))

// Complex multi-column filter (more readable than chaining masks):
result, _ := df.Query(func(row map[string]types.Value) bool {
    dept, _ := row["dept"].AsString()
    salary, _ := row["salary"].AsInt()
    return dept == "Eng" && salary > 90000
})
```

---

### 5. GroupBy

```go
// pandas: df.groupby("dept").agg({"salary": "mean", "years": "sum"})
grouped, _ := df.GroupBy("dept", map[string]func(*series.Series) types.Value{
    "salary": func(s *series.Series) types.Value {
        return types.Float(s.Mean())
    },
    "years": func(s *series.Series) types.Value {
        return types.Float(s.Sum())
    },
})
```

**Algorithm**: Two-phase hash join:
1. Build a `map[string][]int` from group key → row indices
2. For each unique key, extract those rows into a sub-Series and apply the aggregation function

---

### 6. Merge/Join

```go
// pandas: pd.merge(employees, departments, on="dept_id", how="inner")
inner, _ := ops.Merge(employees, departments, "dept_id", &ops.MergeOptions{
    How: ops.InnerJoin,
})

// Left join (all left rows, null for unmatched right)
left, _ := ops.Merge(employees, departments, "dept_id", &ops.MergeOptions{
    How: ops.LeftJoin,
})

// Outer join (all rows from both sides)
outer, _ := ops.Merge(employees, departments, "dept_id", &ops.MergeOptions{
    How: ops.OuterJoin,
})

// Concatenation (stack DataFrames vertically)
combined, _ := ops.Concat([]*dataframe.DataFrame{q1, q2, q3, q4}, false)
```

---

### 7. CSV I/O

```go
// Read: pd.read_csv("data.csv")
df, err := goio.ReadCSVFile("data.csv", nil)  // type-infers int/float/bool/string

// Read with options
df, err = goio.ReadCSVFile("data.tsv", &goio.ReadCSVOptions{
    Delimiter:  '\t',
    NullValues: map[string]bool{"": true, "N/A": true, "-": true},
})

// Write: df.to_csv("output.csv")
err = goio.WriteCSVFile(df, "output.csv", nil)

// Write with custom null representation
err = goio.WriteCSVFile(df, "output.csv", &goio.WriteCSVOptions{
    NullValue: "NA",
})
```

**Type inference**: Reads all CSV values as strings, then for each column tries (in order): int64 → float64 → datetime64 → bool → string. DateTime is serialized as RFC3339 and re-inferred automatically on read. `KindDecimal` columns are written as plain decimal strings (e.g. `"15.00"`) and re-inferred as float64 on read; create them explicitly with `types.Dec()`.

---

### 8. Null handling

```go
// Identify nulls: series.isna()
mask := s.IsNull()

// Count nulls
s.NullCount()

// Remove nulls: series.dropna()
clean := s.DropNull()

// Fill with constant: series.fillna(0)
filled := s.FillNullFloat(0)

// Fill with column mean (common imputation)
filled = s.FillNullMean()

// DataFrame dropna (removes rows with ANY null column)
df2, _ := df.DropNull()

// DataFrame dropna on specific columns
df2, _ = df.DropNull("salary", "name")
```

---

### 9. Statistics

```go
// Summary stats: df.describe()
desc, _ := df.Describe()

// Correlation matrix: df.corr()
// Uses Pearson r = Σ((x-μx)(y-μy)) / ((n-1)·σx·σy)
corr, _ := df.Corr()

// Series statistics
s.Mean()
s.Std()    // sample std (ddof=1)
s.Min()
s.Max()
s.Sum()
s.Count()  // non-null count

// Value frequencies: series.value_counts()
counts := s.ValueCounts()  // map[string]int

// Unique values: series.unique()
unique := s.Unique()
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Tagged union `Value` instead of `interface{}` | Exhaustive switch statements, no heap allocations per value |
| Immutable operations (return new Series/DF) | Prevents aliasing bugs; matches pandas default behavior |
| Columnar storage | Analytics workloads are column-heavy; O(1) column access |
| `(result, error)` return pattern | Go idiomatic; forces callers to handle errors |
| Hash join algorithm | O(n+m) vs O(n*m) naive; what pandas uses internally |
| Bessel's correction in Std() | Sample std (ddof=1) matches pandas/numpy default |
| Null propagation in arithmetic | null + anything = null; SQL and pandas convention |

---

## Limitations vs. pandas

| Feature | goframe | pandas |
|---|---|---|
| Storage | Row-oriented `[]Value` | Columnar numpy arrays (much faster) |
| Dtype system | 7 types | 20+ numpy dtypes |
| DateTime support | ✅ (RFC3339, date-only, CSV inference) | ✅ |
| MultiIndex | ❌ | ✅ |
| Plotting | ❌ | ✅ (matplotlib) |
| Vectorized ops | ❌ (pure Go loops) | ✅ (SIMD via numpy/C) |
| Index alignment | ❌ | ✅ |

This is a reference implementation for learning purposes. For production use, consider [go-gota/gota](https://github.com/go-gota/gota) or [pola-rs/polars](https://github.com/pola-rs/polars) (Rust, with Go bindings).

---

## Getting Started

```bash
git clone https://github.com/LuizCFdosSantos/goframe
cd goframe
go run examples/main.go
```
