// Package main demonstrates the goframe library with real-world examples.
//
// This file serves as the primary documentation through working examples.
// Every concept has a companion explanation explaining WHY we do it this way,
// not just what the code does.
//
// Run with: go run examples/main.go
package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/LuizCdosSantos/goframe/dataframe"
	goio "github.com/LuizCdosSantos/goframe/io"
	"github.com/LuizCdosSantos/goframe/ops"
	"github.com/LuizCdosSantos/goframe/series"
	"github.com/LuizCdosSantos/goframe/types"
)

func main() {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║      goframe — pandas for Go         ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Println()

	example1_Series()
	example2_DataFrame()
	example3_Filtering()
	example4_GroupBy()
	example5_Join()
	example6_NullHandling()
	example7_Statistics()
	example8_CSV()
	example9_DateTime()
	example10_Decimal()
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 1: Series basics
// ─────────────────────────────────────────────────────────────────────────────

func example1_Series() {
	fmt.Println("━━━ Example 1: Series ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Creating a Series from integers
	// pandas:  pd.Series([85, 92, 78, 95, 88], name="scores")
	scores := series.FromInts([]int64{85, 92, 78, 95, 88}, "scores")
	fmt.Println("Integer series:")
	fmt.Println(scores)

	// Creating with custom index labels
	// pandas:  pd.Series([85, 92, 78], index=["alice","bob","carol"], name="scores")
	idx := types.NewStringIndex([]string{"alice", "bob", "carol"})
	vals := []types.Value{types.Int(85), types.Int(92), types.Int(78)}
	named := series.NewWithIndex(vals, idx, "scores")
	fmt.Println("Named index series:")
	fmt.Println(named)

	// Accessing by label: series.loc["alice"]
	val, err := named.Loc(types.Str("alice"))
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("alice's score:", val) // → 85
	}

	// Accessing by position: series.iloc[0]
	fmt.Println("First element (iloc[0]):", named.ILoc(0))  // → 85
	fmt.Println("Last element (iloc[-1]):", named.ILoc(-1)) // → 78

	// Basic statistics
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("  count: %d\n", scores.Count())
	fmt.Printf("  mean:  %.2f\n", scores.Mean())
	fmt.Printf("  std:   %.2f\n", scores.Std())
	fmt.Printf("  min:   %.0f\n", scores.Min())
	fmt.Printf("  max:   %.0f\n", scores.Max())

	// Element-wise operations
	// pandas: scores * 1.1  (10% bonus)
	boosted := scores.Apply(func(v types.Value) types.Value {
		f, _ := v.ToFloat64()
		return types.Float(math.Round(f * 1.1))
	})
	fmt.Println("\n10% bonus applied:")
	fmt.Println(boosted)

	// Arithmetic between Series
	// pandas: s1 + s2
	s1 := series.FromInts([]int64{1, 2, 3}, "a")
	s2 := series.FromInts([]int64{10, 20, 30}, "b")
	sum := s1.Add(s2)
	fmt.Println("s1 + s2:", sum.ILoc(0), sum.ILoc(1), sum.ILoc(2)) // → 11, 22, 33

	fmt.Println()
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 2: DataFrame construction and access
// ─────────────────────────────────────────────────────────────────────────────

func example2_DataFrame() {
	fmt.Println("━━━ Example 2: DataFrame ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Build a DataFrame from raw slices using FromMap
	// pandas:
	//   pd.DataFrame({
	//     "name": ["Alice","Bob","Carol","Dave"],
	//     "dept": ["Eng","Sales","Eng","HR"],
	//     "salary": [95000, 72000, 88000, 61000],
	//     "years": [5, 3, 8, 2],
	//   })
	df, err := dataframe.FromMap(map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Carol", "Dave"},
		"dept":   []string{"Eng", "Sales", "Eng", "HR"},
		"salary": []int64{95000, 72000, 88000, 61000},
		"years":  []int64{5, 3, 8, 2},
	}, []string{"name", "dept", "salary", "years"})
	mustOk(err)

	fmt.Println("Full DataFrame:")
	fmt.Println(df)

	nRows, nCols := df.Shape()
	fmt.Printf("Shape: %d rows × %d columns\n\n", nRows, nCols)

	// Select specific columns: df[["name", "salary"]]
	subset, err := df.Select("name", "salary")
	mustOk(err)
	fmt.Println("Select [name, salary]:")
	fmt.Println(subset)

	// Access a single column: df["salary"]
	salary := df.MustCol("salary")
	fmt.Printf("Total salary budget: $%.0f\n", salary.Sum())
	fmt.Printf("Average salary: $%.2f\n", salary.Mean())
	fmt.Println()

	// Add a computed column: df["salary_k"] = df["salary"] / 1000
	// In pandas: df["salary_k"] = df["salary"] / 1000
	salaryK := salary.Apply(func(v types.Value) types.Value {
		f, _ := v.ToFloat64()
		return types.Float(f / 1000)
	}).Rename("salary_k")

	df2, err := df.WithColumn("salary_k", salaryK)
	mustOk(err)
	fmt.Println("With salary_k column:")
	fmt.Println(df2)

	// Sort by salary descending
	// pandas: df.sort_values("salary", ascending=False)
	sorted, err := df.SortBy("salary", false)
	mustOk(err)
	fmt.Println("Sorted by salary (desc):")
	fmt.Println(sorted)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 3: Filtering
// ─────────────────────────────────────────────────────────────────────────────

func example3_Filtering() {
	fmt.Println("━━━ Example 3: Filtering ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	df, err := dataframe.FromMap(map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Carol", "Dave", "Eve"},
		"dept":   []string{"Eng", "Sales", "Eng", "HR", "Eng"},
		"salary": []int64{95000, 72000, 88000, 61000, 105000},
	}, []string{"name", "dept", "salary"})
	mustOk(err)

	// Simple threshold filter
	// pandas: df[df["salary"] > 80000]
	highEarners, err := df.Filter(df.MustCol("salary").Gt(80000))
	mustOk(err)
	fmt.Println("Salary > 80k:")
	fmt.Println(highEarners)

	// String equality filter
	// pandas: df[df["dept"] == "Eng"]
	engineers, err := df.Filter(df.MustCol("dept").EqStr("Eng"))
	mustOk(err)
	fmt.Println("Department == Eng:")
	fmt.Println(engineers)

	// Multi-column filter using Query (more readable for complex conditions)
	// pandas: df[(df["dept"]=="Eng") & (df["salary"] > 90000)]
	seniorEngineers, err := df.Query(func(row map[string]types.Value) bool {
		dept, _ := row["dept"].AsString()
		salary, _ := row["salary"].AsInt()
		return dept == "Eng" && salary > 90000
	})
	mustOk(err)
	fmt.Println("Senior Engineers (Eng dept + salary > 90k):")
	fmt.Println(seniorEngineers)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 4: GroupBy aggregation
// ─────────────────────────────────────────────────────────────────────────────

func example4_GroupBy() {
	fmt.Println("━━━ Example 4: GroupBy ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	df, err := dataframe.FromMap(map[string]interface{}{
		"dept":   []string{"Eng", "Sales", "Eng", "HR", "Eng", "Sales"},
		"salary": []int64{95000, 72000, 88000, 61000, 105000, 68000},
		"years":  []int64{5, 3, 8, 2, 12, 4},
	}, []string{"dept", "salary", "years"})
	mustOk(err)

	// Group by department, compute mean salary and total years
	// pandas:
	//   df.groupby("dept").agg({
	//     "salary": "mean",
	//     "years": "sum",
	//   })
	grouped, err := df.GroupBy("dept", map[string]func(*series.Series) types.Value{
		"salary": func(s *series.Series) types.Value {
			return types.Float(math.Round(s.Mean()))
		},
		"years": func(s *series.Series) types.Value {
			return types.Float(s.Sum())
		},
	})
	mustOk(err)

	fmt.Println("Mean salary and total years by department:")
	fmt.Println(grouped)

	// Group by with count
	countByDept, err := df.GroupBy("dept", map[string]func(*series.Series) types.Value{
		"salary": func(s *series.Series) types.Value {
			return types.Int(int64(s.Count()))
		},
	})
	mustOk(err)
	fmt.Println("Headcount by department:")
	fmt.Println(countByDept)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 5: Join (Merge)
// ─────────────────────────────────────────────────────────────────────────────

func example5_Join() {
	fmt.Println("━━━ Example 5: Join/Merge ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Two DataFrames to join
	employees, err := dataframe.FromMap(map[string]interface{}{
		"emp_id":  []int64{1, 2, 3, 4},
		"name":    []string{"Alice", "Bob", "Carol", "Dave"},
		"dept_id": []int64{10, 20, 10, 30},
	}, []string{"emp_id", "name", "dept_id"})
	mustOk(err)

	departments, err := dataframe.FromMap(map[string]interface{}{
		"dept_id": []int64{10, 20, 40}, // dept 30 (HR) missing — tests outer join
		"dept":    []string{"Engineering", "Sales", "Marketing"},
	}, []string{"dept_id", "dept"})
	mustOk(err)

	fmt.Println("Employees:")
	fmt.Println(employees)
	fmt.Println("Departments:")
	fmt.Println(departments)

	// Inner join: only matching rows
	// pandas: pd.merge(employees, departments, on="dept_id", how="inner")
	inner, err := ops.Merge(employees, departments, "dept_id", &ops.MergeOptions{How: ops.InnerJoin})
	mustOk(err)
	fmt.Println("INNER JOIN (dept 30/HR has no match → Dave excluded):")
	fmt.Println(inner)

	// Left join: all employees, null dept for unmatched
	// pandas: pd.merge(employees, departments, on="dept_id", how="left")
	left, err := ops.Merge(employees, departments, "dept_id", &ops.MergeOptions{How: ops.LeftJoin})
	mustOk(err)
	fmt.Println("LEFT JOIN (Dave included with null dept):")
	fmt.Println(left)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 6: Null handling
// ─────────────────────────────────────────────────────────────────────────────

func example6_NullHandling() {
	fmt.Println("━━━ Example 6: Null Handling ━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Create a Series with some null values
	// pandas: pd.Series([1.0, np.nan, 3.0, np.nan, 5.0])
	s := series.New([]types.Value{
		types.Float(1.0),
		types.Null(),
		types.Float(3.0),
		types.Null(),
		types.Float(5.0),
	}, "data")

	fmt.Println("Original series:")
	fmt.Println(s)

	// Count nulls
	fmt.Printf("Null count: %d / %d\n", s.NullCount(), s.Len())

	// Drop nulls: series.dropna()
	fmt.Println("\nAfter dropna():")
	fmt.Println(s.DropNull())

	// Fill nulls with a value: series.fillna(0)
	fmt.Println("After fillna(0):")
	fmt.Println(s.FillNullFloat(0))

	// Fill nulls with mean: series.fillna(series.mean())
	fmt.Println("After fillna(mean):")
	fmt.Println(s.FillNullMean())

	// DataFrame null handling
	df, err := dataframe.FromMap(map[string]interface{}{
		"a": []types.Value{types.Int(1), types.Null(), types.Int(3)},
		"b": []types.Value{types.Str("x"), types.Str("y"), types.Null()},
	}, []string{"a", "b"})
	mustOk(err)

	fmt.Println("\nDataFrame with nulls:")
	fmt.Println(df)

	// Drop rows with any null
	fmt.Println("After dropna() (any null):")
	clean, err := df.DropNull()
	mustOk(err)
	fmt.Println(clean)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 7: Correlation matrix
// ─────────────────────────────────────────────────────────────────────────────

func example7_Statistics() {
	fmt.Println("━━━ Example 7: Statistics & Correlation ━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Simulated stock data
	df, err := dataframe.FromMap(map[string]interface{}{
		"AAPL":  []float64{150.0, 152.5, 149.0, 155.0, 158.0, 154.0},
		"GOOGL": []float64{2800.0, 2830.0, 2790.0, 2860.0, 2900.0, 2850.0},
		"MSFT":  []float64{300.0, 303.0, 298.0, 310.0, 315.0, 308.0},
	}, []string{"AAPL", "GOOGL", "MSFT"})
	mustOk(err)

	// Describe: df.describe()
	desc, err := df.Describe()
	mustOk(err)
	fmt.Println("Describe (summary statistics):")
	fmt.Println(desc)

	// Correlation matrix: df.corr()
	corr, err := df.Corr()
	mustOk(err)
	fmt.Println("Correlation matrix:")
	fmt.Println(corr)

	// Value counts on a categorical column
	categories := series.FromStrings([]string{"A", "B", "A", "C", "B", "A", "A"}, "grade")
	counts := categories.ValueCounts()
	fmt.Println("Value counts for grades:")
	for grade, count := range counts {
		fmt.Printf("  %s: %d\n", grade, count)
	}
	fmt.Println()

	// Unique values
	fmt.Println("Unique values:", categories.Unique())
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 8: CSV read and write
// ─────────────────────────────────────────────────────────────────────────────

func example8_CSV() {
	fmt.Println("━━━ Example 8: CSV Read & Write ━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Build a DataFrame to write
	df, err := dataframe.FromMap(map[string]interface{}{
		"name":   []string{"Alice", "Bob", "Carol", "Dave"},
		"dept":   []string{"Eng", "Sales", "Eng", "HR"},
		"salary": []int64{95000, 72000, 88000, 61000},
		"active": []bool{true, true, false, true},
	}, []string{"name", "dept", "salary", "active"})
	mustOk(err)

	// Write to a temp CSV file
	// pandas: df.to_csv("employees.csv", index=False)
	path := os.TempDir() + "/goframe_employees.csv"
	err = goio.WriteCSVFile(df, path, nil)
	mustOk(err)
	fmt.Printf("Written to %s\n\n", path)

	// Read it back with automatic type inference
	// pandas: pd.read_csv("employees.csv")
	loaded, err := goio.ReadCSVFile(path, nil)
	mustOk(err)

	fmt.Println("Loaded from CSV:")
	fmt.Println(loaded)

	nRows, nCols := loaded.Shape()
	fmt.Printf("Shape: %d rows × %d columns\n\n", nRows, nCols)

	// Read with custom options: tab delimiter and extra null strings
	// pandas: pd.read_csv("file.tsv", sep="\t", na_values=["-", "N/A"])
	tsvPath := os.TempDir() + "/goframe_employees.tsv"
	err = goio.WriteCSVFile(df, tsvPath, &goio.WriteCSVOptions{Delimiter: '\t', WriteHeader: true})
	mustOk(err)

	loaded2, err := goio.ReadCSVFile(tsvPath, &goio.ReadCSVOptions{
		Delimiter:  '\t',
		HasHeader:  true,
		NullValues: map[string]bool{"": true, "N/A": true, "-": true},
		InferTypes: true,
	})
	mustOk(err)

	fmt.Println("Loaded from TSV (tab-delimited):")
	fmt.Println(loaded2)
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 9: DateTime type
// ─────────────────────────────────────────────────────────────────────────────

func example9_DateTime() {
	fmt.Println("━━━ Example 9: DateTime ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Creating DateTime values manually
	// pandas: pd.Series(pd.to_datetime(["2024-01-01", "2024-06-15", "2024-12-31"]))
	dates := series.New([]types.Value{
		types.DateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		types.DateTime(time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)),
		types.DateTime(time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)),
	}, "event_time")

	fmt.Println("DateTime series:")
	fmt.Println(dates)

	// Accessing a DateTime value
	v := dates.ILoc(1)
	if ts, ok := v.AsDateTime(); ok {
		fmt.Printf("event_time[1]: %s (year=%d, month=%s)\n\n", ts.Format(time.RFC3339), ts.Year(), ts.Month())
	}

	// Build a DataFrame with timestamps and sort by time
	// pandas: df.sort_values("logged_at")
	df, err := dataframe.FromMap(map[string]interface{}{
		"user": []string{"carol", "alice", "bob"},
		"action": []string{"logout", "login", "login"},
		"logged_at": []types.Value{
			types.DateTime(time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC)),
			types.DateTime(time.Date(2024, 6, 15, 8, 0, 0, 0, time.UTC)),
			types.DateTime(time.Date(2024, 6, 15, 9, 30, 0, 0, time.UTC)),
		},
	}, []string{"user", "action", "logged_at"})
	mustOk(err)

	fmt.Println("Event log (unsorted):")
	fmt.Println(df)

	sorted, err := df.SortBy("logged_at", true)
	mustOk(err)
	fmt.Println("Sorted by logged_at (ascending):")
	fmt.Println(sorted)

	// CSV round-trip: datetime columns are written as RFC3339 and re-inferred on read
	path := os.TempDir() + "/goframe_events.csv"
	mustOk(goio.WriteCSVFile(df, path, nil))

	loaded, err := goio.ReadCSVFile(path, nil)
	mustOk(err)
	fmt.Println("Loaded from CSV (datetime re-inferred):")
	fmt.Println(loaded)

	// Confirm the re-loaded column is still KindDateTime
	v2 := loaded.MustCol("logged_at").ILoc(0)
	if _, ok := v2.AsDateTime(); ok {
		fmt.Printf("dtype: %s\n", v2.Kind)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────────────────────────────────────
// Example 10: Decimal type
// ─────────────────────────────────────────────────────────────────────────────

func example10_Decimal() {
	fmt.Println("━━━ Example 10: Decimal ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// 10a: Decimal exactness — the classic 0.1 + 0.2 problem
	a, _ := types.ParseDecimal("0.10")
	b, _ := types.ParseDecimal("0.20")
	fmt.Printf("float64:  0.1 + 0.2 = %.17f\n", 0.1+0.2)
	fmt.Printf("Decimal:  0.10 + 0.20 = %s  (exact)\n\n", a.Add(b))

	// 10b: Decimal Series — exact values, no float rounding
	prices := series.New([]types.Value{
		types.Dec(types.NewDecimal(9999, 2)),  // 99.99
		types.Dec(types.NewDecimal(1500, 2)),  // 15.00
		types.Dec(types.NewDecimal(25000, 2)), // 250.00
		types.Null(),                          // missing price
	}, "price")

	fmt.Println("Price series:")
	fmt.Println(prices)

	if d, ok := prices.ILoc(0).AsDecimal(); ok {
		fmt.Printf("price[0]: %s  (kind: %s)\n\n", d, prices.ILoc(0).Kind)
	}

	// 10c: Aggregations — all stat operations work via ToFloat64
	fmt.Println("Aggregations (nulls skipped automatically):")
	fmt.Printf("  count: %d\n", prices.Count())
	fmt.Printf("  sum:   %.2f\n", prices.Sum())
	fmt.Printf("  mean:  %.2f\n", prices.Mean())
	fmt.Printf("  min:   %.2f\n", prices.Min())
	fmt.Printf("  max:   %.2f\n", prices.Max())
	fmt.Printf("  std:   %.2f\n\n", prices.Std())

	// 10d: Filtering
	fmt.Println("Prices > 20.00:")
	fmt.Println(prices.Filter(prices.Gt(20.00)))

	// 10e: Sorting
	catalog, err := dataframe.FromMap(map[string]interface{}{
		"product": []string{"Keyboard", "Mouse", "Monitor"},
		"price": []types.Value{
			types.Dec(types.NewDecimal(7999, 2)),
			types.Dec(types.NewDecimal(2999, 2)),
			types.Dec(types.NewDecimal(39999, 2)),
		},
	}, []string{"product", "price"})
	mustOk(err)

	sorted, err := catalog.SortBy("price", true)
	mustOk(err)
	fmt.Println("Products sorted by price (ascending):")
	fmt.Println(sorted)

	// 10f: Describe — decimal columns included as numeric
	desc, err := catalog.Describe()
	mustOk(err)
	fmt.Println("Describe:")
	fmt.Println(desc)

	// 10g: GroupBy with decimal aggregation
	inventory, err := dataframe.FromMap(map[string]interface{}{
		"category": []string{"Electronics", "Electronics", "Accessories"},
		"price": []types.Value{
			types.Dec(types.NewDecimal(39999, 2)),
			types.Dec(types.NewDecimal(7999, 2)),
			types.Dec(types.NewDecimal(2999, 2)),
		},
	}, []string{"category", "price"})
	mustOk(err)

	grouped, err := inventory.GroupBy("category", map[string]func(*series.Series) types.Value{
		"price": func(s *series.Series) types.Value { return types.Float(s.Mean()) },
	})
	mustOk(err)
	fmt.Println("Average price by category:")
	fmt.Println(grouped)

	// 10h: CSV write — decimal serialized as plain string (e.g. "79.99")
	path := os.TempDir() + "/goframe_catalog.csv"
	mustOk(goio.WriteCSVFile(catalog, path, nil))
	loaded, err := goio.ReadCSVFile(path, nil)
	mustOk(err)
	v := loaded.MustCol("price").ILoc(0)
	fmt.Printf("CSV round-trip: dtype=%s  value=%s\n", v.Kind, v)
	fmt.Println()
}

// mustOk panics on error — for example simplicity, not production use!
func mustOk(err error) {
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}
}
