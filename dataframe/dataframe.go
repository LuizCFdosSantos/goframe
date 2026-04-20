// Package dataframe implements goframe's DataFrame — a 2D labeled data structure.
//
// # What is a DataFrame?
//
// A DataFrame is a table: rows × columns. Each column is a Series with a name.
// All columns share the same row Index. This is exactly pandas' DataFrame.
//
// Internally we store:
//   - columns  map[string]*series.Series  — column name → Series
//   - colOrder []string                   — preserves insertion order
//   - index    *types.Index               — shared row labels
//
// # Why not just a [][]Value 2D slice?
//
// Columnar storage (one Series per column) is how real data processing works:
//   - Column selection (df["price"]) is O(1) — just a map lookup
//   - Aggregations on a column don't touch other columns' memory
//   - Adding a column is cheap — add one entry to the map
//   - Consistent with pandas' internal columnar storage
//
// Row-based storage ([][]Value, like a CSV in memory) would make row operations
// fast but column operations expensive — the opposite of analytics workloads.
//
// # Index Consistency Invariant
//
// All columns in a DataFrame must have the same length and use compatible
// indexes. We enforce this at construction and mutation time.
package dataframe

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/LuizCdosSantos/goframe/series"
	"github.com/LuizCdosSantos/goframe/types"
)

// DataFrame is a 2-dimensional labeled data structure.
type DataFrame struct {
	columns  map[string]*series.Series
	colOrder []string     // insertion-ordered column names
	index    *types.Index // shared row index
}

// New creates a DataFrame from a map of column name → Series.
//
// All Series must have the same length. Column order in the output
// follows the order of the colOrder slice — pass nil to sort alphabetically.
//
// Example:
//
//	df, err := dataframe.New(map[string]*series.Series{
//	    "name":  series.FromStrings([]string{"Alice", "Bob"}, "name"),
//	    "score": series.FromInts([]int64{90, 85}, "score"),
//	}, []string{"name", "score"})
func New(cols map[string]*series.Series, colOrder []string) (*DataFrame, error) {
	if len(cols) == 0 {
		return &DataFrame{
			columns:  make(map[string]*series.Series),
			colOrder: nil,
			index:    types.NewRangeIndex(0),
		}, nil
	}

	// Validate all columns have the same length
	var expectedLen int
	var expectedName string
	first := true
	for name, s := range cols {
		if first {
			expectedLen = s.Len()
			expectedName = name
			first = false
		} else if s.Len() != expectedLen {
			return nil, fmt.Errorf(
				"column %q has length %d but column %q has length %d; all columns must be the same length",
				name, s.Len(), expectedName, expectedLen,
			)
		}
	}

	// Determine column order
	order := colOrder
	if order == nil {
		// Default: alphabetical sort (deterministic output)
		order = make([]string, 0, len(cols))
		for name := range cols {
			order = append(order, name)
		}
		sort.Strings(order)
	} else {
		// Validate that all provided names exist
		for _, name := range order {
			if _, ok := cols[name]; !ok {
				return nil, fmt.Errorf("colOrder contains %q but that column was not provided", name)
			}
		}
	}

	// Use the first column's index as the shared index.
	// In a production library, we'd validate all indexes are compatible.
	var sharedIndex *types.Index
	for _, s := range cols {
		sharedIndex = s.Index()
		break
	}

	// Copy columns map to prevent external mutation
	copiedCols := make(map[string]*series.Series, len(cols))
	for k, v := range cols {
		copiedCols[k] = v
	}

	return &DataFrame{
		columns:  copiedCols,
		colOrder: order,
		index:    sharedIndex,
	}, nil
}

// FromMap is a convenience constructor accepting raw Go slices.
// Useful for quick DataFrame creation in tests and examples.
//
// The data map values must be []int64, []float64, []string, or []bool.
// Returns an error for unsupported types.
func FromMap(data map[string]interface{}, colOrder []string) (*DataFrame, error) {
	cols := make(map[string]*series.Series, len(data))
	for name, raw := range data {
		var s *series.Series
		switch v := raw.(type) {
		case []int64:
			s = series.FromInts(v, name)
		case []float64:
			s = series.FromFloats(v, name)
		case []string:
			s = series.FromStrings(v, name)
		case []bool:
			vals := make([]types.Value, len(v))
			for i, b := range v {
				vals[i] = types.Bool(b)
			}
			s = series.New(vals, name)
		case []types.Value:
			s = series.New(v, name)
		default:
			return nil, fmt.Errorf("column %q: unsupported type %T; use []int64, []float64, []string, or []bool", name, raw)
		}
		cols[name] = s
	}
	return New(cols, colOrder)
}

// --- Core accessors ---

// Shape returns (nRows, nCols) — equivalent to df.shape in pandas.
func (df *DataFrame) Shape() (int, int) {
	if len(df.columns) == 0 {
		return 0, 0
	}
	// Get length from first column
	for _, s := range df.columns {
		return s.Len(), len(df.columns)
	}
	return 0, 0
}

// Columns returns the column names in order.
func (df *DataFrame) Columns() []string {
	out := make([]string, len(df.colOrder))
	copy(out, df.colOrder)
	return out
}

// Index returns the shared row index.
func (df *DataFrame) Index() *types.Index { return df.index }

// Len returns the number of rows.
func (df *DataFrame) Len() int {
	nRows, _ := df.Shape()
	return nRows
}

// Col returns the Series for a column by name.
// Returns an error if the column doesn't exist.
// Equivalent to df["colname"] in pandas.
func (df *DataFrame) Col(name string) (*series.Series, error) {
	s, ok := df.columns[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found; available: %v", name, df.colOrder)
	}
	return s, nil
}

// MustCol is like Col but panics on error — use only when you know the column exists.
func (df *DataFrame) MustCol(name string) *series.Series {
	s, err := df.Col(name)
	if err != nil {
		panic(err)
	}
	return s
}

// HasColumn returns true if a column with the given name exists.
func (df *DataFrame) HasColumn(name string) bool {
	_, ok := df.columns[name]
	return ok
}

// --- Column selection ---

// Select returns a new DataFrame containing only the specified columns.
// Equivalent to df[["a", "b"]] in pandas.
// Preserves the order given in `names`.
func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	cols := make(map[string]*series.Series, len(names))
	for _, name := range names {
		s, err := df.Col(name)
		if err != nil {
			return nil, err
		}
		cols[name] = s
	}
	return New(cols, names)
}

// Drop returns a new DataFrame with the specified columns removed.
// Equivalent to df.drop(columns=[...]) in pandas.
func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	toRemove := make(map[string]bool, len(names))
	for _, n := range names {
		if !df.HasColumn(n) {
			return nil, fmt.Errorf("drop: column %q not found", n)
		}
		toRemove[n] = true
	}

	var newOrder []string
	newCols := make(map[string]*series.Series)
	for _, name := range df.colOrder {
		if !toRemove[name] {
			newOrder = append(newOrder, name)
			newCols[name] = df.columns[name]
		}
	}
	return New(newCols, newOrder)
}

// WithColumn returns a new DataFrame with an added or replaced column.
// Equivalent to df["new_col"] = series in pandas (but immutable — returns new DF).
//
// If the column already exists, it is replaced. If it's new, it's appended.
// The new Series must have the same length as the DataFrame.
func (df *DataFrame) WithColumn(name string, s *series.Series) (*DataFrame, error) {
	nRows, _ := df.Shape()
	if nRows > 0 && s.Len() != nRows {
		return nil, fmt.Errorf(
			"WithColumn: new column %q has length %d but DataFrame has %d rows",
			name, s.Len(), nRows,
		)
	}

	newCols := make(map[string]*series.Series, len(df.columns)+1)
	for k, v := range df.columns {
		newCols[k] = v
	}
	newCols[name] = s.Rename(name) // ensure column name matches key

	// If replacing existing column, keep the same order; if new, append
	var newOrder []string
	existed := false
	for _, existing := range df.colOrder {
		newOrder = append(newOrder, existing)
		if existing == name {
			existed = true
		}
	}
	if !existed {
		newOrder = append(newOrder, name)
	}

	return New(newCols, newOrder)
}

// Rename renames columns. The `mapping` maps old name → new name.
// Equivalent to df.rename(columns={...}) in pandas.
func (df *DataFrame) Rename(mapping map[string]string) (*DataFrame, error) {
	for oldName := range mapping {
		if !df.HasColumn(oldName) {
			return nil, fmt.Errorf("rename: column %q not found", oldName)
		}
	}

	newCols := make(map[string]*series.Series, len(df.columns))
	newOrder := make([]string, len(df.colOrder))
	for i, name := range df.colOrder {
		if newName, ok := mapping[name]; ok {
			newCols[newName] = df.columns[name].Rename(newName)
			newOrder[i] = newName
		} else {
			newCols[name] = df.columns[name]
			newOrder[i] = name
		}
	}
	return New(newCols, newOrder)
}

// --- Row selection ---

// ILoc returns the row at integer position i as a map[string]types.Value.
// Equivalent to df.iloc[i] in pandas.
func (df *DataFrame) ILoc(i int) map[string]types.Value {
	row := make(map[string]types.Value, len(df.columns))
	for _, name := range df.colOrder {
		row[name] = df.columns[name].ILoc(i)
	}
	return row
}

// ILocRange returns a new DataFrame with rows [start, end).
// Equivalent to df.iloc[start:end] in pandas.
func (df *DataFrame) ILocRange(start, end int) (*DataFrame, error) {
	newCols := make(map[string]*series.Series, len(df.columns))
	for _, name := range df.colOrder {
		newCols[name] = df.columns[name].ILocRange(start, end)
	}
	return New(newCols, df.colOrder)
}

// Head returns the first n rows. Equivalent to df.head(n).
func (df *DataFrame) Head(n int) (*DataFrame, error) {
	nRows := df.Len()
	if n > nRows {
		n = nRows
	}
	return df.ILocRange(0, n)
}

// Tail returns the last n rows. Equivalent to df.tail(n).
func (df *DataFrame) Tail(n int) (*DataFrame, error) {
	nRows := df.Len()
	if n > nRows {
		n = nRows
	}
	return df.ILocRange(nRows-n, nRows)
}

// --- Filtering ---

// Filter returns a new DataFrame keeping only rows where mask[i] == true.
// Equivalent to df[boolean_mask] in pandas.
//
// Example:
//
//	mask := df.MustCol("price").Gt(100)
//	cheap := df.Filter(mask)
func (df *DataFrame) Filter(mask *series.Series) (*DataFrame, error) {
	newCols := make(map[string]*series.Series, len(df.columns))
	for _, name := range df.colOrder {
		newCols[name] = df.columns[name].Filter(mask)
	}
	return New(newCols, df.colOrder)
}

// Query is a higher-level filter that takes a predicate function over rows.
// The predicate receives a map[colName]Value for each row.
// This is less efficient than Filter (can't vectorize) but more readable for
// multi-column conditions.
//
// Example:
//
//	result, _ := df.Query(func(row map[string]types.Value) bool {
//	    price, _ := row["price"].AsFloat()
//	    qty, _ := row["qty"].AsInt()
//	    return price > 100 && qty > 5
//	})
func (df *DataFrame) Query(predicate func(map[string]types.Value) bool) (*DataFrame, error) {
	n := df.Len()
	maskVals := make([]types.Value, n)
	for i := 0; i < n; i++ {
		row := df.ILoc(i)
		maskVals[i] = types.Bool(predicate(row))
	}
	mask := series.New(maskVals, "_mask")
	return df.Filter(mask)
}

// --- Sorting ---

// SortBy returns a new DataFrame sorted by the given column.
// ascending=true for smallest-first (default in pandas).
func (df *DataFrame) SortBy(colName string, ascending bool) (*DataFrame, error) {
	col, err := df.Col(colName)
	if err != nil {
		return nil, err
	}

	// Build (value, original_position) pairs for sorting
	n := df.Len()
	type pair struct {
		val types.Value
		pos int
	}
	pairs := make([]pair, n)
	for i := 0; i < n; i++ {
		pairs[i] = pair{val: col.ILoc(i), pos: i}
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		a, b := pairs[i].val, pairs[j].val
		if a.IsNull() {
			return false
		}
		if b.IsNull() {
			return true
		}
		lt := a.LessThan(b)
		if ascending {
			return lt
		}
		return !lt && !a.Equal(b)
	})

	// Reorder all columns according to the new order
	newCols := make(map[string]*series.Series, len(df.columns))
	for _, name := range df.colOrder {
		srcSeries := df.columns[name]
		newVals := make([]types.Value, n)
		newLabels := make([]types.Value, n)
		for newPos, p := range pairs {
			newVals[newPos] = srcSeries.ILoc(p.pos)
			newLabels[newPos] = df.index.Label(p.pos)
		}
		newCols[name] = series.NewWithIndex(newVals, types.NewIndex(newLabels), name)
	}
	return New(newCols, df.colOrder)
}

// --- GroupBy and Aggregation ---

// GroupBy groups the DataFrame by unique values of a column and applies
// an aggregation function to each group.
//
// This is a simplified version of pandas' df.groupby("col").agg(func).
//
// Returns a new DataFrame with one row per unique group value.
// The group key column is always the first column in the result.
//
// Example — average price per category:
//
//	result, _ := df.GroupBy("category", map[string]func(*series.Series) types.Value{
//	    "price": func(s *series.Series) types.Value { return types.Float(s.Mean()) },
//	    "qty":   func(s *series.Series) types.Value { return types.Float(s.Sum()) },
//	})
func (df *DataFrame) GroupBy(
	groupCol string,
	aggs map[string]func(*series.Series) types.Value,
) (*DataFrame, error) {

	keyCol, err := df.Col(groupCol)
	if err != nil {
		return nil, err
	}

	// Phase 1: Collect row indices for each group.
	// We use an ordered approach: first occurrence of a key determines order.
	// This matches pandas' sort=False behavior for predictable ordering.
	type group struct {
		key     types.Value
		indices []int
	}
	var groupOrder []string
	groups := make(map[string]*group)

	for i := 0; i < df.Len(); i++ {
		key := keyCol.ILoc(i)
		if key.IsNull() {
			continue // skip null keys, like pandas dropna=True (default)
		}
		keyStr := key.String()
		if _, exists := groups[keyStr]; !exists {
			groupOrder = append(groupOrder, keyStr)
			groups[keyStr] = &group{key: key}
		}
		groups[keyStr].indices = append(groups[keyStr].indices, i)
	}

	// Phase 2: For each group, extract a sub-Series for each aggregated column
	// and apply the aggregation function.
	nGroups := len(groupOrder)

	// Build the group key column
	keyVals := make([]types.Value, nGroups)
	for i, keyStr := range groupOrder {
		keyVals[i] = groups[keyStr].key
	}

	resultCols := map[string]*series.Series{
		groupCol: series.New(keyVals, groupCol),
	}
	resultOrder := []string{groupCol}

	for aggColName, aggFn := range aggs {
		if aggColName == groupCol {
			continue // skip re-aggregating the group key
		}
		srcCol, err := df.Col(aggColName)
		if err != nil {
			return nil, fmt.Errorf("groupby: agg column %q: %w", aggColName, err)
		}

		aggVals := make([]types.Value, nGroups)
		for i, keyStr := range groupOrder {
			g := groups[keyStr]
			// Extract the subset of srcCol belonging to this group
			subVals := make([]types.Value, len(g.indices))
			for j, idx := range g.indices {
				subVals[j] = srcCol.ILoc(idx)
			}
			subSeries := series.New(subVals, aggColName)
			aggVals[i] = aggFn(subSeries)
		}
		resultCols[aggColName] = series.New(aggVals, aggColName)
		resultOrder = append(resultOrder, aggColName)
	}

	return New(resultCols, resultOrder)
}

// --- Null handling ---

// DropNull removes rows where any of the specified columns contain null.
// If cols is empty, checks ALL columns — matching pandas' df.dropna() default.
func (df *DataFrame) DropNull(cols ...string) (*DataFrame, error) {
	checkCols := cols
	if len(checkCols) == 0 {
		checkCols = df.colOrder
	}

	n := df.Len()
	maskVals := make([]types.Value, n)
	for i := range maskVals {
		maskVals[i] = types.Bool(true) // assume row is valid
	}

	for _, colName := range checkCols {
		col, err := df.Col(colName)
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			if col.ILoc(i).IsNull() {
				maskVals[i] = types.Bool(false)
			}
		}
	}

	mask := series.New(maskVals, "_dropna_mask")
	return df.Filter(mask)
}

// FillNull replaces null values in all columns with the given value.
// For per-column control, use df.WithColumn(name, col.FillNull(v)).
func (df *DataFrame) FillNull(fill types.Value) (*DataFrame, error) {
	newCols := make(map[string]*series.Series, len(df.columns))
	for _, name := range df.colOrder {
		newCols[name] = df.columns[name].FillNull(fill)
	}
	return New(newCols, df.colOrder)
}

// --- Aggregations on entire DataFrame ---

// Describe returns summary statistics for all numeric columns.
// Equivalent to df.describe() in pandas.
func (df *DataFrame) Describe() (*DataFrame, error) {
	statNames := []string{"count", "mean", "std", "min", "max"}
	resultCols := make(map[string]*series.Series)
	var numericCols []string

	for _, name := range df.colOrder {
		col := df.columns[name]
		dtype := col.Dtype()
		if dtype == types.KindInt || dtype == types.KindFloat {
			stats := []types.Value{
				types.Float(float64(col.Count())),
				types.Float(col.Mean()),
				types.Float(col.Std()),
				types.Float(col.Min()),
				types.Float(col.Max()),
			}
			resultCols[name] = series.NewWithIndex(
				stats,
				types.NewStringIndex(statNames),
				name,
			)
			numericCols = append(numericCols, name)
		}
	}

	if len(numericCols) == 0 {
		return nil, fmt.Errorf("describe: no numeric columns found")
	}

	return New(resultCols, numericCols)
}

// Apply applies a function to each column and returns a Series of results.
// Equivalent to df.apply(func, axis=0) in pandas (column-wise application).
//
// Example — compute column sums:
//
//	sums := df.Apply(func(s *series.Series) types.Value {
//	    return types.Float(s.Sum())
//	}, "sums")
func (df *DataFrame) Apply(fn func(*series.Series) types.Value, resultName string) *series.Series {
	vals := make([]types.Value, len(df.colOrder))
	labels := make([]types.Value, len(df.colOrder))
	for i, name := range df.colOrder {
		vals[i] = fn(df.columns[name])
		labels[i] = types.Str(name)
	}
	return series.NewWithIndex(vals, types.NewIndex(labels), resultName)
}

// Corr computes pairwise Pearson correlation between all numeric columns.
// Returns a square DataFrame (correlation matrix), like df.corr() in pandas.
//
// Pearson correlation formula:
//
//	r = Σ((x-mean_x)(y-mean_y)) / (n * std_x * std_y)
//
// Returns values in [-1, 1] where 1=perfect positive, -1=perfect negative, 0=uncorrelated.
func (df *DataFrame) Corr() (*DataFrame, error) {
	// Collect numeric columns
	var numCols []string
	for _, name := range df.colOrder {
		d := df.columns[name].Dtype()
		if d == types.KindInt || d == types.KindFloat {
			numCols = append(numCols, name)
		}
	}
	if len(numCols) < 2 {
		return nil, fmt.Errorf("corr: need at least 2 numeric columns")
	}

	// Precompute means and std for efficiency (avoid recomputing per pair)
	means := make(map[string]float64)
	stds := make(map[string]float64)
	for _, name := range numCols {
		means[name] = df.columns[name].Mean()
		stds[name] = df.columns[name].Std()
	}

	n := df.Len()
	resultCols := make(map[string]*series.Series)
	for _, colA := range numCols {
		corrVals := make([]types.Value, len(numCols))
		for j, colB := range numCols {
			if colA == colB {
				corrVals[j] = types.Float(1.0) // self-correlation is always 1
				continue
			}
			// Compute Pearson r
			var cov float64
			count := 0
			for i := 0; i < n; i++ {
				av := df.columns[colA].ILoc(i)
				bv := df.columns[colB].ILoc(i)
				af, err1 := av.ToFloat64()
				bf, err2 := bv.ToFloat64()
				if err1 != nil || err2 != nil || math.IsNaN(af) || math.IsNaN(bf) {
					continue
				}
				cov += (af - means[colA]) * (bf - means[colB])
				count++
			}
			if count < 2 || stds[colA] == 0 || stds[colB] == 0 {
				corrVals[j] = types.Float(math.NaN())
			} else {
				// Divide by (count-1)*std_a*std_b to get Pearson r
				r := cov / (float64(count-1) * stds[colA] * stds[colB])
				corrVals[j] = types.Float(r)
			}
		}
		resultCols[colA] = series.NewWithIndex(
			corrVals,
			types.NewStringIndex(numCols),
			colA,
		)
	}
	return New(resultCols, numCols)
}

// --- Display ---

// String returns a human-readable table view of the DataFrame.
// Truncates to 20 rows and 8 columns for readability.
func (df *DataFrame) String() string {
	const maxRows = 20
	const maxCols = 8
	const colWidth = 12

	nRows, nCols := df.Shape()
	var sb strings.Builder

	fmt.Fprintf(&sb, "DataFrame: %d rows × %d columns\n", nRows, nCols)

	// Column headers
	fmt.Fprintf(&sb, "%-*s", colWidth, "")
	displayCols := df.colOrder
	if len(displayCols) > maxCols {
		displayCols = displayCols[:maxCols]
	}
	for _, name := range displayCols {
		if len(name) > colWidth-1 {
			name = name[:colWidth-2] + "…"
		}
		fmt.Fprintf(&sb, "%-*s", colWidth, name)
	}
	if nCols > maxCols {
		fmt.Fprintf(&sb, "  (+%d more)", nCols-maxCols)
	}
	sb.WriteString("\n")

	// Separator
	sb.WriteString(strings.Repeat("─", colWidth+len(displayCols)*colWidth))
	sb.WriteString("\n")

	// Rows
	displayRows := nRows
	if displayRows > maxRows {
		displayRows = maxRows
	}
	for i := 0; i < displayRows; i++ {
		label := df.index.Label(i).String()
		fmt.Fprintf(&sb, "%-*s", colWidth, label)
		for _, name := range displayCols {
			val := df.columns[name].ILoc(i).String()
			if len(val) > colWidth-1 {
				val = val[:colWidth-2] + "…"
			}
			fmt.Fprintf(&sb, "%-*s", colWidth, val)
		}
		sb.WriteString("\n")
	}
	if nRows > maxRows {
		fmt.Fprintf(&sb, "... (%d more rows)\n", nRows-maxRows)
	}

	return sb.String()
}
