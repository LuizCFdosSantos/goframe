// Package series implements a 1-dimensional labeled array — goframe's Series.
//
// # What is a Series?
//
// pandas' Series is the fundamental building block: a 1D array of values with
// an associated Index (row labels). It's like a column in a spreadsheet that
// also knows the name of each row.
//
// A DataFrame is essentially a dict of Series (one per column) all sharing the
// same Index.
//
// # Series in goframe
//
// Our Series holds:
//   - col   types.Column   — typed columnar storage (IntColumn, FloatColumn, etc.)
//   - index *types.Index   — row labels (default: 0, 1, 2, ...)
//   - name  string         — column name (used when building DataFrames)
//
// # Immutability Principle
//
// Like pandas (mostly), operations on a Series return a NEW Series rather than
// modifying in place. This prevents subtle bugs from aliasing — two variables
// accidentally sharing the same underlying data.
package series

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/LuizCdosSantos/goframe/types"
)

// Series is a 1-dimensional labeled array capable of holding any goframe Value.
// Internally it uses typed columnar storage to minimize memory overhead.
type Series struct {
	col   types.Column
	index *types.Index
	name  string
}

// New creates a Series from a slice of Values.
//
// The index defaults to 0, 1, 2, ... (RangeIndex) — matching pandas:
//
//	pd.Series([1, 2, 3])  →  index is 0, 1, 2
//
// Internally, New detects the value type and uses typed columnar storage
// (e.g., []int64 for integer columns) instead of []types.Value.
func New(data []types.Value, name string) *Series {
	return &Series{
		col:   types.NewColumn(data),
		index: types.NewRangeIndex(len(data)),
		name:  name,
	}
}

// NewWithIndex creates a Series with custom row labels.
//
// Panics if len(data) != index.Len() — a Series must have exactly one label
// per data point. This matches pandas' behavior:
//
//	pd.Series([1,2], index=["a","b","c"])  →  ValueError
func NewWithIndex(data []types.Value, index *types.Index, name string) *Series {
	if len(data) != index.Len() {
		panic(fmt.Sprintf(
			"series: data length %d does not match index length %d",
			len(data), index.Len(),
		))
	}
	return &Series{col: types.NewColumn(data), index: index, name: name}
}

// FromInts is a convenience constructor for integer Series.
// Bypasses []Value boxing — the backing store is []int64 directly.
func FromInts(vals []int64, name string) *Series {
	return &Series{
		col:   types.NewIntColumn(vals),
		index: types.NewRangeIndex(len(vals)),
		name:  name,
	}
}

// FromFloats is a convenience constructor for float Series.
// Bypasses []Value boxing — the backing store is []float64 directly.
func FromFloats(vals []float64, name string) *Series {
	return &Series{
		col:   types.NewFloatColumn(vals),
		index: types.NewRangeIndex(len(vals)),
		name:  name,
	}
}

// FromStrings is a convenience constructor for string Series.
// Bypasses []Value boxing — the backing store is []string directly.
func FromStrings(vals []string, name string) *Series {
	return &Series{
		col:   types.NewStringColumn(vals),
		index: types.NewRangeIndex(len(vals)),
		name:  name,
	}
}

// --- Core accessors ---

// Len returns the number of elements. Equivalent to len(series) in pandas.
func (s *Series) Len() int { return s.col.Len() }

// Name returns the Series name (column name).
func (s *Series) Name() string { return s.name }

// Index returns the row-label index.
func (s *Series) Index() *types.Index { return s.index }

// ILoc returns the value at integer position i (0-based).
// Equivalent to series.iloc[i] in pandas.
//
// Negative indexing is supported: ILoc(-1) returns the last element.
// Panics on out-of-bounds — use Len() to guard if needed.
func (s *Series) ILoc(i int) types.Value {
	if i < 0 {
		i = s.col.Len() + i
	}
	if i < 0 || i >= s.col.Len() {
		panic(fmt.Sprintf("series iloc: index %d out of bounds for length %d", i, s.col.Len()))
	}
	return s.col.Get(i)
}

// Loc returns the value at the row with the given label.
// Equivalent to series.loc[label] in pandas.
//
// Returns an error if the label is not found or the index has duplicates.
func (s *Series) Loc(label types.Value) (types.Value, error) {
	pos, err := s.index.Locate(label)
	if err != nil {
		return types.Null(), fmt.Errorf("series loc: %w", err)
	}
	return s.col.Get(pos), nil
}

// Values returns a copy of the underlying data as a []types.Value slice.
// Equivalent to series.values in pandas (returns numpy array there).
func (s *Series) Values() []types.Value {
	n := s.col.Len()
	out := make([]types.Value, n)
	for i := 0; i < n; i++ {
		out[i] = s.col.Get(i)
	}
	return out
}

// Dtype returns the dominant type of the Series.
//
// For typed columns (IntColumn, FloatColumn, etc.) this is O(1).
// For mixed-type GenericColumns this scans all values.
func (s *Series) Dtype() types.Kind {
	return s.col.Dtype()
}

// --- Slicing ---

// ILocRange returns a new Series containing positions [start, end).
// Equivalent to series.iloc[start:end].
//
// Uses zero-copy slice sharing on the underlying typed column — no allocation
// for the data array. The index is sliced independently.
func (s *Series) ILocRange(start, end int) *Series {
	if start < 0 || end > s.col.Len() || start > end {
		panic(fmt.Sprintf("series iloc range [%d:%d] invalid for length %d", start, end, s.col.Len()))
	}
	return &Series{
		col:   s.col.Slice(start, end),
		index: s.index.Slice(start, end),
		name:  s.name,
	}
}

// Head returns the first n elements, like pandas' series.head(n).
func (s *Series) Head(n int) *Series {
	if n > s.col.Len() {
		n = s.col.Len()
	}
	return s.ILocRange(0, n)
}

// Tail returns the last n elements, like pandas' series.tail(n).
func (s *Series) Tail(n int) *Series {
	if n > s.col.Len() {
		n = s.col.Len()
	}
	return s.ILocRange(s.col.Len()-n, s.col.Len())
}

// --- Filtering ---

// Filter returns a new Series containing only elements where mask[i] is true.
// Equivalent to series[boolean_mask] in pandas.
//
// The mask must have the same length as the Series. This is the core primitive
// for conditional selection — everything from s[s > 5] to complex queries
// ultimately produces a boolean mask and calls Filter (or equivalent).
func (s *Series) Filter(mask *Series) *Series {
	if mask.Len() != s.Len() {
		panic(fmt.Sprintf("filter: mask length %d != series length %d", mask.Len(), s.Len()))
	}

	var resultData []types.Value
	var resultLabels []types.Value

	n := s.col.Len()
	for i := 0; i < n; i++ {
		boolVal, ok := mask.col.Get(i).AsBool()
		if !ok {
			panic(fmt.Sprintf("filter: mask element at position %d is not bool (got %s)", i, mask.col.Get(i).Kind))
		}
		if boolVal {
			resultData = append(resultData, s.col.Get(i))
			resultLabels = append(resultLabels, s.index.Label(i))
		}
	}

	if len(resultData) == 0 {
		return New(nil, s.name)
	}

	return NewWithIndex(resultData, types.NewIndex(resultLabels), s.name)
}

// Where is like Filter but keeps null for False positions (pandas' where() behavior).
// This preserves the original index alignment — useful for assignment.
func (s *Series) Where(mask *Series) *Series {
	if mask.Len() != s.Len() {
		panic("where: mask length must match series length")
	}
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		boolVal, _ := mask.col.Get(i).AsBool()
		if boolVal {
			result[i] = s.col.Get(i)
		} else {
			result[i] = types.Null()
		}
	}
	return NewWithIndex(result, s.index, s.name)
}

// --- Element-wise operations ---

// Map applies a function to every element and returns a new Series.
// This is the foundation for all element-wise transformations.
// Equivalent to series.apply(func) or series.map(func) in pandas.
func (s *Series) Map(fn func(types.Value) types.Value) *Series {
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		result[i] = fn(s.col.Get(i))
	}
	return NewWithIndex(result, s.index, s.name)
}

// Apply is an alias for Map that matches pandas' naming convention.
func (s *Series) Apply(fn func(types.Value) types.Value) *Series {
	return s.Map(fn)
}

// MapWithIndex applies a function to each (index_label, value) pair.
// Useful when the transformation depends on the row label.
func (s *Series) MapWithIndex(fn func(types.Value, types.Value) types.Value) *Series {
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		result[i] = fn(s.index.Label(i), s.col.Get(i))
	}
	return NewWithIndex(result, s.index, s.name)
}

// --- Arithmetic operations ---
// These implement element-wise math, mimicking pandas' operator overloading.
// In Python: s1 + s2 calls __add__; in Go, we use explicit method calls.
//
// All arithmetic operations skip null values (propagate null), matching
// pandas' default skipna behavior.

// Add returns element-wise sum of s and other.
func (s *Series) Add(other *Series) *Series {
	return s.binaryOp(other, func(a, b float64) float64 { return a + b }, "add")
}

// Sub returns element-wise difference (s - other).
func (s *Series) Sub(other *Series) *Series {
	return s.binaryOp(other, func(a, b float64) float64 { return a - b }, "sub")
}

// Mul returns element-wise product.
func (s *Series) Mul(other *Series) *Series {
	return s.binaryOp(other, func(a, b float64) float64 { return a * b }, "mul")
}

// Div returns element-wise division. Division by zero yields NaN (not a panic).
func (s *Series) Div(other *Series) *Series {
	return s.binaryOp(other, func(a, b float64) float64 { return a / b }, "div")
}

// AddScalar adds a constant to every element.
// Equivalent to series + 5 in pandas.
func (s *Series) AddScalar(v float64) *Series {
	return s.Map(func(val types.Value) types.Value {
		if val.IsNull() {
			return types.Null()
		}
		f, err := val.ToFloat64()
		if err != nil {
			return types.Null()
		}
		return types.Float(f + v)
	})
}

func (s *Series) binaryOp(other *Series, op func(float64, float64) float64, opName string) *Series {
	if s.Len() != other.Len() {
		panic(fmt.Sprintf("%s: series lengths differ (%d vs %d). "+
			"Hint: use Align() first if indexes differ.", opName, s.Len(), other.Len()))
	}
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		a, b := s.col.Get(i), other.col.Get(i)
		if a.IsNull() || b.IsNull() {
			result[i] = types.Null()
			continue
		}
		af, err1 := a.ToFloat64()
		bf, err2 := b.ToFloat64()
		if err1 != nil || err2 != nil {
			result[i] = types.Null()
			continue
		}
		result[i] = types.Float(op(af, bf))
	}
	return NewWithIndex(result, s.index, s.name)
}

// --- Comparison operations ---

// Gt returns a boolean Series where s[i] > threshold.
func (s *Series) Gt(threshold float64) *Series {
	return s.compareScalar(threshold, func(v, t float64) bool { return v > t })
}

// Lt returns a boolean Series where s[i] < threshold.
func (s *Series) Lt(threshold float64) *Series {
	return s.compareScalar(threshold, func(v, t float64) bool { return v < t })
}

// Gte returns a boolean Series where s[i] >= threshold.
func (s *Series) Gte(threshold float64) *Series {
	return s.compareScalar(threshold, func(v, t float64) bool { return v >= t })
}

// Lte returns a boolean Series where s[i] <= threshold.
func (s *Series) Lte(threshold float64) *Series {
	return s.compareScalar(threshold, func(v, t float64) bool { return v <= t })
}

// Eq returns a boolean Series where s[i] == threshold.
func (s *Series) Eq(threshold float64) *Series {
	return s.compareScalar(threshold, func(v, t float64) bool { return v == t })
}

// EqStr returns a boolean Series where s[i] == str (string comparison).
func (s *Series) EqStr(str string) *Series {
	// Fast path: avoid boxing for typed string columns.
	if sc, ok := s.col.(*types.StringColumn); ok {
		n := sc.Len()
		result := make([]types.Value, n)
		for i := 0; i < n; i++ {
			if sc.IsNull(i) {
				result[i] = types.Bool(false)
			} else {
				result[i] = types.Bool(sc.RawAt(i) == str)
			}
		}
		return NewWithIndex(result, s.index, s.name)
	}
	return s.Map(func(v types.Value) types.Value {
		if v.IsNull() {
			return types.Bool(false)
		}
		sv, ok := v.AsString()
		return types.Bool(ok && sv == str)
	})
}

func (s *Series) compareScalar(threshold float64, cmp func(float64, float64) bool) *Series {
	return s.Map(func(v types.Value) types.Value {
		if v.IsNull() {
			return types.Bool(false)
		}
		f, err := v.ToFloat64()
		if err != nil {
			return types.Bool(false)
		}
		return types.Bool(cmp(f, threshold))
	})
}

// IsNull returns a boolean Series indicating which elements are null.
// Equivalent to series.isna() or series.isnull() in pandas.
func (s *Series) IsNull() *Series {
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		result[i] = types.Bool(s.col.IsNull(i))
	}
	return NewWithIndex(result, s.index, s.name)
}

// IsNotNull returns the inverse of IsNull().
// Equivalent to series.notna() in pandas.
func (s *Series) IsNotNull() *Series {
	n := s.col.Len()
	result := make([]types.Value, n)
	for i := 0; i < n; i++ {
		result[i] = types.Bool(!s.col.IsNull(i))
	}
	return NewWithIndex(result, s.index, s.name)
}

// --- Aggregation functions ---
// These reduce a Series to a single scalar value.
// All aggregations skip null values by default (skipna=True in pandas).
// IntColumn and FloatColumn have typed fast paths that avoid per-element boxing.

// Sum returns the sum of all non-null numeric values.
// Returns math.NaN() if all values are null.
func (s *Series) Sum() float64 {
	switch col := s.col.(type) {
	case *types.IntColumn:
		total, count := col.SumInt()
		if count == 0 {
			return math.NaN()
		}
		return float64(total)
	case *types.FloatColumn:
		total, count := col.SumFloat()
		if count == 0 {
			return math.NaN()
		}
		return total
	default:
		var total float64
		count := 0
		for i := 0; i < s.col.Len(); i++ {
			f, err := s.col.Get(i).ToFloat64()
			if err != nil || math.IsNaN(f) {
				continue
			}
			total += f
			count++
		}
		if count == 0 {
			return math.NaN()
		}
		return total
	}
}

// Mean returns the arithmetic mean of non-null values.
// Equivalent to series.mean() in pandas.
func (s *Series) Mean() float64 {
	switch col := s.col.(type) {
	case *types.IntColumn:
		total, count := col.SumInt()
		if count == 0 {
			return math.NaN()
		}
		return float64(total) / float64(count)
	case *types.FloatColumn:
		total, count := col.SumFloat()
		if count == 0 {
			return math.NaN()
		}
		return total / float64(count)
	default:
		var total float64
		count := 0
		for i := 0; i < s.col.Len(); i++ {
			f, err := s.col.Get(i).ToFloat64()
			if err != nil || math.IsNaN(f) {
				continue
			}
			total += f
			count++
		}
		if count == 0 {
			return math.NaN()
		}
		return total / float64(count)
	}
}

// Std returns the sample standard deviation (ddof=1, matching pandas default).
func (s *Series) Std() float64 {
	mean := s.Mean()
	if math.IsNaN(mean) {
		return math.NaN()
	}

	var sumSq float64
	count := 0
	for i := 0; i < s.col.Len(); i++ {
		f, err := s.col.Get(i).ToFloat64()
		if err != nil || math.IsNaN(f) {
			continue
		}
		diff := f - mean
		sumSq += diff * diff
		count++
	}
	if count < 2 {
		return math.NaN()
	}
	return math.Sqrt(sumSq / float64(count-1))
}

// Min returns the smallest non-null value (as float64).
func (s *Series) Min() float64 {
	switch col := s.col.(type) {
	case *types.IntColumn:
		lo, _, count := col.MinMaxInt()
		if count == 0 {
			return math.NaN()
		}
		return float64(lo)
	case *types.FloatColumn:
		lo, _, count := col.MinMaxFloat()
		if count == 0 {
			return math.NaN()
		}
		return lo
	default:
		minVal := math.Inf(1)
		found := false
		for i := 0; i < s.col.Len(); i++ {
			f, err := s.col.Get(i).ToFloat64()
			if err != nil || math.IsNaN(f) {
				continue
			}
			if f < minVal {
				minVal = f
				found = true
			}
		}
		if !found {
			return math.NaN()
		}
		return minVal
	}
}

// Max returns the largest non-null value (as float64).
func (s *Series) Max() float64 {
	switch col := s.col.(type) {
	case *types.FloatColumn:
		_, hi, count := col.MinMaxFloat()
		if count == 0 {
			return math.NaN()
		}
		return hi
	case *types.IntColumn:
		_, hi, count := col.MinMaxInt()
		if count == 0 {
			return math.NaN()
		}
		return float64(hi)
	default:
		maxVal := math.Inf(-1)
		found := false
		for i := 0; i < s.col.Len(); i++ {
			f, err := s.col.Get(i).ToFloat64()
			if err != nil || math.IsNaN(f) {
				continue
			}
			if f > maxVal {
				maxVal = f
				found = true
			}
		}
		if !found {
			return math.NaN()
		}
		return maxVal
	}
}

// Count returns the number of non-null values.
// Equivalent to series.count() in pandas (NOT len(series)).
func (s *Series) Count() int {
	n := s.col.Len()
	count := 0
	for i := 0; i < n; i++ {
		if !s.col.IsNull(i) {
			count++
		}
	}
	return count
}

// NullCount returns the number of null values.
func (s *Series) NullCount() int {
	return s.Len() - s.Count()
}

// ValueCounts returns a map of each unique value to its frequency.
// Equivalent to series.value_counts() in pandas.
// Returns map[string]int for simplicity; key is Value.String().
func (s *Series) ValueCounts() map[string]int {
	counts := make(map[string]int)
	n := s.col.Len()
	for i := 0; i < n; i++ {
		v := s.col.Get(i)
		if !v.IsNull() {
			counts[v.String()]++
		}
	}
	return counts
}

// Unique returns a new Series with duplicate values removed.
// Order of first occurrence is preserved — matching pandas.
func (s *Series) Unique() *Series {
	seen := make(map[string]bool)
	var unique []types.Value
	n := s.col.Len()
	for i := 0; i < n; i++ {
		v := s.col.Get(i)
		key := v.String()
		if !seen[key] {
			seen[key] = true
			unique = append(unique, v)
		}
	}
	return New(unique, s.name)
}

// --- Null handling ---

// DropNull returns a new Series with all null values removed.
// Equivalent to series.dropna() in pandas.
// The index is updated to reflect the remaining rows.
func (s *Series) DropNull() *Series {
	mask := s.IsNotNull()
	return s.Filter(mask)
}

// FillNull replaces all null values with the given fill value.
// Equivalent to series.fillna(value) in pandas.
func (s *Series) FillNull(fill types.Value) *Series {
	return s.Map(func(v types.Value) types.Value {
		if v.IsNull() {
			return fill
		}
		return v
	})
}

// FillNullFloat is a convenience version of FillNull for numeric columns.
func (s *Series) FillNullFloat(fill float64) *Series {
	return s.FillNull(types.Float(fill))
}

// FillNullMean replaces nulls with the column mean — a common imputation strategy.
func (s *Series) FillNullMean() *Series {
	mean := s.Mean()
	return s.FillNullFloat(mean)
}

// --- Sorting ---

// SortValues returns a new Series sorted by value.
// ascending=true for A→Z / small→large (matches pandas' default).
// Nulls are sorted to the end (matching pandas' na_position='last' default).
func (s *Series) SortValues(ascending bool) *Series {
	type pair struct {
		val   types.Value
		label types.Value
	}
	n := s.col.Len()
	pairs := make([]pair, n)
	for i := 0; i < n; i++ {
		pairs[i] = pair{val: s.col.Get(i), label: s.index.Label(i)}
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

	sortedData := make([]types.Value, n)
	sortedLabels := make([]types.Value, n)
	for i, p := range pairs {
		sortedData[i] = p.val
		sortedLabels[i] = p.label
	}
	return NewWithIndex(sortedData, types.NewIndex(sortedLabels), s.name)
}

// --- Renaming ---

// Rename returns a new Series with a different name.
// The column and index are shared — no copy needed since they're immutable.
func (s *Series) Rename(newName string) *Series {
	return &Series{
		col:   s.col,
		index: s.index,
		name:  newName,
	}
}

// --- Display ---

// String returns a pandas-like string representation for debugging.
func (s *Series) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Name: %s, dtype: %s\n", s.name, s.Dtype())
	n := s.col.Len()
	for i := 0; i < n; i++ {
		label := s.index.Label(i)
		v := s.col.Get(i)
		fmt.Fprintf(&sb, "%-6s %s\n", label.String(), v.String())
	}
	return sb.String()
}

// Describe returns a summary statistics Series, like pandas' series.describe().
// Returns a new Series with index labels: count, mean, std, min, max.
func (s *Series) Describe() *Series {
	stats := []types.Value{
		types.Float(float64(s.Count())),
		types.Float(s.Mean()),
		types.Float(s.Std()),
		types.Float(s.Min()),
		types.Float(s.Max()),
	}
	idx := types.NewStringIndex([]string{"count", "mean", "std", "min", "max"})
	return NewWithIndex(stats, idx, s.name)
}
