package dataframe_test

import (
	"math"
	"testing"

	"github.com/LuizCFdosSantos/goframe/dataframe"
	"github.com/LuizCFdosSantos/goframe/series"
	"github.com/LuizCFdosSantos/goframe/types"
)

// helpers

func mustDF(t *testing.T, cols map[string]*series.Series, order []string) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols, order)
	if err != nil {
		t.Fatalf("dataframe.New: %v", err)
	}
	return df
}

func mustFromMap(t *testing.T, data map[string]interface{}, order []string) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.FromMap(data, order)
	if err != nil {
		t.Fatalf("dataframe.FromMap: %v", err)
	}
	return df
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────────────

func TestNew_Empty(t *testing.T) {
	df, err := dataframe.New(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Errorf("empty DataFrame shape = (%d,%d), want (0,0)", rows, cols)
	}
}

func TestNew_ColumnLengthMismatch(t *testing.T) {
	cols := map[string]*series.Series{
		"a": series.FromInts([]int64{1, 2, 3}, "a"),
		"b": series.FromInts([]int64{1, 2}, "b"),
	}
	_, err := dataframe.New(cols, nil)
	if err == nil {
		t.Error("expected error for mismatched column lengths")
	}
}

func TestNew_ColOrderValidation(t *testing.T) {
	cols := map[string]*series.Series{
		"a": series.FromInts([]int64{1}, "a"),
	}
	_, err := dataframe.New(cols, []string{"a", "nonexistent"})
	if err == nil {
		t.Error("expected error for colOrder referencing missing column")
	}
}

func TestNew_AlphabeticalDefault(t *testing.T) {
	cols := map[string]*series.Series{
		"z": series.FromInts([]int64{1}, "z"),
		"a": series.FromInts([]int64{2}, "a"),
		"m": series.FromInts([]int64{3}, "m"),
	}
	df, err := dataframe.New(cols, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	order := df.Columns()
	if order[0] != "a" || order[1] != "m" || order[2] != "z" {
		t.Errorf("expected alphabetical order [a,m,z], got %v", order)
	}
}

func TestFromMap_Ints(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []int64{1, 2, 3},
	}, []string{"x"})
	if df.Len() != 3 {
		t.Errorf("Len() = %d, want 3", df.Len())
	}
}

func TestFromMap_Floats(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"f": []float64{1.1, 2.2},
	}, []string{"f"})
	if df.Len() != 2 {
		t.Errorf("Len() = %d, want 2", df.Len())
	}
}

func TestFromMap_Strings(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"s": []string{"a", "b"},
	}, []string{"s"})
	if df.Len() != 2 {
		t.Errorf("Len() = %d, want 2", df.Len())
	}
}

func TestFromMap_Bools(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"b": []bool{true, false},
	}, []string{"b"})
	if df.Len() != 2 {
		t.Errorf("Len() = %d, want 2", df.Len())
	}
}

func TestFromMap_Values(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"v": []types.Value{types.Int(1), types.Null()},
	}, []string{"v"})
	if df.Len() != 2 {
		t.Errorf("Len() = %d, want 2", df.Len())
	}
}

func TestFromMap_UnsupportedType(t *testing.T) {
	_, err := dataframe.FromMap(map[string]interface{}{
		"bad": []complex128{1 + 2i},
	}, nil)
	if err == nil {
		t.Error("expected error for unsupported column type")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Core accessors
// ─────────────────────────────────────────────────────────────────────────────

func TestShape(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2, 3},
		"b": []int64{4, 5, 6},
	}, []string{"a", "b"})
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Errorf("Shape() = (%d,%d), want (3,2)", rows, cols)
	}
}

func TestCol_Found(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []int64{10, 20},
	}, []string{"x"})
	s, err := df.Col("x")
	if err != nil || s == nil {
		t.Errorf("Col('x') returned (%v, %v)", s, err)
	}
}

func TestCol_NotFound(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []int64{1},
	}, []string{"x"})
	_, err := df.Col("missing")
	if err == nil {
		t.Error("expected error for missing column")
	}
}

func TestMustCol_Panics(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []int64{1},
	}, []string{"x"})
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustCol for missing column should panic")
		}
	}()
	df.MustCol("missing")
}

func TestHasColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1},
	}, []string{"a"})
	if !df.HasColumn("a") {
		t.Error("HasColumn('a') should be true")
	}
	if df.HasColumn("z") {
		t.Error("HasColumn('z') should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Column selection
// ─────────────────────────────────────────────────────────────────────────────

func TestSelect(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
		"c": []int64{5, 6},
	}, []string{"a", "b", "c"})
	sel, err := df.Select("a", "c")
	if err != nil || sel == nil {
		t.Fatalf("Select error: %v", err)
	}
	if !sel.HasColumn("a") || !sel.HasColumn("c") || sel.HasColumn("b") {
		t.Error("Select should keep only a and c")
	}
}

func TestSelect_MissingColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.Select("missing")
	if err == nil {
		t.Error("Select with missing column should return error")
	}
}

func TestDrop(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1},
		"b": []int64{2},
	}, []string{"a", "b"})
	dropped, err := df.Drop("b")
	if err != nil || dropped.HasColumn("b") || !dropped.HasColumn("a") {
		t.Errorf("Drop('b') failed: err=%v, cols=%v", err, dropped.Columns())
	}
}

func TestDrop_MissingColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.Drop("missing")
	if err == nil {
		t.Error("Drop with missing column should return error")
	}
}

func TestWithColumn_New(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	newCol := series.FromInts([]int64{10, 20}, "b")
	df2, err := df.WithColumn("b", newCol)
	if err != nil || !df2.HasColumn("b") {
		t.Errorf("WithColumn failed: %v", err)
	}
}

func TestWithColumn_Replace(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	replacement := series.FromInts([]int64{99, 98}, "a")
	df2, err := df.WithColumn("a", replacement)
	if err != nil {
		t.Fatalf("WithColumn replace: %v", err)
	}
	val := df2.MustCol("a").ILoc(0)
	if n, _ := val.AsInt(); n != 99 {
		t.Errorf("WithColumn replace: expected 99, got %v", val)
	}
}

func TestWithColumn_LengthMismatch(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	wrongLen := series.FromInts([]int64{1, 2, 3}, "b")
	_, err := df.WithColumn("b", wrongLen)
	if err == nil {
		t.Error("expected error for length mismatch in WithColumn")
	}
}

func TestRename(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"old": []int64{1, 2}}, []string{"old"})
	df2, err := df.Rename(map[string]string{"old": "new"})
	if err != nil || !df2.HasColumn("new") || df2.HasColumn("old") {
		t.Errorf("Rename failed: err=%v, cols=%v", err, df2.Columns())
	}
}

func TestRename_MissingColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.Rename(map[string]string{"missing": "new"})
	if err == nil {
		t.Error("Rename with missing column should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Row selection
// ─────────────────────────────────────────────────────────────────────────────

func TestILoc(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{10, 20, 30},
	}, []string{"a"})
	row := df.ILoc(1)
	v := row["a"]
	if n, _ := v.AsInt(); n != 20 {
		t.Errorf("ILoc(1)['a'] = %v, want 20", v)
	}
}

func TestILocRange(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2, 3, 4},
	}, []string{"a"})
	sub, err := df.ILocRange(1, 3)
	if err != nil || sub.Len() != 2 {
		t.Errorf("ILocRange(1,3): err=%v, len=%d", err, sub.Len())
	}
	if n, _ := sub.MustCol("a").ILoc(0).AsInt(); n != 2 {
		t.Errorf("ILocRange(1,3)[0] = %v, want 2", sub.MustCol("a").ILoc(0))
	}
}

func TestHead(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2, 3, 4, 5},
	}, []string{"a"})
	h, err := df.Head(3)
	if err != nil || h.Len() != 3 {
		t.Errorf("Head(3): err=%v, len=%d", err, h.Len())
	}
}

func TestHead_LargerThanDF(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	h, err := df.Head(100)
	if err != nil || h.Len() != 2 {
		t.Errorf("Head(100) on 2-row DF: err=%v, len=%d", err, h.Len())
	}
}

func TestTail(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2, 3, 4, 5},
	}, []string{"a"})
	tail, err := df.Tail(2)
	if err != nil || tail.Len() != 2 {
		t.Errorf("Tail(2): err=%v, len=%d", err, tail.Len())
	}
	if n, _ := tail.MustCol("a").ILoc(0).AsInt(); n != 4 {
		t.Errorf("Tail(2)[0] = %v, want 4", tail.MustCol("a").ILoc(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Filtering
// ─────────────────────────────────────────────────────────────────────────────

func TestFilter(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"v": []int64{1, 2, 3, 4},
	}, []string{"v"})
	mask := df.MustCol("v").Gt(2)
	filtered, err := df.Filter(mask)
	if err != nil || filtered.Len() != 2 {
		t.Errorf("Filter(>2): err=%v, len=%d", err, filtered.Len())
	}
}

func TestQuery(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"price": []float64{50.0, 150.0, 200.0},
		"qty":   []int64{1, 2, 3},
	}, []string{"price", "qty"})
	result, err := df.Query(func(row map[string]types.Value) bool {
		p, _ := row["price"].AsFloat()
		return p > 100.0
	})
	if err != nil || result.Len() != 2 {
		t.Errorf("Query(price>100): err=%v, len=%d", err, result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorting
// ─────────────────────────────────────────────────────────────────────────────

func TestSortBy_Ascending(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"v": []int64{3, 1, 2},
	}, []string{"v"})
	sorted, err := df.SortBy("v", true)
	if err != nil {
		t.Fatalf("SortBy: %v", err)
	}
	if n, _ := sorted.MustCol("v").ILoc(0).AsInt(); n != 1 {
		t.Errorf("SortBy ascending: first = %v, want 1", sorted.MustCol("v").ILoc(0))
	}
}

func TestSortBy_Descending(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"v": []int64{3, 1, 2},
	}, []string{"v"})
	sorted, err := df.SortBy("v", false)
	if err != nil {
		t.Fatalf("SortBy desc: %v", err)
	}
	if n, _ := sorted.MustCol("v").ILoc(0).AsInt(); n != 3 {
		t.Errorf("SortBy descending: first = %v, want 3", sorted.MustCol("v").ILoc(0))
	}
}

func TestSortBy_NullsLast(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"v": []types.Value{types.Int(2), types.Null(), types.Int(1)},
	}, []string{"v"})
	sorted, err := df.SortBy("v", true)
	if err != nil {
		t.Fatalf("SortBy with nulls: %v", err)
	}
	if !sorted.MustCol("v").ILoc(-1).IsNull() {
		t.Error("nulls should sort last")
	}
}

func TestSortBy_MissingColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.SortBy("missing", true)
	if err == nil {
		t.Error("SortBy with missing column should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// GroupBy
// ─────────────────────────────────────────────────────────────────────────────

func TestGroupBy_Sum(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"cat":   []string{"a", "b", "a", "b"},
		"value": []float64{1.0, 2.0, 3.0, 4.0},
	}, []string{"cat", "value"})

	result, err := df.GroupBy("cat", map[string]func(*series.Series) types.Value{
		"value": func(s *series.Series) types.Value { return types.Float(s.Sum()) },
	})
	if err != nil {
		t.Fatalf("GroupBy: %v", err)
	}
	if result.Len() != 2 {
		t.Errorf("GroupBy result rows = %d, want 2", result.Len())
	}
}

func TestGroupBy_NullKeySkipped(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"cat":   []types.Value{types.Str("a"), types.Null(), types.Str("a")},
		"value": []float64{1.0, 2.0, 3.0},
	}, []string{"cat", "value"})

	result, err := df.GroupBy("cat", map[string]func(*series.Series) types.Value{
		"value": func(s *series.Series) types.Value { return types.Float(s.Sum()) },
	})
	if err != nil {
		t.Fatalf("GroupBy with null key: %v", err)
	}
	if result.Len() != 1 {
		t.Errorf("null keys should be skipped; got %d groups", result.Len())
	}
}

func TestGroupBy_MissingGroupColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.GroupBy("missing", nil)
	if err == nil {
		t.Error("GroupBy with missing column should return error")
	}
}

func TestGroupBy_MissingAggColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"cat": []string{"a"},
	}, []string{"cat"})
	_, err := df.GroupBy("cat", map[string]func(*series.Series) types.Value{
		"missing": func(s *series.Series) types.Value { return types.Null() },
	})
	if err == nil {
		t.Error("GroupBy with missing agg column should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Null handling
// ─────────────────────────────────────────────────────────────────────────────

func TestDropNull_AllCols(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []types.Value{types.Int(1), types.Null(), types.Int(3)},
		"b": []types.Value{types.Int(4), types.Int(5), types.Null()},
	}, []string{"a", "b"})
	clean, err := df.DropNull()
	if err != nil || clean.Len() != 1 {
		t.Errorf("DropNull(): err=%v, len=%d, want 1", err, clean.Len())
	}
}

func TestDropNull_SpecificCols(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []types.Value{types.Int(1), types.Null()},
		"b": []types.Value{types.Null(), types.Int(2)},
	}, []string{"a", "b"})
	clean, err := df.DropNull("a")
	if err != nil || clean.Len() != 1 {
		t.Errorf("DropNull('a'): err=%v, len=%d, want 1", err, clean.Len())
	}
}

func TestDropNull_MissingColumn(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := df.DropNull("missing")
	if err == nil {
		t.Error("DropNull with missing column should return error")
	}
}

func TestFillNull(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []types.Value{types.Int(1), types.Null()},
	}, []string{"a"})
	filled, err := df.FillNull(types.Int(0))
	if err != nil {
		t.Fatalf("FillNull: %v", err)
	}
	if filled.MustCol("a").ILoc(1).IsNull() {
		t.Error("FillNull should replace null with 0")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregations
// ─────────────────────────────────────────────────────────────────────────────

func TestDescribe(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []float64{1.0, 2.0, 3.0},
	}, []string{"x"})
	desc, err := df.Describe()
	if err != nil || desc == nil {
		t.Fatalf("Describe: %v", err)
	}
	rows, _ := desc.Shape()
	if rows != 5 {
		t.Errorf("Describe rows = %d, want 5 (count,mean,std,min,max)", rows)
	}
}

func TestDescribe_NoNumericCols(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"name": []string{"alice", "bob"},
	}, []string{"name"})
	_, err := df.Describe()
	if err == nil {
		t.Error("Describe with no numeric columns should return error")
	}
}

func TestApply(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []float64{1.0, 2.0},
		"b": []float64{3.0, 4.0},
	}, []string{"a", "b"})
	sums := df.Apply(func(s *series.Series) types.Value {
		return types.Float(s.Sum())
	}, "sums")
	if sums.Len() != 2 {
		t.Errorf("Apply result len = %d, want 2", sums.Len())
	}
}

func TestCorr(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x": []float64{1.0, 2.0, 3.0},
		"y": []float64{2.0, 4.0, 6.0},
	}, []string{"x", "y"})
	corr, err := df.Corr()
	if err != nil {
		t.Fatalf("Corr: %v", err)
	}
	// x and y are perfectly correlated
	xy, _ := corr.MustCol("x").ILoc(1).AsFloat() // corr[x,y]
	if math.Abs(xy-1.0) > 1e-6 {
		t.Errorf("Corr(x,y) = %f, want 1.0 (perfect correlation)", xy)
	}
}

func TestCorr_TooFewNumericCols(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"x":    []float64{1.0, 2.0},
		"name": []string{"a", "b"},
	}, []string{"x", "name"})
	_, err := df.Corr()
	if err == nil {
		t.Error("Corr with fewer than 2 numeric columns should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Display
// ─────────────────────────────────────────────────────────────────────────────

func TestString_NonEmpty(t *testing.T) {
	df := mustFromMap(t, map[string]interface{}{
		"a": []int64{1, 2},
	}, []string{"a"})
	s := df.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestString_TruncatesLargeDF(t *testing.T) {
	vals := make([]int64, 30)
	for i := range vals {
		vals[i] = int64(i)
	}
	df := mustFromMap(t, map[string]interface{}{"v": vals}, []string{"v"})
	s := df.String()
	if s == "" {
		t.Error("String() for large DataFrame should not be empty")
	}
}
