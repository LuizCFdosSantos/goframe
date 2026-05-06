package ops_test

import (
	"testing"

	"github.com/LuizCFdosSantos/goframe/dataframe"
	"github.com/LuizCFdosSantos/goframe/ops"
	"github.com/LuizCFdosSantos/goframe/series"
	"github.com/LuizCFdosSantos/goframe/types"
)

// helpers

func makeDF(t *testing.T, data map[string]interface{}, order []string) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.FromMap(data, order)
	if err != nil {
		t.Fatalf("FromMap: %v", err)
	}
	return df
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — InnerJoin
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_Inner_Basic(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 2, 3},
		"val": []string{"a", "b", "c"},
	}, []string{"id", "val"})

	right := makeDF(t, map[string]interface{}{
		"id":    []int64{2, 3, 4},
		"label": []string{"x", "y", "z"},
	}, []string{"id", "label"})

	result, err := ops.Merge(left, right, "id", nil)
	if err != nil {
		t.Fatalf("Merge inner: %v", err)
	}
	// Only keys 2 and 3 exist in both
	if result.Len() != 2 {
		t.Errorf("inner join rows = %d, want 2", result.Len())
	}
	if !result.HasColumn("id") || !result.HasColumn("val") || !result.HasColumn("label") {
		t.Errorf("unexpected columns: %v", result.Columns())
	}
}

func TestMerge_Inner_NoMatches(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id": []int64{1, 2},
	}, []string{"id"})
	right := makeDF(t, map[string]interface{}{
		"id": []int64{3, 4},
	}, []string{"id"})
	result, err := ops.Merge(left, right, "id", nil)
	if err != nil {
		t.Fatalf("Merge no matches: %v", err)
	}
	if result.Len() != 0 {
		t.Errorf("inner join with no matches: rows = %d, want 0", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — LeftJoin
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_Left(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 2, 3},
		"val": []string{"a", "b", "c"},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{2, 3},
		"label": []string{"x", "y"},
	}, []string{"id", "label"})

	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.LeftJoin})
	if err != nil {
		t.Fatalf("Merge left: %v", err)
	}
	// All 3 left rows should appear
	if result.Len() != 3 {
		t.Errorf("left join rows = %d, want 3", result.Len())
	}
	// Row with id=1 should have null label
	labelCol := result.MustCol("label")
	if !labelCol.ILoc(0).IsNull() {
		t.Errorf("left join unmatched row: label should be null, got %v", labelCol.ILoc(0))
	}
}

func TestMerge_Left_NullKey(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []types.Value{types.Int(1), types.Null()},
		"val": []string{"a", "b"},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{1},
		"label": []string{"x"},
	}, []string{"id", "label"})

	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.LeftJoin})
	if err != nil {
		t.Fatalf("Merge left null key: %v", err)
	}
	// null key row should still appear in left join (with null on right side)
	if result.Len() != 2 {
		t.Errorf("left join with null key: rows = %d, want 2", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — RightJoin
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_Right(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 2},
		"val": []string{"a", "b"},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{2, 3},
		"label": []string{"x", "y"},
	}, []string{"id", "label"})

	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.RightJoin})
	if err != nil {
		t.Fatalf("Merge right: %v", err)
	}
	// All 2 right rows should appear
	if result.Len() != 2 {
		t.Errorf("right join rows = %d, want 2", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — OuterJoin
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_Outer(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 2},
		"val": []string{"a", "b"},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{2, 3},
		"label": []string{"x", "y"},
	}, []string{"id", "label"})

	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.OuterJoin})
	if err != nil {
		t.Fatalf("Merge outer: %v", err)
	}
	// ids: 1 (left-only), 2 (both), 3 (right-only)
	if result.Len() != 3 {
		t.Errorf("outer join rows = %d, want 3", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — column conflicts & suffixes
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_ColumnConflict_DefaultSuffixes(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":    []int64{1, 2},
		"score": []int64{10, 20},
	}, []string{"id", "score"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{1, 2},
		"score": []int64{100, 200},
	}, []string{"id", "score"})

	result, err := ops.Merge(left, right, "id", nil)
	if err != nil {
		t.Fatalf("Merge conflict: %v", err)
	}
	if !result.HasColumn("score_left") || !result.HasColumn("score_right") {
		t.Errorf("expected suffixed columns, got: %v", result.Columns())
	}
}

func TestMerge_ColumnConflict_CustomSuffixes(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1},
		"val": []int64{10},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":  []int64{1},
		"val": []int64{20},
	}, []string{"id", "val"})

	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{
		LeftSuffix:  "_L",
		RightSuffix: "_R",
	})
	if err != nil {
		t.Fatalf("Merge custom suffixes: %v", err)
	}
	if !result.HasColumn("val_L") || !result.HasColumn("val_R") {
		t.Errorf("expected _L/_R suffixed columns, got: %v", result.Columns())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — error cases
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_MissingLeftKey(t *testing.T) {
	left := makeDF(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	right := makeDF(t, map[string]interface{}{"id": []int64{1}}, []string{"id"})
	_, err := ops.Merge(left, right, "id", nil)
	if err == nil {
		t.Error("Merge with missing left key should return error")
	}
}

func TestMerge_MissingRightKey(t *testing.T) {
	left := makeDF(t, map[string]interface{}{"id": []int64{1}}, []string{"id"})
	right := makeDF(t, map[string]interface{}{"a": []int64{1}}, []string{"a"})
	_, err := ops.Merge(left, right, "id", nil)
	if err == nil {
		t.Error("Merge with missing right key should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — one-to-many
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_OneToMany(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id": []int64{1},
	}, []string{"id"})
	right := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 1, 1},
		"val": []int64{10, 20, 30},
	}, []string{"id", "val"})

	result, err := ops.Merge(left, right, "id", nil)
	if err != nil {
		t.Fatalf("Merge one-to-many: %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("one-to-many join rows = %d, want 3", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge — null keys in right DF
// ─────────────────────────────────────────────────────────────────────────────

func TestMerge_NullKeyInRight_OuterJoin(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id": []int64{1},
	}, []string{"id"})
	right := makeDF(t, map[string]interface{}{
		"id": []types.Value{types.Null()},
	}, []string{"id"})
	// null keys in right DF are skipped — they never match
	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.OuterJoin})
	if err != nil {
		t.Fatalf("Merge null right key outer: %v", err)
	}
	// left row (id=1) unmatched + right null row unmatched = 2 rows
	if result.Len() != 2 {
		t.Errorf("outer join rows = %d, want 2", result.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Concat
// ─────────────────────────────────────────────────────────────────────────────

func TestConcat_Empty(t *testing.T) {
	result, err := ops.Concat(nil, false)
	if err != nil || result == nil {
		t.Fatalf("Concat nil: err=%v, result=%v", err, result)
	}
	if result.Len() != 0 {
		t.Errorf("Concat nil rows = %d, want 0", result.Len())
	}
}

func TestConcat_Single(t *testing.T) {
	df := makeDF(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	result, err := ops.Concat([]*dataframe.DataFrame{df}, false)
	if err != nil || result.Len() != 2 {
		t.Errorf("Concat single: err=%v, rows=%d", err, result.Len())
	}
}

func TestConcat_Multiple(t *testing.T) {
	df1 := makeDF(t, map[string]interface{}{"a": []int64{1, 2}}, []string{"a"})
	df2 := makeDF(t, map[string]interface{}{"a": []int64{3, 4, 5}}, []string{"a"})
	result, err := ops.Concat([]*dataframe.DataFrame{df1, df2}, false)
	if err != nil || result.Len() != 5 {
		t.Errorf("Concat two dfs: err=%v, rows=%d, want 5", err, result.Len())
	}
}

func TestConcat_MissingColsError(t *testing.T) {
	df1 := makeDF(t, map[string]interface{}{
		"a": []int64{1},
		"b": []int64{2},
	}, []string{"a", "b"})
	df2 := makeDF(t, map[string]interface{}{
		"a": []int64{3},
	}, []string{"a"})
	_, err := ops.Concat([]*dataframe.DataFrame{df1, df2}, false)
	if err == nil {
		t.Error("Concat with missing columns and allowMissingCols=false should return error")
	}
}

func TestConcat_AllowMissingCols(t *testing.T) {
	df1 := makeDF(t, map[string]interface{}{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
	}, []string{"a", "b"})
	df2 := makeDF(t, map[string]interface{}{
		"a": []int64{5},
	}, []string{"a"})
	result, err := ops.Concat([]*dataframe.DataFrame{df1, df2}, true)
	if err != nil {
		t.Fatalf("Concat allow missing cols: %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("rows = %d, want 3", result.Len())
	}
	// "b" column for df2 rows should be null
	if !result.MustCol("b").ILoc(2).IsNull() {
		t.Errorf("missing column cell should be null, got %v", result.MustCol("b").ILoc(2))
	}
}

func TestConcat_PreservesColumnOrder(t *testing.T) {
	df1 := makeDF(t, map[string]interface{}{
		"x": []int64{1},
		"y": []int64{2},
	}, []string{"x", "y"})
	df2 := makeDF(t, map[string]interface{}{
		"x": []int64{3},
		"y": []int64{4},
	}, []string{"x", "y"})
	result, err := ops.Concat([]*dataframe.DataFrame{df1, df2}, false)
	if err != nil {
		t.Fatalf("Concat preserve order: %v", err)
	}
	cols := result.Columns()
	if cols[0] != "x" || cols[1] != "y" {
		t.Errorf("column order = %v, want [x, y]", cols)
	}
}

func TestConcat_ThreeDFs(t *testing.T) {
	mkSeries := func(vals []int64) *dataframe.DataFrame {
		return makeDF(t, map[string]interface{}{"v": vals}, []string{"v"})
	}
	result, err := ops.Concat([]*dataframe.DataFrame{
		mkSeries([]int64{1}),
		mkSeries([]int64{2}),
		mkSeries([]int64{3}),
	}, false)
	if err != nil || result.Len() != 3 {
		t.Errorf("Concat three: err=%v, rows=%d", err, result.Len())
	}
}

func TestMerge_RightJoin_NullKeyInRight(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1},
		"val": []string{"a"},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []types.Value{types.Null(), types.Int(1)},
		"label": []string{"x", "y"},
	}, []string{"id", "label"})
	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.RightJoin})
	if err != nil {
		t.Fatalf("right join null right key: %v", err)
	}
	// id=1 matches; null key in right is unmatched → 2 rows
	if result.Len() != 2 {
		t.Errorf("right join rows = %d, want 2", result.Len())
	}
}

func TestMerge_OuterJoin_NullKeyLeft(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id": []types.Value{types.Null(), types.Int(1)},
	}, []string{"id"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{1, 2},
		"label": []string{"a", "b"},
	}, []string{"id", "label"})
	result, err := ops.Merge(left, right, "id", &ops.MergeOptions{How: ops.OuterJoin})
	if err != nil {
		t.Fatalf("outer join null left key: %v", err)
	}
	// null left key (unmatched) + id=1 (matched) + id=2 (unmatched right) = 3
	if result.Len() != 3 {
		t.Errorf("outer join rows = %d, want 3", result.Len())
	}
}

func TestMerge_Inner_PreservesValues(t *testing.T) {
	left := makeDF(t, map[string]interface{}{
		"id":  []int64{1, 2},
		"val": []int64{10, 20},
	}, []string{"id", "val"})
	right := makeDF(t, map[string]interface{}{
		"id":    []int64{1, 2},
		"extra": []int64{100, 200},
	}, []string{"id", "extra"})

	result, err := ops.Merge(left, right, "id", nil)
	if err != nil {
		t.Fatalf("Merge preserve values: %v", err)
	}
	v := result.MustCol("val").ILoc(0)
	if n, _ := v.AsInt(); n != 10 {
		t.Errorf("val[0] = %v, want 10", v)
	}
	e := result.MustCol("extra").ILoc(0)
	if n, _ := e.AsInt(); n != 100 {
		t.Errorf("extra[0] = %v, want 100", e)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Series — additional coverage tests
// ─────────────────────────────────────────────────────────────────────────────

func TestSeries_HeadTail(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3, 4, 5}, "x")
	h := s.Head(3)
	if h.Len() != 3 {
		t.Errorf("Head(3) len = %d, want 3", h.Len())
	}
	tail := s.Tail(2)
	if tail.Len() != 2 {
		t.Errorf("Tail(2) len = %d, want 2", tail.Len())
	}
}

func TestSeries_Sum(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	if s.Sum() != 6.0 {
		t.Errorf("Sum() = %f, want 6", s.Sum())
	}
}

func TestSeries_MinMax(t *testing.T) {
	s := series.FromFloats([]float64{3.0, 1.0, 2.0}, "x")
	if s.Min() != 1.0 {
		t.Errorf("Min() = %f, want 1.0", s.Min())
	}
	if s.Max() != 3.0 {
		t.Errorf("Max() = %f, want 3.0", s.Max())
	}
}

func TestSeries_NullCount(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null(), types.Null()}, "x")
	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %d, want 2", s.NullCount())
	}
}

func TestSeries_IsNull_IsNotNull(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null()}, "x")
	nullMask := s.IsNull()
	if nullMask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("IsNull()[0] should be false for non-null")
	}
	if !nullMask.ILoc(1).Equal(types.Bool(true)) {
		t.Error("IsNull()[1] should be true for null")
	}
	notNull := s.IsNotNull()
	if !notNull.ILoc(0).Equal(types.Bool(true)) {
		t.Error("IsNotNull()[0] should be true")
	}
}

func TestSeries_Eq_EqStr(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a"}, "x")
	mask := s.EqStr("a")
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("EqStr('a')[0] should be true")
	}
	if !mask.ILoc(1).Equal(types.Bool(false)) {
		t.Error("EqStr('a')[1] should be false")
	}
}

func TestSeries_Arithmetic(t *testing.T) {
	a := series.FromFloats([]float64{1.0, 2.0}, "a")
	b := series.FromFloats([]float64{3.0, 4.0}, "b")
	sub := a.Sub(b)
	if f, _ := sub.ILoc(0).AsFloat(); f != -2.0 {
		t.Errorf("Sub()[0] = %f, want -2.0", f)
	}
	mul := a.Mul(b)
	if f, _ := mul.ILoc(0).AsFloat(); f != 3.0 {
		t.Errorf("Mul()[0] = %f, want 3.0", f)
	}
	div := b.Div(a)
	if f, _ := div.ILoc(0).AsFloat(); f != 3.0 {
		t.Errorf("Div()[0] = %f, want 3.0", f)
	}
	added := a.AddScalar(10.0)
	if f, _ := added.ILoc(0).AsFloat(); f != 11.0 {
		t.Errorf("AddScalar(10)[0] = %f, want 11.0", f)
	}
}

func TestSeries_Comparisons(t *testing.T) {
	s := series.FromInts([]int64{1, 5, 3}, "x")
	gt := s.Gt(3)
	if !gt.ILoc(1).Equal(types.Bool(true)) {
		t.Error("Gt(3)[1] should be true (5>3)")
	}
	lt := s.Lt(3)
	if !lt.ILoc(0).Equal(types.Bool(true)) {
		t.Error("Lt(3)[0] should be true (1<3)")
	}
	gte := s.Gte(3)
	if !gte.ILoc(2).Equal(types.Bool(true)) {
		t.Error("Gte(3)[2] should be true (3>=3)")
	}
	lte := s.Lte(3)
	if !lte.ILoc(0).Equal(types.Bool(true)) {
		t.Error("Lte(3)[0] should be true (1<=3)")
	}
	eq := s.Eq(5)
	if !eq.ILoc(1).Equal(types.Bool(true)) {
		t.Error("Eq(5)[1] should be true")
	}
}

func TestSeries_Where(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	mask := s.Gt(1)
	result := s.Where(mask)
	if !result.ILoc(0).IsNull() {
		t.Error("Where: value not matching mask should be null")
	}
	if n, _ := result.ILoc(1).AsInt(); n != 2 {
		t.Errorf("Where: value matching mask should be 2, got %v", result.ILoc(1))
	}
}

func TestSeries_Map(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	doubled := s.Map(func(v types.Value) types.Value {
		if n, ok := v.AsInt(); ok {
			return types.Int(n * 2)
		}
		return v
	})
	if n, _ := doubled.ILoc(0).AsInt(); n != 2 {
		t.Errorf("Map double[0] = %v, want 2", doubled.ILoc(0))
	}
}

func TestSeries_Apply(t *testing.T) {
	s := series.FromInts([]int64{1, 2}, "x")
	result := s.Apply(func(v types.Value) types.Value {
		return types.Int(0)
	})
	if n, _ := result.ILoc(0).AsInt(); n != 0 {
		t.Errorf("Apply: expected 0, got %v", result.ILoc(0))
	}
}

func TestSeries_MapWithIndex(t *testing.T) {
	s := series.FromInts([]int64{10, 20}, "x")
	// MapWithIndex signature is fn(label, value) — label is the index label
	result := s.MapWithIndex(func(label, v types.Value) types.Value {
		return v
	})
	if n, _ := result.ILoc(0).AsInt(); n != 10 {
		t.Errorf("MapWithIndex[0] = %v, want 10", result.ILoc(0))
	}
}

func TestSeries_ValueCounts(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a", "a"}, "x")
	counts := s.ValueCounts()
	if counts["a"] != 3 {
		t.Errorf("ValueCounts['a'] = %d, want 3", counts["a"])
	}
	if counts["b"] != 1 {
		t.Errorf("ValueCounts['b'] = %d, want 1", counts["b"])
	}
}

func TestSeries_Unique(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a", "c"}, "x")
	u := s.Unique()
	if u.Len() != 3 {
		t.Errorf("Unique() len = %d, want 3", u.Len())
	}
}

func TestSeries_Describe(t *testing.T) {
	s := series.FromFloats([]float64{1.0, 2.0, 3.0}, "x")
	desc := s.Describe()
	if desc == nil {
		t.Error("Describe() should not return nil")
	}
}

func TestSeries_FillNull(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null()}, "x")
	filled := s.FillNull(types.Int(0))
	if n, _ := filled.ILoc(1).AsInt(); n != 0 {
		t.Errorf("FillNull: expected 0, got %v", filled.ILoc(1))
	}
}

func TestSeries_FillNullFloat(t *testing.T) {
	s := series.New([]types.Value{types.Float(1.0), types.Null()}, "x")
	filled := s.FillNullFloat(99.0)
	if f, _ := filled.ILoc(1).AsFloat(); f != 99.0 {
		t.Errorf("FillNullFloat: expected 99.0, got %v", filled.ILoc(1))
	}
}

func TestSeries_SortDescending(t *testing.T) {
	s := series.FromInts([]int64{1, 3, 2}, "x")
	sorted := s.SortValues(false)
	if n, _ := sorted.ILoc(0).AsInt(); n != 3 {
		t.Errorf("SortValues desc: first = %v, want 3", sorted.ILoc(0))
	}
}

func TestSeries_Rename(t *testing.T) {
	s := series.FromInts([]int64{1}, "old")
	r := s.Rename("new")
	if r.Name() != "new" {
		t.Errorf("Rename: got %q, want 'new'", r.Name())
	}
}

func TestSeries_Dtype(t *testing.T) {
	s := series.FromInts([]int64{1, 2}, "x")
	if s.Dtype() != types.KindInt {
		t.Errorf("Dtype() = %v, want KindInt", s.Dtype())
	}
}

func TestSeries_Dtype_Mixed(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Str("a")}, "x")
	if s.Dtype() != types.KindString {
		t.Errorf("Dtype() for mixed = %v, want KindString", s.Dtype())
	}
}

func TestSeries_String(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	str := s.String()
	if str == "" {
		t.Error("Series.String() should not be empty")
	}
}

func TestSeries_Values(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	vals := s.Values()
	if len(vals) != 3 {
		t.Errorf("Values() len = %d, want 3", len(vals))
	}
	// Mutating returned slice should not affect series
	vals[0] = types.Int(999)
	if n, _ := s.ILoc(0).AsInt(); n != 1 {
		t.Error("Values() should return a copy")
	}
}

func TestSeries_ILocRange(t *testing.T) {
	s := series.FromInts([]int64{10, 20, 30, 40}, "x")
	sub := s.ILocRange(1, 3)
	if sub.Len() != 2 {
		t.Errorf("ILocRange(1,3) len = %d, want 2", sub.Len())
	}
	if n, _ := sub.ILoc(0).AsInt(); n != 20 {
		t.Errorf("ILocRange[0] = %v, want 20", sub.ILoc(0))
	}
}

func TestSeries_Loc(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b", "c"})
	s := series.NewWithIndex([]types.Value{types.Int(1), types.Int(2), types.Int(3)}, idx, "x")
	v, err := s.Loc(types.Str("b"))
	if err != nil {
		t.Fatalf("Loc('b'): %v", err)
	}
	if n, _ := v.AsInt(); n != 2 {
		t.Errorf("Loc('b') = %v, want 2", v)
	}
}

func TestSeries_FromStrings(t *testing.T) {
	s := series.FromStrings([]string{"x", "y"}, "col")
	if s.Len() != 2 {
		t.Errorf("FromStrings len = %d, want 2", s.Len())
	}
	if v, _ := s.ILoc(0).AsString(); v != "x" {
		t.Errorf("FromStrings[0] = %v, want 'x'", s.ILoc(0))
	}
}
