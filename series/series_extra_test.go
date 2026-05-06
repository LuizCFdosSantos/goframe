package series_test

import (
	"math"
	"testing"

	"github.com/LuizCFdosSantos/goframe/series"
	"github.com/LuizCFdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constructors
// ─────────────────────────────────────────────────────────────────────────────

func TestFromInts(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	if s.Len() != 3 {
		t.Errorf("FromInts len = %d, want 3", s.Len())
	}
	if v, _ := s.ILoc(0).AsInt(); v != 1 {
		t.Errorf("FromInts[0] = %v, want 1", s.ILoc(0))
	}
}

func TestFromFloats(t *testing.T) {
	s := series.FromFloats([]float64{1.5, 2.5}, "f")
	if s.Len() != 2 {
		t.Errorf("FromFloats len = %d, want 2", s.Len())
	}
	if f, _ := s.ILoc(0).AsFloat(); math.Abs(f-1.5) > 1e-9 {
		t.Errorf("FromFloats[0] = %v, want 1.5", s.ILoc(0))
	}
}

func TestFromStrings(t *testing.T) {
	s := series.FromStrings([]string{"a", "b"}, "col")
	if s.Len() != 2 {
		t.Errorf("FromStrings len = %d, want 2", s.Len())
	}
	if v, _ := s.ILoc(0).AsString(); v != "a" {
		t.Errorf("FromStrings[0] = %v, want 'a'", s.ILoc(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Accessors
// ─────────────────────────────────────────────────────────────────────────────

func TestName(t *testing.T) {
	s := series.FromInts([]int64{1}, "myname")
	if s.Name() != "myname" {
		t.Errorf("Name() = %q, want 'myname'", s.Name())
	}
}

func TestValues_ReturnsCopy(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	vals := s.Values()
	if len(vals) != 3 {
		t.Errorf("Values() len = %d, want 3", len(vals))
	}
	vals[0] = types.Int(999)
	if n, _ := s.ILoc(0).AsInt(); n != 1 {
		t.Error("Values() should return a copy, not a reference")
	}
}

func TestLoc_Found(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b", "c"})
	s := series.NewWithIndex([]types.Value{types.Int(10), types.Int(20), types.Int(30)}, idx, "x")
	v, err := s.Loc(types.Str("b"))
	if err != nil {
		t.Fatalf("Loc('b'): %v", err)
	}
	if n, _ := v.AsInt(); n != 20 {
		t.Errorf("Loc('b') = %v, want 20", v)
	}
}

func TestILocRange(t *testing.T) {
	s := series.FromInts([]int64{10, 20, 30, 40}, "x")
	sub := s.ILocRange(1, 3)
	if sub.Len() != 2 {
		t.Errorf("ILocRange(1,3) len = %d, want 2", sub.Len())
	}
	if n, _ := sub.ILoc(0).AsInt(); n != 20 {
		t.Errorf("ILocRange(1,3)[0] = %v, want 20", sub.ILoc(0))
	}
}

func TestHead(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3, 4, 5}, "x")
	h := s.Head(3)
	if h.Len() != 3 {
		t.Errorf("Head(3) len = %d, want 3", h.Len())
	}
	if n, _ := h.ILoc(0).AsInt(); n != 1 {
		t.Errorf("Head(3)[0] = %v, want 1", h.ILoc(0))
	}
}

func TestTail(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3, 4, 5}, "x")
	tail := s.Tail(2)
	if tail.Len() != 2 {
		t.Errorf("Tail(2) len = %d, want 2", tail.Len())
	}
	if n, _ := tail.ILoc(0).AsInt(); n != 4 {
		t.Errorf("Tail(2)[0] = %v, want 4", tail.ILoc(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Null handling
// ─────────────────────────────────────────────────────────────────────────────

func TestNullCount(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null(), types.Null()}, "x")
	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %d, want 2", s.NullCount())
	}
}

func TestIsNull_Mask(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null()}, "x")
	mask := s.IsNull()
	if !mask.ILoc(1).Equal(types.Bool(true)) {
		t.Error("IsNull()[1] should be true for null element")
	}
	if !mask.ILoc(0).Equal(types.Bool(false)) {
		t.Error("IsNull()[0] should be false for non-null element")
	}
}

func TestIsNotNull_Mask(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null()}, "x")
	mask := s.IsNotNull()
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("IsNotNull()[0] should be true")
	}
	if !mask.ILoc(1).Equal(types.Bool(false)) {
		t.Error("IsNotNull()[1] should be false for null")
	}
}

func TestFillNull(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Null()}, "x")
	filled := s.FillNull(types.Int(0))
	if n, _ := filled.ILoc(1).AsInt(); n != 0 {
		t.Errorf("FillNull(0): got %v, want 0", filled.ILoc(1))
	}
}

func TestFillNullFloat(t *testing.T) {
	s := series.New([]types.Value{types.Float(1.0), types.Null()}, "x")
	filled := s.FillNullFloat(99.0)
	if f, _ := filled.ILoc(1).AsFloat(); math.Abs(f-99.0) > 1e-9 {
		t.Errorf("FillNullFloat(99): got %v, want 99.0", filled.ILoc(1))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Arithmetic operations
// ─────────────────────────────────────────────────────────────────────────────

func TestSub(t *testing.T) {
	a := series.FromFloats([]float64{5.0, 3.0}, "a")
	b := series.FromFloats([]float64{1.0, 2.0}, "b")
	result := a.Sub(b)
	if f, _ := result.ILoc(0).AsFloat(); math.Abs(f-4.0) > 1e-9 {
		t.Errorf("Sub[0] = %v, want 4.0", result.ILoc(0))
	}
}

func TestMul(t *testing.T) {
	a := series.FromFloats([]float64{2.0, 3.0}, "a")
	b := series.FromFloats([]float64{4.0, 5.0}, "b")
	result := a.Mul(b)
	if f, _ := result.ILoc(0).AsFloat(); math.Abs(f-8.0) > 1e-9 {
		t.Errorf("Mul[0] = %v, want 8.0", result.ILoc(0))
	}
}

func TestDiv(t *testing.T) {
	a := series.FromFloats([]float64{6.0, 8.0}, "a")
	b := series.FromFloats([]float64{2.0, 4.0}, "b")
	result := a.Div(b)
	if f, _ := result.ILoc(0).AsFloat(); math.Abs(f-3.0) > 1e-9 {
		t.Errorf("Div[0] = %v, want 3.0", result.ILoc(0))
	}
}

func TestAddScalar(t *testing.T) {
	s := series.FromFloats([]float64{1.0, 2.0}, "x")
	result := s.AddScalar(10.0)
	if f, _ := result.ILoc(0).AsFloat(); math.Abs(f-11.0) > 1e-9 {
		t.Errorf("AddScalar(10)[0] = %v, want 11.0", result.ILoc(0))
	}
}

func TestSub_NullPropagation(t *testing.T) {
	a := series.New([]types.Value{types.Null(), types.Float(5.0)}, "a")
	b := series.FromFloats([]float64{1.0, 2.0}, "b")
	result := a.Sub(b)
	if !result.ILoc(0).IsNull() {
		t.Error("null - anything = null")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Comparison operations
// ─────────────────────────────────────────────────────────────────────────────

func TestGt(t *testing.T) {
	s := series.FromInts([]int64{1, 5, 3}, "x")
	mask := s.Gt(3)
	if !mask.ILoc(1).Equal(types.Bool(true)) {
		t.Error("Gt(3)[1] should be true (5>3)")
	}
	if !mask.ILoc(0).Equal(types.Bool(false)) {
		t.Error("Gt(3)[0] should be false (1 not >3)")
	}
}

func TestLt(t *testing.T) {
	s := series.FromInts([]int64{1, 5}, "x")
	mask := s.Lt(3)
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("Lt(3)[0] should be true")
	}
	if !mask.ILoc(1).Equal(types.Bool(false)) {
		t.Error("Lt(3)[1] should be false")
	}
}

func TestGte(t *testing.T) {
	s := series.FromInts([]int64{3, 4}, "x")
	mask := s.Gte(3)
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("Gte(3)[0] should be true (3>=3)")
	}
}

func TestLte(t *testing.T) {
	s := series.FromInts([]int64{3, 4}, "x")
	mask := s.Lte(3)
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("Lte(3)[0] should be true (3<=3)")
	}
	if !mask.ILoc(1).Equal(types.Bool(false)) {
		t.Error("Lte(3)[1] should be false (4 not <=3)")
	}
}

func TestEq(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	mask := s.Eq(2)
	if !mask.ILoc(1).Equal(types.Bool(true)) {
		t.Error("Eq(2)[1] should be true")
	}
	if !mask.ILoc(0).Equal(types.Bool(false)) {
		t.Error("Eq(2)[0] should be false")
	}
}

func TestEqStr(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a"}, "x")
	mask := s.EqStr("a")
	if !mask.ILoc(0).Equal(types.Bool(true)) {
		t.Error("EqStr('a')[0] should be true")
	}
	if !mask.ILoc(1).Equal(types.Bool(false)) {
		t.Error("EqStr('a')[1] should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Filtering and transformation
// ─────────────────────────────────────────────────────────────────────────────

func TestWhere_KeepsMatchedValues(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	mask := s.Gt(1)
	result := s.Where(mask)
	if !result.ILoc(0).IsNull() {
		t.Error("Where: non-matching value should become null")
	}
	if n, _ := result.ILoc(1).AsInt(); n != 2 {
		t.Errorf("Where: matching value should be kept, got %v", result.ILoc(1))
	}
}

func TestMap(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	doubled := s.Map(func(v types.Value) types.Value {
		if n, ok := v.AsInt(); ok {
			return types.Int(n * 2)
		}
		return v
	})
	if n, _ := doubled.ILoc(2).AsInt(); n != 6 {
		t.Errorf("Map double[2] = %v, want 6", doubled.ILoc(2))
	}
}

func TestApply(t *testing.T) {
	s := series.FromInts([]int64{10, 20}, "x")
	result := s.Apply(func(v types.Value) types.Value {
		return types.Int(0)
	})
	if n, _ := result.ILoc(0).AsInt(); n != 0 {
		t.Errorf("Apply: expected 0, got %v", result.ILoc(0))
	}
}

func TestMapWithIndex(t *testing.T) {
	s := series.FromInts([]int64{10, 20}, "x")
	// MapWithIndex fn signature: fn(label, value)
	result := s.MapWithIndex(func(label, v types.Value) types.Value {
		return v // return the value unchanged
	})
	if n, _ := result.ILoc(0).AsInt(); n != 10 {
		t.Errorf("MapWithIndex[0] = %v, want 10", result.ILoc(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregations
// ─────────────────────────────────────────────────────────────────────────────

func TestSum(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	if s.Sum() != 6.0 {
		t.Errorf("Sum() = %f, want 6.0", s.Sum())
	}
}

func TestSum_WithNulls(t *testing.T) {
	s := series.New([]types.Value{types.Float(1.0), types.Null(), types.Float(3.0)}, "x")
	if math.Abs(s.Sum()-4.0) > 1e-9 {
		t.Errorf("Sum() with null = %f, want 4.0", s.Sum())
	}
}

func TestMin(t *testing.T) {
	s := series.FromFloats([]float64{3.0, 1.0, 2.0}, "x")
	if math.Abs(s.Min()-1.0) > 1e-9 {
		t.Errorf("Min() = %f, want 1.0", s.Min())
	}
}

func TestMax(t *testing.T) {
	s := series.FromFloats([]float64{3.0, 1.0, 2.0}, "x")
	if math.Abs(s.Max()-3.0) > 1e-9 {
		t.Errorf("Max() = %f, want 3.0", s.Max())
	}
}

func TestMin_WithNulls(t *testing.T) {
	s := series.New([]types.Value{types.Float(5.0), types.Null(), types.Float(2.0)}, "x")
	if math.Abs(s.Min()-2.0) > 1e-9 {
		t.Errorf("Min() with null = %f, want 2.0", s.Min())
	}
}

func TestValueCounts(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a", "a"}, "x")
	counts := s.ValueCounts()
	if counts["a"] != 3 {
		t.Errorf("ValueCounts['a'] = %d, want 3", counts["a"])
	}
	if counts["b"] != 1 {
		t.Errorf("ValueCounts['b'] = %d, want 1", counts["b"])
	}
}

func TestUnique(t *testing.T) {
	s := series.FromStrings([]string{"a", "b", "a", "c"}, "x")
	u := s.Unique()
	if u.Len() != 3 {
		t.Errorf("Unique() len = %d, want 3", u.Len())
	}
}

func TestDescribe(t *testing.T) {
	s := series.FromFloats([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, "x")
	d := s.Describe()
	if d == nil {
		t.Fatal("Describe() should not return nil")
	}
	if d.Len() == 0 {
		t.Error("Describe() should have entries")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorting
// ─────────────────────────────────────────────────────────────────────────────

func TestSortValues_Ascending(t *testing.T) {
	s := series.FromInts([]int64{3, 1, 2}, "x")
	sorted := s.SortValues(true)
	if n, _ := sorted.ILoc(0).AsInt(); n != 1 {
		t.Errorf("SortValues asc[0] = %v, want 1", sorted.ILoc(0))
	}
}

func TestSortValues_Descending(t *testing.T) {
	s := series.FromInts([]int64{3, 1, 2}, "x")
	sorted := s.SortValues(false)
	if n, _ := sorted.ILoc(0).AsInt(); n != 3 {
		t.Errorf("SortValues desc[0] = %v, want 3", sorted.ILoc(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Metadata
// ─────────────────────────────────────────────────────────────────────────────

func TestRename(t *testing.T) {
	s := series.FromInts([]int64{1}, "old")
	r := s.Rename("new")
	if r.Name() != "new" {
		t.Errorf("Rename: got %q, want 'new'", r.Name())
	}
	if s.Name() != "old" {
		t.Error("Rename should not mutate the original series")
	}
}

func TestDtype_Int(t *testing.T) {
	s := series.FromInts([]int64{1, 2}, "x")
	if s.Dtype() != types.KindInt {
		t.Errorf("Dtype() = %v, want KindInt", s.Dtype())
	}
}

func TestDtype_Float(t *testing.T) {
	s := series.FromFloats([]float64{1.0}, "x")
	if s.Dtype() != types.KindFloat {
		t.Errorf("Dtype() = %v, want KindFloat", s.Dtype())
	}
}

func TestDtype_String(t *testing.T) {
	s := series.FromStrings([]string{"a"}, "x")
	if s.Dtype() != types.KindString {
		t.Errorf("Dtype() = %v, want KindString", s.Dtype())
	}
}

func TestDtype_Mixed(t *testing.T) {
	s := series.New([]types.Value{types.Int(1), types.Str("a")}, "x")
	if s.Dtype() != types.KindString {
		t.Errorf("Dtype() for mixed = %v, want KindString", s.Dtype())
	}
}

func TestDtype_Null(t *testing.T) {
	s := series.New([]types.Value{types.Null(), types.Null()}, "x")
	if s.Dtype() != types.KindNull {
		t.Errorf("Dtype() for all-null = %v, want KindNull", s.Dtype())
	}
}

func TestString(t *testing.T) {
	s := series.FromInts([]int64{1, 2, 3}, "x")
	str := s.String()
	if str == "" {
		t.Error("Series.String() should not be empty")
	}
}

func TestString_Large(t *testing.T) {
	// Verify String() handles a large series (truncation)
	vals := make([]int64, 30)
	for i := range vals {
		vals[i] = int64(i)
	}
	s := series.FromInts(vals, "big")
	str := s.String()
	if str == "" {
		t.Error("Series.String() for large series should not be empty")
	}
}
