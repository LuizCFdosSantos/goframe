package types_test

import (
	"math"
	"testing"

	"github.com/LuizCdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Kind.String tests
// ─────────────────────────────────────────────────────────────────────────────

func TestKindString(t *testing.T) {
	cases := []struct {
		kind types.Kind
		want string
	}{
		{types.KindNull, "null"},
		{types.KindInt, "int64"},
		{types.KindFloat, "float64"},
		{types.KindString, "object"},
		{types.KindBool, "bool"},
		{types.Kind(99), "unknown(99)"},
	}
	for _, c := range cases {
		if got := c.kind.String(); got != c.want {
			t.Errorf("Kind(%d).String() = %q, want %q", c.kind, got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Value constructor + accessor tests
// ─────────────────────────────────────────────────────────────────────────────

func TestNull(t *testing.T) {
	v := types.Null()
	if !v.IsNull() {
		t.Error("Null().IsNull() should be true")
	}
	if v.Kind != types.KindNull {
		t.Errorf("expected KindNull, got %v", v.Kind)
	}
}

func TestInt(t *testing.T) {
	v := types.Int(42)
	n, ok := v.AsInt()
	if !ok || n != 42 {
		t.Errorf("Int(42).AsInt() = (%d, %v), want (42, true)", n, ok)
	}
	if _, ok := v.AsFloat(); ok {
		t.Error("Int value should not be accessible via AsFloat")
	}
	if _, ok := v.AsString(); ok {
		t.Error("Int value should not be accessible via AsString")
	}
	if _, ok := v.AsBool(); ok {
		t.Error("Int value should not be accessible via AsBool")
	}
}

func TestFloat(t *testing.T) {
	v := types.Float(3.14)
	f, ok := v.AsFloat()
	if !ok || math.Abs(f-3.14) > 1e-9 {
		t.Errorf("Float(3.14).AsFloat() = (%f, %v), want (3.14, true)", f, ok)
	}
	if _, ok := v.AsInt(); ok {
		t.Error("Float value should not be accessible via AsInt")
	}
}

func TestStr(t *testing.T) {
	v := types.Str("hello")
	s, ok := v.AsString()
	if !ok || s != "hello" {
		t.Errorf("Str(hello).AsString() = (%q, %v), want (hello, true)", s, ok)
	}
	if _, ok := v.AsInt(); ok {
		t.Error("String value should not be accessible via AsInt")
	}
}

func TestBool(t *testing.T) {
	vt := types.Bool(true)
	b, ok := vt.AsBool()
	if !ok || !b {
		t.Errorf("Bool(true).AsBool() = (%v, %v), want (true, true)", b, ok)
	}
	vf := types.Bool(false)
	b2, ok2 := vf.AsBool()
	if !ok2 || b2 {
		t.Errorf("Bool(false).AsBool() = (%v, %v), want (false, true)", b2, ok2)
	}
	if _, ok := vt.AsInt(); ok {
		t.Error("Bool value should not be accessible via AsInt")
	}
}

func TestIsNull_NonNull(t *testing.T) {
	if types.Int(1).IsNull() {
		t.Error("Int(1).IsNull() should be false")
	}
	if types.Float(0).IsNull() {
		t.Error("Float(0).IsNull() should be false")
	}
	if types.Str("").IsNull() {
		t.Error("Str('').IsNull() should be false")
	}
	if types.Bool(false).IsNull() {
		t.Error("Bool(false).IsNull() should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ToFloat64 tests
// ─────────────────────────────────────────────────────────────────────────────

func TestToFloat64_Null(t *testing.T) {
	f, err := types.Null().ToFloat64()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !math.IsNaN(f) {
		t.Errorf("Null.ToFloat64() should return NaN, got %f", f)
	}
}

func TestToFloat64_Int(t *testing.T) {
	f, err := types.Int(7).ToFloat64()
	if err != nil || math.Abs(f-7.0) > 1e-9 {
		t.Errorf("Int(7).ToFloat64() = (%f, %v), want (7.0, nil)", f, err)
	}
}

func TestToFloat64_Float(t *testing.T) {
	f, err := types.Float(2.5).ToFloat64()
	if err != nil || math.Abs(f-2.5) > 1e-9 {
		t.Errorf("Float(2.5).ToFloat64() = (%f, %v), want (2.5, nil)", f, err)
	}
}

func TestToFloat64_BoolTrue(t *testing.T) {
	f, err := types.Bool(true).ToFloat64()
	if err != nil || math.Abs(f-1.0) > 1e-9 {
		t.Errorf("Bool(true).ToFloat64() = (%f, %v), want (1.0, nil)", f, err)
	}
}

func TestToFloat64_BoolFalse(t *testing.T) {
	f, err := types.Bool(false).ToFloat64()
	if err != nil || math.Abs(f-0.0) > 1e-9 {
		t.Errorf("Bool(false).ToFloat64() = (%f, %v), want (0.0, nil)", f, err)
	}
}

func TestToFloat64_StringNumeric(t *testing.T) {
	f, err := types.Str("3.14").ToFloat64()
	if err != nil || math.Abs(f-3.14) > 1e-9 {
		t.Errorf("Str('3.14').ToFloat64() = (%f, %v), want (3.14, nil)", f, err)
	}
}

func TestToFloat64_StringInvalid(t *testing.T) {
	_, err := types.Str("not-a-number").ToFloat64()
	if err == nil {
		t.Error("Str('not-a-number').ToFloat64() should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Value.String tests
// ─────────────────────────────────────────────────────────────────────────────

func TestValueString(t *testing.T) {
	cases := []struct {
		v    types.Value
		want string
	}{
		{types.Null(), "<null>"},
		{types.Int(42), "42"},
		{types.Float(1.5), "1.5"},
		{types.Str("hello"), "hello"},
		{types.Bool(true), "true"},
		{types.Bool(false), "false"},
	}
	for _, c := range cases {
		if got := c.v.String(); got != c.want {
			t.Errorf("Value.String() = %q, want %q", got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Equal tests
// ─────────────────────────────────────────────────────────────────────────────

func TestEqual_SameKind(t *testing.T) {
	if !types.Null().Equal(types.Null()) {
		t.Error("Null == Null should be true")
	}
	if !types.Int(5).Equal(types.Int(5)) {
		t.Error("Int(5) == Int(5) should be true")
	}
	if types.Int(5).Equal(types.Int(6)) {
		t.Error("Int(5) == Int(6) should be false")
	}
	if !types.Float(1.0).Equal(types.Float(1.0)) {
		t.Error("Float(1.0) == Float(1.0) should be true")
	}
	if !types.Str("a").Equal(types.Str("a")) {
		t.Error("Str(a) == Str(a) should be true")
	}
	if types.Str("a").Equal(types.Str("b")) {
		t.Error("Str(a) == Str(b) should be false")
	}
	if !types.Bool(true).Equal(types.Bool(true)) {
		t.Error("Bool(true) == Bool(true) should be true")
	}
	if types.Bool(true).Equal(types.Bool(false)) {
		t.Error("Bool(true) == Bool(false) should be false")
	}
}

func TestEqual_DifferentKind(t *testing.T) {
	if types.Int(1).Equal(types.Float(1.0)) {
		t.Error("Int(1) == Float(1.0) should be false (different kinds)")
	}
	if types.Null().Equal(types.Int(0)) {
		t.Error("Null == Int(0) should be false")
	}
}

func TestEqual_FloatNaN(t *testing.T) {
	nan := types.Float(math.NaN())
	if nan.Equal(nan) {
		t.Error("NaN == NaN should be false per IEEE 754")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LessThan tests
// ─────────────────────────────────────────────────────────────────────────────

func TestLessThan_Int(t *testing.T) {
	if !types.Int(1).LessThan(types.Int(2)) {
		t.Error("Int(1) < Int(2) should be true")
	}
	if types.Int(2).LessThan(types.Int(1)) {
		t.Error("Int(2) < Int(1) should be false")
	}
	if types.Int(1).LessThan(types.Int(1)) {
		t.Error("Int(1) < Int(1) should be false")
	}
}

func TestLessThan_Float(t *testing.T) {
	if !types.Float(1.0).LessThan(types.Float(2.0)) {
		t.Error("Float(1.0) < Float(2.0) should be true")
	}
}

func TestLessThan_String(t *testing.T) {
	if !types.Str("apple").LessThan(types.Str("banana")) {
		t.Error("'apple' < 'banana' should be true")
	}
}

func TestLessThan_Bool(t *testing.T) {
	if !types.Bool(false).LessThan(types.Bool(true)) {
		t.Error("false < true should be true")
	}
	if types.Bool(true).LessThan(types.Bool(false)) {
		t.Error("true < false should be false")
	}
	if types.Bool(true).LessThan(types.Bool(true)) {
		t.Error("true < true should be false")
	}
}

func TestLessThan_IntFloat_Promotion(t *testing.T) {
	// Mixed int/float: promoted to float for comparison
	if !types.Int(1).LessThan(types.Float(2.0)) {
		t.Error("Int(1) < Float(2.0) should be true via promotion")
	}
}

func TestLessThan_IncomparableTypes_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("comparing String and Bool should panic")
		}
	}()
	types.Str("x").LessThan(types.Bool(true))
}

func TestLessThan_NullType_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("comparing two Nulls should panic (not orderable)")
		}
	}()
	types.Null().LessThan(types.Null())
}

// ─────────────────────────────────────────────────────────────────────────────
// Index tests
// ─────────────────────────────────────────────────────────────────────────────

func TestNewRangeIndex(t *testing.T) {
	idx := types.NewRangeIndex(3)
	if idx.Len() != 3 {
		t.Errorf("Len() = %d, want 3", idx.Len())
	}
	for i := 0; i < 3; i++ {
		lbl := idx.Label(i)
		n, ok := lbl.AsInt()
		if !ok || n != int64(i) {
			t.Errorf("Label(%d) = %v, want Int(%d)", i, lbl, i)
		}
	}
	if !idx.IsUnique() {
		t.Error("RangeIndex should be unique")
	}
}

func TestNewIndex_Unique(t *testing.T) {
	idx := types.NewIndex([]types.Value{types.Str("a"), types.Str("b"), types.Str("c")})
	if !idx.IsUnique() {
		t.Error("index with distinct labels should be unique")
	}
	pos, err := idx.Locate(types.Str("b"))
	if err != nil || pos != 1 {
		t.Errorf("Locate('b') = (%d, %v), want (1, nil)", pos, err)
	}
}

func TestNewIndex_Duplicate(t *testing.T) {
	idx := types.NewIndex([]types.Value{types.Str("a"), types.Str("a")})
	if idx.IsUnique() {
		t.Error("index with duplicate labels should not be unique")
	}
	_, err := idx.Locate(types.Str("a"))
	if err == nil {
		t.Error("Locate on non-unique index should return error")
	}
}

func TestNewStringIndex(t *testing.T) {
	idx := types.NewStringIndex([]string{"x", "y"})
	if idx.Len() != 2 {
		t.Errorf("Len() = %d, want 2", idx.Len())
	}
	lbl := idx.Label(0)
	s, ok := lbl.AsString()
	if !ok || s != "x" {
		t.Errorf("Label(0) = %v, want Str('x')", lbl)
	}
}

func TestIndex_Locate_NotFound(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b"})
	_, err := idx.Locate(types.Str("z"))
	if err == nil {
		t.Error("Locate for missing label should return error")
	}
}

func TestIndex_Labels_ReturnsCopy(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b"})
	lbls := idx.Labels()
	lbls[0] = types.Str("mutated")
	// Original should be unchanged
	if idx.Label(0).String() != "a" {
		t.Error("Labels() should return a copy, not the original slice")
	}
}

func TestIndex_Slice(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b", "c", "d"})
	sliced := idx.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("Slice(1,3).Len() = %d, want 2", sliced.Len())
	}
	if s, _ := sliced.Label(0).AsString(); s != "b" {
		t.Errorf("Slice(1,3).Label(0) = %v, want 'b'", sliced.Label(0))
	}
}

func TestIndex_String(t *testing.T) {
	idx := types.NewStringIndex([]string{"a", "b"})
	s := idx.String()
	if s == "" {
		t.Error("Index.String() should not be empty")
	}
}
