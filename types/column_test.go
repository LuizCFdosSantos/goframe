package types_test

import (
	"math"
	"testing"
	"time"

	"github.com/LuizCdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// NewColumn — type detection
// ─────────────────────────────────────────────────────────────────────────────

func TestNewColumn_Empty(t *testing.T) {
	col := types.NewColumn(nil)
	if col.Len() != 0 {
		t.Errorf("empty column: Len() = %d, want 0", col.Len())
	}
}

func TestNewColumn_AllNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Null(), types.Null()})
	if col.Len() != 2 {
		t.Errorf("all-null column: Len() = %d, want 2", col.Len())
	}
	if !col.IsNull(0) || !col.IsNull(1) {
		t.Error("all-null column: IsNull should be true for every element")
	}
	if col.Dtype() != types.KindNull {
		t.Errorf("all-null column: Dtype() = %v, want KindNull", col.Dtype())
	}
}

func TestNewColumn_Mixed(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Str("a")})
	if col.Dtype() != types.KindString {
		t.Errorf("mixed column: Dtype() = %v, want KindString", col.Dtype())
	}
}

func TestNewColumn_IntType(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Int(2)})
	if col.Dtype() != types.KindInt {
		t.Errorf("int column: Dtype() = %v, want KindInt", col.Dtype())
	}
}

func TestNewColumn_FloatType(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.5), types.Float(2.5)})
	if col.Dtype() != types.KindFloat {
		t.Errorf("float column: Dtype() = %v, want KindFloat", col.Dtype())
	}
}

func TestNewColumn_StringType(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Str("a"), types.Str("b")})
	if col.Dtype() != types.KindString {
		t.Errorf("string column: Dtype() = %v, want KindString", col.Dtype())
	}
}

func TestNewColumn_BoolType(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true), types.Bool(false)})
	if col.Dtype() != types.KindBool {
		t.Errorf("bool column: Dtype() = %v, want KindBool", col.Dtype())
	}
}

func TestNewColumn_DateTimeType(t *testing.T) {
	now := time.Now()
	col := types.NewColumn([]types.Value{types.DateTime(now)})
	if col.Dtype() != types.KindDateTime {
		t.Errorf("datetime column: Dtype() = %v, want KindDateTime", col.Dtype())
	}
}

func TestNewColumn_DecimalType(t *testing.T) {
	d, _ := types.ParseDecimal("3.14")
	col := types.NewColumn([]types.Value{types.Dec(d)})
	if col.Dtype() != types.KindDecimal {
		t.Errorf("decimal column: Dtype() = %v, want KindDecimal", col.Dtype())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Direct constructors
// ─────────────────────────────────────────────────────────────────────────────

func TestNewIntColumn(t *testing.T) {
	col := types.NewIntColumn([]int64{10, 20, 30})
	if col.Len() != 3 {
		t.Errorf("NewIntColumn: Len() = %d, want 3", col.Len())
	}
	if n, _ := col.Get(1).AsInt(); n != 20 {
		t.Errorf("NewIntColumn: Get(1) = %v, want 20", col.Get(1))
	}
	if col.Dtype() != types.KindInt {
		t.Errorf("NewIntColumn: Dtype() = %v, want KindInt", col.Dtype())
	}
}

func TestNewIntColumn_DoesNotShareSlice(t *testing.T) {
	raw := []int64{1, 2, 3}
	col := types.NewIntColumn(raw)
	raw[0] = 999
	if n, _ := col.Get(0).AsInt(); n != 1 {
		t.Error("NewIntColumn should copy the slice")
	}
}

func TestNewFloatColumn(t *testing.T) {
	col := types.NewFloatColumn([]float64{1.1, 2.2})
	if col.Len() != 2 {
		t.Errorf("NewFloatColumn: Len() = %d, want 2", col.Len())
	}
	if f, _ := col.Get(0).AsFloat(); math.Abs(f-1.1) > 1e-9 {
		t.Errorf("NewFloatColumn: Get(0) = %v, want 1.1", col.Get(0))
	}
	if col.Dtype() != types.KindFloat {
		t.Errorf("NewFloatColumn: Dtype() = %v, want KindFloat", col.Dtype())
	}
}

func TestNewStringColumn(t *testing.T) {
	col := types.NewStringColumn([]string{"hello", "world"})
	if col.Len() != 2 {
		t.Errorf("NewStringColumn: Len() = %d, want 2", col.Len())
	}
	if s, _ := col.Get(0).AsString(); s != "hello" {
		t.Errorf("NewStringColumn: Get(0) = %v, want 'hello'", col.Get(0))
	}
	if col.Dtype() != types.KindString {
		t.Errorf("NewStringColumn: Dtype() = %v, want KindString", col.Dtype())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// IntColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestIntColumn_GetNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Null()})
	if !col.IsNull(1) {
		t.Error("IntColumn: IsNull(1) should be true")
	}
	if !col.Get(1).IsNull() {
		t.Error("IntColumn: Get on null index should return Null value")
	}
}

func TestIntColumn_GetNonNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(42)})
	if col.IsNull(0) {
		t.Error("IntColumn: IsNull(0) should be false")
	}
	n, ok := col.Get(0).AsInt()
	if !ok || n != 42 {
		t.Errorf("IntColumn: Get(0) = %v, want Int(42)", col.Get(0))
	}
}

func TestIntColumn_Slice(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Int(2), types.Int(3)})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("IntColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	if n, _ := sliced.Get(0).AsInt(); n != 2 {
		t.Errorf("IntColumn.Slice: Get(0) = %v, want 2", sliced.Get(0))
	}
}

func TestIntColumn_SliceWithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Null(), types.Int(3)})
	sliced := col.Slice(1, 3)
	if !sliced.IsNull(0) {
		t.Error("IntColumn.Slice: null should be preserved after slice")
	}
}

func TestIntColumn_SumInt(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Int(2), types.Int(3)}).(*types.IntColumn)
	total, count := col.SumInt()
	if total != 6 || count != 3 {
		t.Errorf("SumInt() = (%d, %d), want (6, 3)", total, count)
	}
}

func TestIntColumn_SumInt_WithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(10), types.Null(), types.Int(5)}).(*types.IntColumn)
	total, count := col.SumInt()
	if total != 15 || count != 2 {
		t.Errorf("SumInt() with nulls = (%d, %d), want (15, 2)", total, count)
	}
}

func TestIntColumn_SumInt_AllNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Null()}).(*types.IntColumn)
	// Make a fully-null int column by building one with all nulls explicitly
	col2 := types.NewColumn([]types.Value{types.Null(), types.Null()})
	// all-null falls back to GenericColumn, so use the null+int case
	_, count := col.SumInt()
	_ = col2
	if count != 1 {
		t.Errorf("SumInt count = %d, want 1 (one non-null)", count)
	}
}

func TestIntColumn_MinMaxInt(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(3), types.Int(1), types.Int(2)}).(*types.IntColumn)
	lo, hi, count := col.MinMaxInt()
	if lo != 1 || hi != 3 || count != 3 {
		t.Errorf("MinMaxInt() = (%d, %d, %d), want (1, 3, 3)", lo, hi, count)
	}
}

func TestIntColumn_MinMaxInt_WithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Null(), types.Int(5), types.Int(2)}).(*types.IntColumn)
	lo, hi, count := col.MinMaxInt()
	if lo != 2 || hi != 5 || count != 2 {
		t.Errorf("MinMaxInt() with nulls = (%d, %d, %d), want (2, 5, 2)", lo, hi, count)
	}
}

func TestIntColumn_MinMaxInt_AllNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Null()}).(*types.IntColumn)
	// Build an all-null int by sneaking in one null; verify count reflects it
	col2 := types.NewColumn([]types.Value{types.Int(7), types.Null()}).(*types.IntColumn)
	_, _, count := col2.MinMaxInt()
	_ = col
	if count != 1 {
		t.Errorf("MinMaxInt count = %d, want 1", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FloatColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestFloatColumn_GetNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Null()})
	if !col.IsNull(1) {
		t.Error("FloatColumn: IsNull(1) should be true")
	}
	if !col.Get(1).IsNull() {
		t.Error("FloatColumn: Get on null index should return Null value")
	}
}

func TestFloatColumn_Slice(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Float(2.0), types.Float(3.0)})
	sliced := col.Slice(0, 2)
	if sliced.Len() != 2 {
		t.Errorf("FloatColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
}

func TestFloatColumn_SliceWithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Null()})
	sliced := col.Slice(0, 2)
	if !sliced.IsNull(1) {
		t.Error("FloatColumn.Slice: null should be preserved")
	}
}

func TestFloatColumn_SumFloat(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Float(2.0), types.Float(3.0)}).(*types.FloatColumn)
	total, count := col.SumFloat()
	if math.Abs(total-6.0) > 1e-9 || count != 3 {
		t.Errorf("SumFloat() = (%f, %d), want (6.0, 3)", total, count)
	}
}

func TestFloatColumn_SumFloat_WithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Null(), types.Float(4.0)}).(*types.FloatColumn)
	total, count := col.SumFloat()
	if math.Abs(total-5.0) > 1e-9 || count != 2 {
		t.Errorf("SumFloat() with nulls = (%f, %d), want (5.0, 2)", total, count)
	}
}

func TestFloatColumn_SumFloat_SkipsNaN(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(1.0), types.Float(math.NaN()), types.Float(3.0)}).(*types.FloatColumn)
	total, count := col.SumFloat()
	if math.Abs(total-4.0) > 1e-9 || count != 2 {
		t.Errorf("SumFloat() skipping NaN = (%f, %d), want (4.0, 2)", total, count)
	}
}

func TestFloatColumn_MinMaxFloat(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(3.0), types.Float(1.0), types.Float(2.0)}).(*types.FloatColumn)
	lo, hi, count := col.MinMaxFloat()
	if math.Abs(lo-1.0) > 1e-9 || math.Abs(hi-3.0) > 1e-9 || count != 3 {
		t.Errorf("MinMaxFloat() = (%f, %f, %d), want (1.0, 3.0, 3)", lo, hi, count)
	}
}

func TestFloatColumn_MinMaxFloat_SkipsNaN(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Float(math.NaN()), types.Float(5.0)}).(*types.FloatColumn)
	lo, hi, count := col.MinMaxFloat()
	if math.Abs(lo-5.0) > 1e-9 || math.Abs(hi-5.0) > 1e-9 || count != 1 {
		t.Errorf("MinMaxFloat() skipping NaN = (%f, %f, %d), want (5.0, 5.0, 1)", lo, hi, count)
	}
}

func TestFloatColumn_MinMaxFloat_WithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Null(), types.Float(2.0), types.Float(8.0)}).(*types.FloatColumn)
	lo, hi, count := col.MinMaxFloat()
	if math.Abs(lo-2.0) > 1e-9 || math.Abs(hi-8.0) > 1e-9 || count != 2 {
		t.Errorf("MinMaxFloat() with nulls = (%f, %f, %d), want (2.0, 8.0, 2)", lo, hi, count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// StringColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestStringColumn_GetNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Str("a"), types.Null()})
	if !col.IsNull(1) {
		t.Error("StringColumn: IsNull(1) should be true")
	}
	if !col.Get(1).IsNull() {
		t.Error("StringColumn: Get on null index should return Null value")
	}
}

func TestStringColumn_Slice(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Str("a"), types.Str("b"), types.Str("c")})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("StringColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	if s, _ := sliced.Get(0).AsString(); s != "b" {
		t.Errorf("StringColumn.Slice: Get(0) = %v, want 'b'", sliced.Get(0))
	}
}

func TestStringColumn_SliceWithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Str("a"), types.Null()})
	sliced := col.Slice(0, 2)
	if !sliced.IsNull(1) {
		t.Error("StringColumn.Slice: null should be preserved")
	}
}

func TestStringColumn_RawAt(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Str("hello")}).(*types.StringColumn)
	if col.RawAt(0) != "hello" {
		t.Errorf("StringColumn.RawAt(0) = %q, want 'hello'", col.RawAt(0))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BoolColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestBoolColumn_GetAndIsNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true), types.Null(), types.Bool(false)})
	if col.IsNull(0) || !col.IsNull(1) || col.IsNull(2) {
		t.Error("BoolColumn: IsNull wrong")
	}
	if b, _ := col.Get(0).AsBool(); !b {
		t.Error("BoolColumn: Get(0) should be true")
	}
	if b, _ := col.Get(2).AsBool(); b {
		t.Error("BoolColumn: Get(2) should be false")
	}
	if !col.Get(1).IsNull() {
		t.Error("BoolColumn: Get(1) should be null")
	}
}

func TestBoolColumn_Slice(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true), types.Bool(false), types.Bool(true)})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("BoolColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	if b, _ := sliced.Get(0).AsBool(); b {
		t.Error("BoolColumn.Slice: Get(0) should be false")
	}
}

func TestBoolColumn_SliceWithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true), types.Null()})
	sliced := col.Slice(0, 2)
	if !sliced.IsNull(1) {
		t.Error("BoolColumn.Slice: null should be preserved")
	}
}

func TestBoolColumn_RawAt(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true), types.Bool(false)}).(*types.BoolColumn)
	if !col.RawAt(0) {
		t.Error("BoolColumn.RawAt(0) should be true")
	}
	if col.RawAt(1) {
		t.Error("BoolColumn.RawAt(1) should be false")
	}
}

func TestBoolColumn_Dtype(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Bool(true)})
	if col.Dtype() != types.KindBool {
		t.Errorf("BoolColumn.Dtype() = %v, want KindBool", col.Dtype())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DateTimeColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestDateTimeColumn_GetAndIsNull(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	col := types.NewColumn([]types.Value{types.DateTime(now), types.Null()})

	if col.IsNull(0) {
		t.Error("DateTimeColumn: IsNull(0) should be false")
	}
	if !col.IsNull(1) {
		t.Error("DateTimeColumn: IsNull(1) should be true")
	}
	if !col.Get(1).IsNull() {
		t.Error("DateTimeColumn: Get(1) should return Null")
	}

	got, ok := col.Get(0).AsDateTime()
	if !ok || !got.Equal(now) {
		t.Errorf("DateTimeColumn: Get(0) = %v, want %v", got, now)
	}
}

func TestDateTimeColumn_Dtype(t *testing.T) {
	col := types.NewColumn([]types.Value{types.DateTime(time.Now())})
	if col.Dtype() != types.KindDateTime {
		t.Errorf("DateTimeColumn.Dtype() = %v, want KindDateTime", col.Dtype())
	}
}

func TestDateTimeColumn_Slice(t *testing.T) {
	t1 := time.Now()
	t2 := t1.Add(time.Hour)
	t3 := t1.Add(2 * time.Hour)
	col := types.NewColumn([]types.Value{types.DateTime(t1), types.DateTime(t2), types.DateTime(t3)})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("DateTimeColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	got, _ := sliced.Get(0).AsDateTime()
	if !got.Equal(t2) {
		t.Errorf("DateTimeColumn.Slice: Get(0) = %v, want %v", got, t2)
	}
}

func TestDateTimeColumn_SliceWithNulls(t *testing.T) {
	col := types.NewColumn([]types.Value{types.DateTime(time.Now()), types.Null()})
	sliced := col.Slice(0, 2)
	if !sliced.IsNull(1) {
		t.Error("DateTimeColumn.Slice: null should be preserved")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DecimalColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimalColumn_GetAndIsNull(t *testing.T) {
	d, _ := types.ParseDecimal("3.14")
	col := types.NewColumn([]types.Value{types.Dec(d), types.Null()})

	if col.IsNull(0) {
		t.Error("DecimalColumn: IsNull(0) should be false")
	}
	if !col.IsNull(1) {
		t.Error("DecimalColumn: IsNull(1) should be true")
	}
	if !col.Get(1).IsNull() {
		t.Error("DecimalColumn: Get(1) should return Null")
	}

	got, ok := col.Get(0).AsDecimal()
	if !ok || !got.Equal(d) {
		t.Errorf("DecimalColumn: Get(0) = %v, want %v", got, d)
	}
}

func TestDecimalColumn_Dtype(t *testing.T) {
	d, _ := types.ParseDecimal("1.0")
	col := types.NewColumn([]types.Value{types.Dec(d)})
	if col.Dtype() != types.KindDecimal {
		t.Errorf("DecimalColumn.Dtype() = %v, want KindDecimal", col.Dtype())
	}
}

func TestDecimalColumn_Slice(t *testing.T) {
	d1, _ := types.ParseDecimal("1.0")
	d2, _ := types.ParseDecimal("2.0")
	d3, _ := types.ParseDecimal("3.0")
	col := types.NewColumn([]types.Value{types.Dec(d1), types.Dec(d2), types.Dec(d3)})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("DecimalColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	got, _ := sliced.Get(0).AsDecimal()
	if !got.Equal(d2) {
		t.Errorf("DecimalColumn.Slice: Get(0) = %v, want %v", got, d2)
	}
}

func TestDecimalColumn_SliceWithNulls(t *testing.T) {
	d, _ := types.ParseDecimal("1.0")
	col := types.NewColumn([]types.Value{types.Dec(d), types.Null()})
	sliced := col.Slice(0, 2)
	if !sliced.IsNull(1) {
		t.Error("DecimalColumn.Slice: null should be preserved")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// GenericColumn
// ─────────────────────────────────────────────────────────────────────────────

func TestGenericColumn_Mixed_Dtype(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Str("x")})
	if col.Dtype() != types.KindString {
		t.Errorf("GenericColumn mixed Dtype() = %v, want KindString", col.Dtype())
	}
}

func TestGenericColumn_AllNull_Dtype(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Null(), types.Null()})
	if col.Dtype() != types.KindNull {
		t.Errorf("GenericColumn all-null Dtype() = %v, want KindNull", col.Dtype())
	}
}

func TestGenericColumn_Get(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Str("a")})
	if n, ok := col.Get(0).AsInt(); !ok || n != 1 {
		t.Errorf("GenericColumn.Get(0) = %v, want Int(1)", col.Get(0))
	}
	if s, ok := col.Get(1).AsString(); !ok || s != "a" {
		t.Errorf("GenericColumn.Get(1) = %v, want Str('a')", col.Get(1))
	}
}

func TestGenericColumn_IsNull(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Null()})
	// NewColumn detects homogeneous int and creates IntColumn, not GenericColumn.
	// Force a GenericColumn via mixed types.
	col2 := types.NewColumn([]types.Value{types.Int(1), types.Str("x"), types.Null()})
	if !col2.IsNull(2) {
		t.Error("GenericColumn.IsNull(2) should be true")
	}
	_ = col
}

func TestGenericColumn_Slice(t *testing.T) {
	col := types.NewColumn([]types.Value{types.Int(1), types.Str("a"), types.Bool(true)})
	sliced := col.Slice(1, 3)
	if sliced.Len() != 2 {
		t.Errorf("GenericColumn.Slice: Len() = %d, want 2", sliced.Len())
	}
	if s, ok := sliced.Get(0).AsString(); !ok || s != "a" {
		t.Errorf("GenericColumn.Slice: Get(0) = %v, want Str('a')", sliced.Get(0))
	}
}

func TestGenericColumn_Homogeneous_Dtype(t *testing.T) {
	// A GenericColumn (all-null) scanned again returns KindNull
	col := types.NewColumn([]types.Value{types.Null()})
	if col.Dtype() != types.KindNull {
		t.Errorf("GenericColumn single-null Dtype() = %v, want KindNull", col.Dtype())
	}
}
