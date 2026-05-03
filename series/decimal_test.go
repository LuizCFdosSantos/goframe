package series_test

import (
	"math"
	"testing"

	"github.com/LuizCdosSantos/goframe/series"
	"github.com/LuizCdosSantos/goframe/types"
)

// decimal helpers — keep tests readable
func dec(s string) types.Value {
	d, _ := types.ParseDecimal(s)
	return types.Dec(d)
}

func decSeries(amounts ...string) *series.Series {
	vals := make([]types.Value, len(amounts))
	for i, a := range amounts {
		if a == "" {
			vals[i] = types.Null()
		} else {
			vals[i] = dec(a)
		}
	}
	return series.New(vals, "price")
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregations
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_Sum(t *testing.T) {
	s := decSeries("10.00", "5.99", "4.01")
	got := s.Sum()
	if math.Abs(got-20.00) > 1e-9 {
		t.Errorf("Sum() = %f, want 20.00", got)
	}
}

func TestDecimal_Sum_SkipsNulls(t *testing.T) {
	s := decSeries("10.00", "", "5.00")
	got := s.Sum()
	if math.Abs(got-15.00) > 1e-9 {
		t.Errorf("Sum() with null = %f, want 15.00", got)
	}
}

func TestDecimal_Sum_AllNull(t *testing.T) {
	s := decSeries("", "")
	if !math.IsNaN(s.Sum()) {
		t.Error("Sum() of all-null decimal series should be NaN")
	}
}

func TestDecimal_Mean(t *testing.T) {
	s := decSeries("10.00", "20.00", "30.00")
	got := s.Mean()
	if math.Abs(got-20.00) > 1e-9 {
		t.Errorf("Mean() = %f, want 20.00", got)
	}
}

func TestDecimal_Mean_SkipsNulls(t *testing.T) {
	s := decSeries("10.00", "", "30.00")
	got := s.Mean()
	if math.Abs(got-20.00) > 1e-9 {
		t.Errorf("Mean() with null = %f, want 20.00", got)
	}
}

func TestDecimal_Min(t *testing.T) {
	s := decSeries("9.99", "49.99", "1.00")
	got := s.Min()
	if math.Abs(got-1.00) > 1e-9 {
		t.Errorf("Min() = %f, want 1.00", got)
	}
}

func TestDecimal_Max(t *testing.T) {
	s := decSeries("9.99", "49.99", "1.00")
	got := s.Max()
	if math.Abs(got-49.99) > 1e-9 {
		t.Errorf("Max() = %f, want 49.99", got)
	}
}

func TestDecimal_Std(t *testing.T) {
	// std of [10, 20, 30] = 10 (sample std)
	s := decSeries("10.00", "20.00", "30.00")
	got := s.Std()
	if math.Abs(got-10.00) > 1e-9 {
		t.Errorf("Std() = %f, want 10.00", got)
	}
}

func TestDecimal_Count(t *testing.T) {
	s := decSeries("10.00", "", "30.00")
	if s.Count() != 2 {
		t.Errorf("Count() = %d, want 2", s.Count())
	}
	if s.NullCount() != 1 {
		t.Errorf("NullCount() = %d, want 1", s.NullCount())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorting
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_SortValues_Ascending(t *testing.T) {
	s := decSeries("30.00", "10.00", "20.00")
	sorted := s.SortValues(true)
	want := []string{"10.00", "20.00", "30.00"}
	for i, w := range want {
		if got := sorted.ILoc(i).String(); got != w {
			t.Errorf("SortValues asc [%d] = %q, want %q", i, got, w)
		}
	}
}

func TestDecimal_SortValues_Descending(t *testing.T) {
	s := decSeries("30.00", "10.00", "20.00")
	sorted := s.SortValues(false)
	want := []string{"30.00", "20.00", "10.00"}
	for i, w := range want {
		if got := sorted.ILoc(i).String(); got != w {
			t.Errorf("SortValues desc [%d] = %q, want %q", i, got, w)
		}
	}
}

func TestDecimal_SortValues_NullsLast(t *testing.T) {
	s := decSeries("20.00", "", "10.00")
	sorted := s.SortValues(true)
	if !sorted.ILoc(2).IsNull() {
		t.Errorf("SortValues: null should sort to end, got %v", sorted.ILoc(2))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Filtering
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_Gt(t *testing.T) {
	s := decSeries("5.00", "15.00", "25.00")
	mask := s.Gt(10.00)
	filtered := s.Filter(mask)
	if filtered.Len() != 2 {
		t.Errorf("Gt(10.00) filtered len = %d, want 2", filtered.Len())
	}
}

func TestDecimal_Lt(t *testing.T) {
	s := decSeries("5.00", "15.00", "25.00")
	mask := s.Lt(10.00)
	filtered := s.Filter(mask)
	if filtered.Len() != 1 {
		t.Errorf("Lt(10.00) filtered len = %d, want 1", filtered.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Null handling
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_DropNull(t *testing.T) {
	s := decSeries("10.00", "", "30.00")
	clean := s.DropNull()
	if clean.Len() != 2 {
		t.Errorf("DropNull() len = %d, want 2", clean.Len())
	}
}

func TestDecimal_FillNullMean(t *testing.T) {
	// mean of [10, 30] = 20 → null filled with Float(20)
	s := decSeries("10.00", "", "30.00")
	filled := s.FillNullMean()
	v := filled.ILoc(1)
	f, ok := v.AsFloat()
	if !ok {
		t.Fatalf("FillNullMean: expected Float at index 1, got kind %v", v.Kind)
	}
	if math.Abs(f-20.00) > 1e-9 {
		t.Errorf("FillNullMean: filled value = %f, want 20.00", f)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Describe
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_Describe(t *testing.T) {
	s := decSeries("10.00", "20.00", "30.00")
	desc := s.Describe()

	find := func(label string) float64 {
		v, err := desc.Loc(types.Str(label))
		if err != nil {
			t.Fatalf("Describe: label %q not found", label)
		}
		f, _ := v.AsFloat()
		return f
	}

	if math.Abs(find("count")-3) > 1e-9 {
		t.Errorf("Describe count = %f, want 3", find("count"))
	}
	if math.Abs(find("mean")-20.00) > 1e-9 {
		t.Errorf("Describe mean = %f, want 20.00", find("mean"))
	}
	if math.Abs(find("min")-10.00) > 1e-9 {
		t.Errorf("Describe min = %f, want 10.00", find("min"))
	}
	if math.Abs(find("max")-30.00) > 1e-9 {
		t.Errorf("Describe max = %f, want 30.00", find("max"))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Decimal exactness proof: the classic 0.1 + 0.2 case
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimal_ExactnessVsFloat64(t *testing.T) {
	// With float64 this would be 0.30000000000000004
	a, _ := types.ParseDecimal("0.10")
	b, _ := types.ParseDecimal("0.20")
	result := a.Add(b)
	if result.String() != "0.30" {
		t.Errorf("0.10 + 0.20 = %q, want \"0.30\" (Decimal is exact)", result.String())
	}

	// Prove float64 would fail
	if 0.1+0.2 == 0.3 {
		t.Log("float64 happened to be exact (platform anomaly)")
	}
}
