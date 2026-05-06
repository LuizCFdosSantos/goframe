// Package series_test demonstrates and verifies Series behavior.
//
// Each test is also documentation — reading the tests gives you a precise
// specification of what each method does, including edge cases.
//
// Run with: go test ./series/...
// Run with coverage: go test -cover ./...
package series_test

import (
	"math"
	"testing"

	"github.com/LuizCFdosSantos/goframe/series"
	"github.com/LuizCFdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Construction tests
// ─────────────────────────────────────────────────────────────────────────────

func TestNew_DefaultRangeIndex(t *testing.T) {
	// When no index is specified, Series should get a 0-based integer index.
	// This matches pandas: pd.Series([1,2,3]).index → RangeIndex(start=0, stop=3, step=1)
	s := series.FromInts([]int64{10, 20, 30}, "x")

	if s.Len() != 3 {
		t.Errorf("expected len 3, got %d", s.Len())
	}

	// Index labels should be 0, 1, 2
	for i := 0; i < 3; i++ {
		label := s.Index().Label(i)
		n, ok := label.AsInt()
		if !ok || n != int64(i) {
			t.Errorf("index[%d]: expected Int(%d), got %v", i, i, label)
		}
	}
}

func TestNew_CopiesInputSlice(t *testing.T) {
	// Series should not be affected by mutations to the original slice.
	// This tests the defensive copy in New().
	vals := []types.Value{types.Int(1), types.Int(2)}
	s := series.New(vals, "x")

	vals[0] = types.Int(999) // mutate original

	// Series should still have 1, not 999
	if got := s.ILoc(0); !got.Equal(types.Int(1)) {
		t.Errorf("Series was mutated by external slice change: got %v", got)
	}
}

func TestNewWithIndex_LengthMismatch_Panics(t *testing.T) {
	// If data and index lengths don't match, we panic immediately.
	// Better to panic at construction than return corrupt data silently.
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for length mismatch, but did not panic")
		}
	}()

	idx := types.NewStringIndex([]string{"a", "b"}) // length 2
	data := []types.Value{types.Int(1)}             // length 1 — mismatch!
	series.NewWithIndex(data, idx, "x")
}

// ─────────────────────────────────────────────────────────────────────────────
// Access tests
// ─────────────────────────────────────────────────────────────────────────────

func TestILoc_NegativeIndex(t *testing.T) {
	// ILoc(-1) should return the last element, like Python's negative indexing.
	// pandas: s.iloc[-1] returns the last element.
	s := series.FromInts([]int64{10, 20, 30}, "x")

	if got := s.ILoc(-1); !got.Equal(types.Int(30)) {
		t.Errorf("ILoc(-1): expected 30, got %v", got)
	}
	if got := s.ILoc(-3); !got.Equal(types.Int(10)) {
		t.Errorf("ILoc(-3): expected 10, got %v", got)
	}
}

func TestLoc_NotFound(t *testing.T) {
	// Loc should return an error (not panic) when the label doesn't exist.
	// pandas raises KeyError in this case.
	idx := types.NewStringIndex([]string{"a", "b"})
	s := series.NewWithIndex([]types.Value{types.Int(1), types.Int(2)}, idx, "x")

	_, err := s.Loc(types.Str("z"))
	if err == nil {
		t.Error("expected error for missing label, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation tests
// ─────────────────────────────────────────────────────────────────────────────

func TestMean_SkipsNulls(t *testing.T) {
	// Mean should skip null values — like pandas' skipna=True (default).
	// pd.Series([1, np.nan, 3]).mean() → 2.0 (not NaN)
	s := series.New([]types.Value{
		types.Float(1.0),
		types.Null(),
		types.Float(3.0),
	}, "x")

	got := s.Mean()
	if math.Abs(got-2.0) > 1e-9 {
		t.Errorf("Mean with null: expected 2.0, got %f", got)
	}
}

func TestMean_AllNull(t *testing.T) {
	// Mean of all-null Series should be NaN, not 0.
	// This matches pandas: pd.Series([np.nan, np.nan]).mean() → nan
	s := series.New([]types.Value{types.Null(), types.Null()}, "x")
	if !math.IsNaN(s.Mean()) {
		t.Errorf("Mean of all-null: expected NaN, got %f", s.Mean())
	}
}

func TestStd_SampleStd(t *testing.T) {
	// Standard deviation uses ddof=1 (Bessel's correction) — same as pandas default.
	// For [2, 4, 4, 4, 5, 5, 7, 9]:
	//   mean = 5.0
	//   sum of squared deviations = 32
	//   std = sqrt(32 / 7) ≈ 2.138
	s := series.FromFloats([]float64{2, 4, 4, 4, 5, 5, 7, 9}, "x")
	expected := math.Sqrt(32.0 / 7.0) // ≈ 2.138
	got := s.Std()
	if math.Abs(got-expected) > 1e-6 {
		t.Errorf("Std: expected %.6f, got %.6f", expected, got)
	}
}

func TestCount_NonNull(t *testing.T) {
	// Count returns non-null count, NOT total length (unlike Go's len()).
	// pandas: pd.Series([1, np.nan, 3]).count() → 2, not 3
	s := series.New([]types.Value{
		types.Int(1),
		types.Null(),
		types.Int(3),
	}, "x")

	if got := s.Count(); got != 2 {
		t.Errorf("Count: expected 2 non-null, got %d", got)
	}
	if got := s.Len(); got != 3 {
		t.Errorf("Len: expected 3 total, got %d", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Filtering tests
// ─────────────────────────────────────────────────────────────────────────────

func TestFilter_PreservesIndex(t *testing.T) {
	// After filtering, the index should contain only the labels of kept rows.
	// pandas: s[s > 2] preserves original index labels for kept rows.
	idx := types.NewStringIndex([]string{"a", "b", "c", "d"})
	vals := []types.Value{types.Int(1), types.Int(3), types.Int(2), types.Int(4)}
	s := series.NewWithIndex(vals, idx, "x")

	// Filter: keep values > 2
	mask := s.Gt(2)
	filtered := s.Filter(mask)

	// Should have "b" (3) and "d" (4)
	if filtered.Len() != 2 {
		t.Errorf("expected 2 elements after filter, got %d", filtered.Len())
	}

	// First label should be "b"
	if label := filtered.Index().Label(0); !label.Equal(types.Str("b")) {
		t.Errorf("expected first label 'b', got %v", label)
	}
}

func TestFilter_MaskLengthMismatch_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for length mismatch")
		}
	}()
	s := series.FromInts([]int64{1, 2, 3}, "x")
	// We can't use Bool mask, so create a proper bool mask of wrong length
	boolMask := series.New([]types.Value{types.Bool(true), types.Bool(false)}, "m")
	s.Filter(boolMask)
}

// ─────────────────────────────────────────────────────────────────────────────
// Arithmetic tests
// ─────────────────────────────────────────────────────────────────────────────

func TestAdd_NullPropagation(t *testing.T) {
	// null + anything = null (null propagation).
	// This matches SQL and pandas default behavior.
	s1 := series.New([]types.Value{types.Float(1.0), types.Null(), types.Float(3.0)}, "a")
	s2 := series.New([]types.Value{types.Float(10.0), types.Float(20.0), types.Float(30.0)}, "b")

	result := s1.Add(s2)

	// position 0: 1 + 10 = 11
	if f, ok := result.ILoc(0).AsFloat(); !ok || math.Abs(f-11.0) > 1e-9 {
		t.Errorf("position 0: expected 11.0, got %v", result.ILoc(0))
	}
	// position 1: null + 20 = null
	if !result.ILoc(1).IsNull() {
		t.Errorf("position 1: expected null, got %v", result.ILoc(1))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Null handling tests
// ─────────────────────────────────────────────────────────────────────────────

func TestDropNull_UpdatesIndex(t *testing.T) {
	// DropNull should update both data AND index — keeping only valid rows' labels.
	idx := types.NewStringIndex([]string{"a", "b", "c"})
	vals := []types.Value{types.Int(1), types.Null(), types.Int(3)}
	s := series.NewWithIndex(vals, idx, "x")

	clean := s.DropNull()
	if clean.Len() != 2 {
		t.Errorf("expected 2 elements, got %d", clean.Len())
	}
	// Index should now be ["a", "c"] — "b" was dropped with its null value
	if label := clean.Index().Label(1); !label.Equal(types.Str("c")) {
		t.Errorf("expected second label 'c', got %v", label)
	}
}

func TestFillNullMean_CorrectMean(t *testing.T) {
	// FillNullMean should use the mean of non-null values only.
	// pd.Series([10, np.nan, 20]).fillna(s.mean()) → [10, 15, 20]
	s := series.New([]types.Value{
		types.Float(10.0),
		types.Null(),
		types.Float(20.0),
	}, "x")

	filled := s.FillNullMean()

	// Middle element should now be mean of (10, 20) = 15
	if f, ok := filled.ILoc(1).AsFloat(); !ok || math.Abs(f-15.0) > 1e-9 {
		t.Errorf("FillNullMean: expected 15.0, got %v", filled.ILoc(1))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorting tests
// ─────────────────────────────────────────────────────────────────────────────

func TestSortValues_NullsLast(t *testing.T) {
	// Nulls should sort to the end regardless of ascending/descending.
	// pandas: series.sort_values(na_position='last') — the default.
	s := series.New([]types.Value{
		types.Float(3.0),
		types.Null(),
		types.Float(1.0),
		types.Float(2.0),
	}, "x")

	sorted := s.SortValues(true) // ascending

	// Last element should be null
	if !sorted.ILoc(-1).IsNull() {
		t.Errorf("expected null at end of sorted series, got %v", sorted.ILoc(-1))
	}
	// First should be 1.0
	if f, ok := sorted.ILoc(0).AsFloat(); !ok || math.Abs(f-1.0) > 1e-9 {
		t.Errorf("expected 1.0 at start, got %v", sorted.ILoc(0))
	}
}
