package types_test

import (
	"math"
	"testing"

	"github.com/LuizCFdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// NewDecimal / ParseDecimal
// ─────────────────────────────────────────────────────────────────────────────

func TestNewDecimal(t *testing.T) {
	cases := []struct {
		value int64
		scale uint8
		want  string
	}{
		{1599, 2, "15.99"},
		{100, 0, "100"},
		{1, 3, "0.001"},
		{0, 2, "0.00"},
		{-150, 2, "-1.50"},
	}
	for _, c := range cases {
		d := types.NewDecimal(c.value, c.scale)
		if got := d.String(); got != c.want {
			t.Errorf("NewDecimal(%d,%d).String() = %q, want %q", c.value, c.scale, got, c.want)
		}
	}
}

func TestParseDecimal_Valid(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"15.99", "15.99"},
		{"100", "100"},
		{"0.001", "0.001"},
		{"0.00", "0.00"},
		{"-3.50", "-3.50"},
		{"1000.00", "1000.00"},
	}
	for _, c := range cases {
		d, err := types.ParseDecimal(c.input)
		if err != nil {
			t.Errorf("ParseDecimal(%q) unexpected error: %v", c.input, err)
			continue
		}
		if got := d.String(); got != c.want {
			t.Errorf("ParseDecimal(%q).String() = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestParseDecimal_Invalid(t *testing.T) {
	cases := []string{"", "abc", "1.2.3", "."}
	for _, input := range cases {
		if _, err := types.ParseDecimal(input); err == nil {
			t.Errorf("ParseDecimal(%q) should return error", input)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Arithmetic
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimalAdd(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"15.99", "4.01", "20.00"},
		{"0.10", "0.20", "0.30"}, // the classic float64 pitfall
		{"100", "0.50", "100.50"},
		{"1.5", "1.50", "3.00"}, // different scales → aligned
	}
	for _, c := range cases {
		a, _ := types.ParseDecimal(c.a)
		b, _ := types.ParseDecimal(c.b)
		got := a.Add(b).String()
		if got != c.want {
			t.Errorf("(%s).Add(%s) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

func TestDecimalSub(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"20.00", "4.01", "15.99"},
		{"1.00", "0.01", "0.99"},
		{"5", "2.50", "2.50"},
	}
	for _, c := range cases {
		a, _ := types.ParseDecimal(c.a)
		b, _ := types.ParseDecimal(c.b)
		got := a.Sub(b).String()
		if got != c.want {
			t.Errorf("(%s).Sub(%s) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

func TestDecimalMul(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"15.00", "2", "30.00"},   // scale 2+0=2
		{"1.50", "2.00", "3.0000"}, // scale 2+2=4 → trailing zeros
		{"3", "3", "9"},
	}
	for _, c := range cases {
		a, _ := types.ParseDecimal(c.a)
		b, _ := types.ParseDecimal(c.b)
		got := a.Mul(b).String()
		if got != c.want {
			t.Errorf("(%s).Mul(%s) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Comparison
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimalCmp(t *testing.T) {
	a, _ := types.ParseDecimal("10.00")
	b, _ := types.ParseDecimal("20.00")
	eq, _ := types.ParseDecimal("10.0") // same value, different scale

	if a.Cmp(b) != -1 {
		t.Errorf("10 Cmp 20 should be -1")
	}
	if b.Cmp(a) != 1 {
		t.Errorf("20 Cmp 10 should be 1")
	}
	if a.Cmp(eq) != 0 {
		t.Errorf("10.00 Cmp 10.0 should be 0 (value-equal)")
	}
}

func TestDecimalEqual(t *testing.T) {
	a, _ := types.ParseDecimal("15.00")
	b, _ := types.ParseDecimal("15.0") // different scale, same value
	c, _ := types.ParseDecimal("15.01")

	if !a.Equal(b) {
		t.Error("15.00 Equal 15.0 should be true")
	}
	if a.Equal(c) {
		t.Error("15.00 Equal 15.01 should be false")
	}
}

func TestDecimalLessThan(t *testing.T) {
	lo, _ := types.ParseDecimal("9.99")
	hi, _ := types.ParseDecimal("10.00")

	if !lo.LessThan(hi) {
		t.Error("9.99 LessThan 10.00 should be true")
	}
	if hi.LessThan(lo) {
		t.Error("10.00 LessThan 9.99 should be false")
	}
	if lo.LessThan(lo) {
		t.Error("9.99 LessThan 9.99 should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ToFloat64
// ─────────────────────────────────────────────────────────────────────────────

func TestDecimalToFloat64(t *testing.T) {
	cases := []struct {
		input string
		want  float64
	}{
		{"15.99", 15.99},
		{"0.10", 0.10},
		{"100", 100.0},
		{"-3.50", -3.50},
	}
	for _, c := range cases {
		d, _ := types.ParseDecimal(c.input)
		got := d.ToFloat64()
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("ParseDecimal(%q).ToFloat64() = %f, want %f", c.input, got, c.want)
		}
	}
}
