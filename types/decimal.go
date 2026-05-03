// Decimal provides exact decimal arithmetic without external dependencies.
//
// # Why Not float64?
//
// float64 cannot represent most decimal fractions exactly:
//
//	0.1 + 0.2 = 0.30000000000000004  (IEEE 754 artifact)
//
// Decimal stores the value as a scaled int64:
//
//	15.99  →  {value: 1599, scale: 2}
//	0.001  →  {value:    1, scale: 3}
//	100    →  {value:  100, scale: 0}
//
// Addition, subtraction, and multiplication are exact.
// Division is intentionally omitted to avoid unbounded scale growth.
package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Decimal is an exact decimal number backed by a scaled int64.
type Decimal struct {
	value int64 // unscaled coefficient
	scale uint8 // digits after the decimal point (0–18)
}

// NewDecimal creates a Decimal from an unscaled integer and a scale.
//
//	NewDecimal(1599, 2)  →  15.99
//	NewDecimal(100,  0)  →  100
//	NewDecimal(1,    3)  →  0.001
func NewDecimal(value int64, scale uint8) Decimal {
	return Decimal{value: value, scale: scale}
}

// ParseDecimal parses a decimal string ("15.99", "-3.5", "100") into a Decimal.
// Returns an error if the string is not a valid decimal number.
func ParseDecimal(s string) (Decimal, error) {
	if s == "" {
		return Decimal{}, fmt.Errorf("ParseDecimal: empty string")
	}

	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	dot := strings.IndexByte(s, '.')
	var intPart, fracPart string
	var scale uint8

	if dot < 0 {
		intPart = s
	} else {
		intPart = s[:dot]
		fracPart = s[dot+1:]
		if len(fracPart) > 18 {
			return Decimal{}, fmt.Errorf("ParseDecimal: too many decimal places in %q", s)
		}
		scale = uint8(len(fracPart))
	}

	combined := intPart + fracPart
	if combined == "" {
		return Decimal{}, fmt.Errorf("ParseDecimal: no digits in %q", s)
	}

	val, err := strconv.ParseInt(combined, 10, 64)
	if err != nil {
		return Decimal{}, fmt.Errorf("ParseDecimal: invalid number %q: %w", s, err)
	}

	if neg {
		val = -val
	}
	return Decimal{value: val, scale: scale}, nil
}

// String returns the canonical decimal string.
//
//	Decimal{1599, 2}.String()  →  "15.99"
//	Decimal{100,  0}.String()  →  "100"
//	Decimal{1,    3}.String()  →  "0.001"
func (d Decimal) String() string {
	if d.scale == 0 {
		return strconv.FormatInt(d.value, 10)
	}

	neg := d.value < 0
	abs := d.value
	if neg {
		abs = -abs
	}

	s := strconv.FormatInt(abs, 10)
	// Pad with leading zeros until we have at least scale+1 digits.
	for len(s) <= int(d.scale) {
		s = "0" + s
	}

	cut := len(s) - int(d.scale)
	result := s[:cut] + "." + s[cut:]
	if neg {
		return "-" + result
	}
	return result
}

// ToFloat64 converts to float64 (approximate).
// Use only for display or aggregation — not for exact arithmetic.
func (d Decimal) ToFloat64() float64 {
	return float64(d.value) / math.Pow10(int(d.scale))
}

// Add returns d + other, exact.
func (d Decimal) Add(other Decimal) Decimal {
	av, bv, s := alignScales(d, other)
	return Decimal{value: av + bv, scale: s}
}

// Sub returns d - other, exact.
func (d Decimal) Sub(other Decimal) Decimal {
	av, bv, s := alignScales(d, other)
	return Decimal{value: av - bv, scale: s}
}

// Mul returns d * other, exact.
// The result scale is d.scale + other.scale.
func (d Decimal) Mul(other Decimal) Decimal {
	return Decimal{value: d.value * other.value, scale: d.scale + other.scale}
}

// Cmp compares d and other. Returns -1, 0, or 1.
func (d Decimal) Cmp(other Decimal) int {
	av, bv, _ := alignScales(d, other)
	switch {
	case av < bv:
		return -1
	case av > bv:
		return 1
	default:
		return 0
	}
}

// Equal returns true if d == other (value-equality, ignoring trailing zeros).
//
//	NewDecimal(150, 1).Equal(NewDecimal(1500, 2))  →  true  (both = 15.0)
func (d Decimal) Equal(other Decimal) bool { return d.Cmp(other) == 0 }

// LessThan returns true if d < other.
func (d Decimal) LessThan(other Decimal) bool { return d.Cmp(other) < 0 }

// alignScales returns both values scaled to the higher scale.
func alignScales(a, b Decimal) (int64, int64, uint8) {
	if a.scale == b.scale {
		return a.value, b.value, a.scale
	}
	if a.scale > b.scale {
		return a.value, scaleUp(b.value, int(a.scale-b.scale)), a.scale
	}
	return scaleUp(a.value, int(b.scale-a.scale)), b.value, b.scale
}

// scaleUp multiplies v by 10^n.
func scaleUp(v int64, n int) int64 {
	for i := 0; i < n; i++ {
		v *= 10
	}
	return v
}
