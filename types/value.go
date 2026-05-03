// Package types defines the core data types used throughout goframe.
//
// # Design Philosophy
//
// In pandas (Python), every value in a Series can be any Python object,
// and Python's dynamic typing handles everything automatically. Go is
// statically typed, so we need an explicit "union type" — a single Go type
// that can hold an int, float, string, bool, or null value.
//
// We solve this with the Value type: a tagged union (also called a discriminated
// union or sum type). Each Value knows what type it holds via the Kind field,
// and the actual data lives in one of the concrete fields.
//
// # Why Not interface{}?
//
// We could store everything as `interface{}` (or `any`), but that has downsides:
//   - Type assertions everywhere make code messy
//   - No compile-time safety about what kinds of values exist
//   - Harder to implement fast type-specific operations (e.g., numeric sum)
//
// Our tagged union gives us a closed set of supported types, which lets us
// write exhaustive switch statements and catch missing cases at compile time.
package types

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Kind represents which data type a Value holds.
// Using an integer enum (rather than strings) makes comparisons O(1).
type Kind int

const (
	// KindNull represents a missing or undefined value — equivalent to
	// Python's None, pandas' NaN, or SQL's NULL.
	// Null is the zero value of Kind, so a zero-initialized Value is null.
	KindNull Kind = iota

	// KindInt represents a 64-bit signed integer.
	// We always use int64 (not int) so behavior is identical on 32-bit and
	// 64-bit systems — important for reproducibility.
	KindInt

	// KindFloat represents a 64-bit IEEE 754 floating-point number.
	// We use float64 to match Go's default float literal type and pandas'
	// numpy.float64 default.
	KindFloat

	// KindString represents a UTF-8 string. Go strings are immutable byte
	// slices, so storing them in a Value is cheap (just a pointer + length).
	KindString

	// KindBool represents a boolean true/false value.
	KindBool

	// KindDateTime represents a date-time value (time.Time).
	KindDateTime

	// KindDecimal represents an exact decimal number using scaled integer arithmetic.
	// Avoids floating-point rounding errors — ideal for financial and scientific data.
	KindDecimal
)

// String returns a human-readable name for the Kind — used in error messages
// and dtype display (mimicking pandas' dtype attribute).
func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindInt:
		return "int64"
	case KindFloat:
		return "float64"
	case KindString:
		return "object" // pandas calls string columns "object" dtype
	case KindBool:
		return "bool"
	case KindDateTime:
		return "datetime64"
	case KindDecimal:
		return "decimal"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// Value is a single data cell — the atom of goframe.
//
// Memory layout:
//
//	Kind    int      (8 bytes)
//	intVal  int64    (8 bytes)
//	fltVal  float64  (8 bytes)
//	strVal  string   (16 bytes: pointer + length)
//	boolVal bool     (1 byte, padded to 8)
//	timeVal time.Time (24 bytes)
//	currVal Currency  (int64 + string header = ~32 bytes)
//	                  ─────────────────────
//	                  ~104 bytes per Value
//
// This is larger than a raw int64 (8 bytes) but much smaller than an
// interface{} holding a boxed value (typically 16 bytes header + heap
// allocation). For a column of 1 million integers, our approach uses
// ~49 MB vs interface{}'s ~16 MB header + heap allocations — but our
// approach avoids GC pressure from millions of tiny heap objects.
//
// Production note: A real high-performance library would use columnar
// storage (e.g., Apache Arrow) where each column is a single typed
// []float64 or []int64 array. We use this approach for clarity.
type Value struct {
	Kind    Kind
	intVal  int64
	fltVal  float64
	strVal  string
	boolVal bool
	timeVal time.Time
	decVal  Decimal
}

// --- Constructors ---
// Each constructor sets the Kind tag and the appropriate field.
// All other fields remain zero — Go zero-initializes struct fields.

// Null returns a null Value, representing a missing data point.
// In pandas: pd.NA, np.nan, or None in a column.
func Null() Value {
	return Value{Kind: KindNull}
}

// Int wraps an int64 in a Value.
func Int(v int64) Value {
	return Value{Kind: KindInt, intVal: v}
}

// Float wraps a float64 in a Value.
// Note: NaN floats are valid but operations should handle them carefully.
func Float(v float64) Value {
	return Value{Kind: KindFloat, fltVal: v}
}

// Str wraps a string in a Value.
// We name it Str (not String) to avoid shadowing the Stringer interface.
func Str(v string) Value {
	return Value{Kind: KindString, strVal: v}
}

// Bool wraps a bool in a Value.
func Bool(v bool) Value {
	return Value{Kind: KindBool, boolVal: v}
}

// DateTime wraps a time.Time in a Value.
func DateTime(v time.Time) Value {
	return Value{Kind: KindDateTime, timeVal: v}
}

// Dec wraps a Decimal in a Value.
func Dec(v Decimal) Value {
	return Value{Kind: KindDecimal, decVal: v}
}

// --- Accessors ---
// Accessors return the typed value plus a boolean indicating success.
// This is Go's idiomatic "comma ok" pattern, like map lookups:
//   val, ok := myMap["key"]

// AsInt returns the integer value and true if Kind == KindInt.
// Returns (0, false) otherwise — never panics.
func (v Value) AsInt() (int64, bool) {
	if v.Kind != KindInt {
		return 0, false
	}
	return v.intVal, true
}

// AsFloat returns the float value and true if Kind == KindFloat.
func (v Value) AsFloat() (float64, bool) {
	if v.Kind != KindFloat {
		return 0, false
	}
	return v.fltVal, true
}

// AsString returns the string value and true if Kind == KindString.
func (v Value) AsString() (string, bool) {
	if v.Kind != KindString {
		return "", false
	}
	return v.strVal, true
}

// AsBool returns the bool value and true if Kind == KindBool.
func (v Value) AsBool() (bool, bool) {
	if v.Kind != KindBool {
		return false, false
	}
	return v.boolVal, true
}

// AsDateTime returns the time value and true if Kind == KindDateTime.
func (v Value) AsDateTime() (time.Time, bool) {
	if v.Kind != KindDateTime {
		return time.Time{}, false
	}
	return v.timeVal, true
}

// AsDecimal returns the Decimal value and true if Kind == KindDecimal.
func (v Value) AsDecimal() (Decimal, bool) {
	if v.Kind != KindDecimal {
		return Decimal{}, false
	}
	return v.decVal, true
}

// IsNull returns true if this Value represents missing data.
func (v Value) IsNull() bool {
	return v.Kind == KindNull
}

// --- Type coercion ---
// These methods attempt to convert the Value to a target type,
// similar to pandas' astype() but at the single-value level.

// ToFloat64 converts the Value to float64.
// This is used internally by numeric aggregation functions so they can
// operate on mixed int/float columns without branching on every element.
//
// Conversion rules:
//   - KindInt   → exact conversion (int64 fits in float64 up to 2^53)
//   - KindFloat → no-op
//   - KindBool  → 0.0 or 1.0 (matches pandas behavior)
//   - KindString → parse as decimal number; error if malformed
//   - KindNull  → math.NaN() (so aggregations can use NaN-aware math)
func (v Value) ToFloat64() (float64, error) {
	switch v.Kind {
	case KindNull:
		return math.NaN(), nil
	case KindInt:
		return float64(v.intVal), nil
	case KindFloat:
		return v.fltVal, nil
	case KindBool:
		if v.boolVal {
			return 1.0, nil
		}
		return 0.0, nil
	case KindString:
		f, err := strconv.ParseFloat(v.strVal, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to float64: %w", v.strVal, err)
		}
		return f, nil
	case KindDateTime:
		return float64(v.timeVal.Unix()), nil
	case KindDecimal:
		return v.decVal.ToFloat64(), nil
	default:
		return 0, fmt.Errorf("unknown Kind %d", v.Kind)
	}
}

// String returns a human-readable representation of the Value.
// This implements the fmt.Stringer interface, so fmt.Println(v) works.
func (v Value) String() string {
	switch v.Kind {
	case KindNull:
		return "<null>"
	case KindInt:
		return strconv.FormatInt(v.intVal, 10)
	case KindFloat:
		return strconv.FormatFloat(v.fltVal, 'g', -1, 64)
	case KindString:
		return v.strVal
	case KindBool:
		if v.boolVal {
			return "true"
		}
		return "false"
	case KindDateTime:
		return v.timeVal.Format(time.RFC3339)
	case KindDecimal:
		return v.decVal.String()
	default:
		return "<unknown>"
	}
}

// Equal returns true if two Values are equal.
//
// Null == Null returns TRUE in goframe (unlike SQL's NULL != NULL semantics).
// This matches pandas behavior: pd.NA == pd.NA is pd.NA (ambiguous), but
// for practical filtering we treat null == null as true.
//
// Float NaN != NaN always (IEEE 754 standard). This is intentional —
// if you want NaN-aware equality, use EqualNaN.
func (v Value) Equal(other Value) bool {
	if v.Kind != other.Kind {
		return false
	}
	switch v.Kind {
	case KindNull:
		return true
	case KindInt:
		return v.intVal == other.intVal
	case KindFloat:
		return v.fltVal == other.fltVal // NaN != NaN per IEEE 754
	case KindString:
		return v.strVal == other.strVal
	case KindBool:
		return v.boolVal == other.boolVal
	case KindDateTime:
		return v.timeVal.Equal(other.timeVal)
	case KindDecimal:
		return v.decVal.Equal(other.decVal)
	}
	return false
}

// LessThan returns true if v < other for orderable types.
// Panics if the types are incomparable (e.g., string vs int).
// Used internally by sorting operations.
func (v Value) LessThan(other Value) bool {
	if v.Kind != other.Kind {
		// Allow int vs float comparison by promoting to float
		vf, ve := v.ToFloat64()
		of, oe := other.ToFloat64()
		if ve == nil && oe == nil {
			return vf < of
		}
		panic(fmt.Sprintf("cannot compare %s and %s", v.Kind, other.Kind))
	}
	switch v.Kind {
	case KindInt:
		return v.intVal < other.intVal
	case KindFloat:
		return v.fltVal < other.fltVal
	case KindString:
		return v.strVal < other.strVal
	case KindBool:
		// false < true
		return !v.boolVal && other.boolVal
	case KindDateTime:
		return v.timeVal.Before(other.timeVal)
	case KindDecimal:
		return v.decVal.LessThan(other.decVal)
	default:
		panic(fmt.Sprintf("type %s is not orderable", v.Kind))
	}
}
