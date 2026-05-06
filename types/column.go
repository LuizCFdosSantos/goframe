package types

import "time"

// Column is the internal typed storage interface for a Series.
// Each implementation stores a single native-type slice, eliminating the
// per-cell boxing overhead of []Value.
//
// Value is still the public API type — Get boxes only when called.
// Internal aggregations bypass Get entirely via typed fast-path methods.
type Column interface {
	Len() int
	Get(i int) Value
	IsNull(i int) bool
	Dtype() Kind
	Slice(start, end int) Column
}

// NewColumn creates the most memory-efficient Column for the given values.
// Homogeneous (single-type) columns get typed storage; mixed columns fall back
// to GenericColumn which stores []Value directly.
func NewColumn(vals []Value) Column {
	n := len(vals)
	if n == 0 {
		return &GenericColumn{}
	}

	var dominant Kind
	hasNulls := false
	mixed := false
	for _, v := range vals {
		if v.IsNull() {
			hasNulls = true
			continue
		}
		if dominant == KindNull {
			dominant = v.Kind
		} else if dominant != v.Kind {
			mixed = true
			break
		}
	}

	if mixed {
		data := make([]Value, n)
		copy(data, vals)
		return &GenericColumn{data: data}
	}

	switch dominant {
	case KindInt:
		return buildIntColumn(vals, hasNulls)
	case KindFloat:
		return buildFloatColumn(vals, hasNulls)
	case KindString:
		return buildStringColumn(vals, hasNulls)
	case KindBool:
		return buildBoolColumn(vals, hasNulls)
	case KindDateTime:
		return buildDateTimeColumn(vals, hasNulls)
	case KindDecimal:
		return buildDecimalColumn(vals, hasNulls)
	default:
		// All nulls or empty
		data := make([]Value, n)
		copy(data, vals)
		return &GenericColumn{data: data}
	}
}

// NewIntColumn creates a typed IntColumn directly from []int64 without boxing.
func NewIntColumn(data []int64) Column {
	d := make([]int64, len(data))
	copy(d, data)
	return &IntColumn{data: d}
}

// NewFloatColumn creates a typed FloatColumn directly from []float64 without boxing.
func NewFloatColumn(data []float64) Column {
	d := make([]float64, len(data))
	copy(d, data)
	return &FloatColumn{data: d}
}

// NewStringColumn creates a typed StringColumn directly from []string without boxing.
func NewStringColumn(data []string) Column {
	d := make([]string, len(data))
	copy(d, data)
	return &StringColumn{data: d}
}

// --- IntColumn ---

// IntColumn stores int64 values without per-cell boxing.
// nulls is nil when the column has no null values (common case).
type IntColumn struct {
	data  []int64
	nulls []bool
}

func buildIntColumn(vals []Value, hasNulls bool) *IntColumn {
	data := make([]int64, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.intVal
		}
	}
	return &IntColumn{data: data, nulls: nulls}
}

func (c *IntColumn) Len() int    { return len(c.data) }
func (c *IntColumn) Dtype() Kind { return KindInt }

func (c *IntColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *IntColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return Int(c.data[i])
}

func (c *IntColumn) Slice(start, end int) Column {
	col := &IntColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// SumInt returns (sum, count) of non-null values without boxing.
func (c *IntColumn) SumInt() (int64, int) {
	var total int64
	count := 0
	for i, v := range c.data {
		if c.nulls == nil || !c.nulls[i] {
			total += v
			count++
		}
	}
	return total, count
}

// MinMaxInt returns (min, max, count) of non-null values without boxing.
func (c *IntColumn) MinMaxInt() (int64, int64, int) {
	var lo, hi int64
	count := 0
	for i, v := range c.data {
		if c.nulls != nil && c.nulls[i] {
			continue
		}
		if count == 0 || v < lo {
			lo = v
		}
		if count == 0 || v > hi {
			hi = v
		}
		count++
	}
	return lo, hi, count
}

// --- FloatColumn ---

// FloatColumn stores float64 values without per-cell boxing.
type FloatColumn struct {
	data  []float64
	nulls []bool
}

func buildFloatColumn(vals []Value, hasNulls bool) *FloatColumn {
	data := make([]float64, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.fltVal
		}
	}
	return &FloatColumn{data: data, nulls: nulls}
}

func (c *FloatColumn) Len() int    { return len(c.data) }
func (c *FloatColumn) Dtype() Kind { return KindFloat }

func (c *FloatColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *FloatColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return Float(c.data[i])
}

func (c *FloatColumn) Slice(start, end int) Column {
	col := &FloatColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// SumFloat returns (sum, count) of non-null, non-NaN values without boxing.
func (c *FloatColumn) SumFloat() (float64, int) {
	var total float64
	count := 0
	for i, v := range c.data {
		if (c.nulls != nil && c.nulls[i]) || v != v { // v != v detects NaN
			continue
		}
		total += v
		count++
	}
	return total, count
}

// MinMaxFloat returns (min, max, count) of non-null, non-NaN values.
func (c *FloatColumn) MinMaxFloat() (float64, float64, int) {
	var lo, hi float64
	count := 0
	for i, v := range c.data {
		if (c.nulls != nil && c.nulls[i]) || v != v {
			continue
		}
		if count == 0 || v < lo {
			lo = v
		}
		if count == 0 || v > hi {
			hi = v
		}
		count++
	}
	return lo, hi, count
}

// --- StringColumn ---

// StringColumn stores string values without per-cell boxing.
type StringColumn struct {
	data  []string
	nulls []bool
}

func buildStringColumn(vals []Value, hasNulls bool) *StringColumn {
	data := make([]string, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.strVal
		}
	}
	return &StringColumn{data: data, nulls: nulls}
}

func (c *StringColumn) Len() int    { return len(c.data) }
func (c *StringColumn) Dtype() Kind { return KindString }

func (c *StringColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *StringColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return Str(c.data[i])
}

func (c *StringColumn) Slice(start, end int) Column {
	col := &StringColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// RawAt returns the raw string at i; caller must verify IsNull first.
func (c *StringColumn) RawAt(i int) string { return c.data[i] }

// --- BoolColumn ---

// BoolColumn stores bool values without per-cell boxing.
type BoolColumn struct {
	data  []bool
	nulls []bool
}

func buildBoolColumn(vals []Value, hasNulls bool) *BoolColumn {
	data := make([]bool, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.boolVal
		}
	}
	return &BoolColumn{data: data, nulls: nulls}
}

func (c *BoolColumn) Len() int    { return len(c.data) }
func (c *BoolColumn) Dtype() Kind { return KindBool }

func (c *BoolColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *BoolColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return Bool(c.data[i])
}

func (c *BoolColumn) Slice(start, end int) Column {
	col := &BoolColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// RawAt returns the raw bool at i; caller must verify IsNull first.
func (c *BoolColumn) RawAt(i int) bool { return c.data[i] }

// --- DateTimeColumn ---

// DateTimeColumn stores time.Time values without per-cell boxing.
type DateTimeColumn struct {
	data  []time.Time
	nulls []bool
}

func buildDateTimeColumn(vals []Value, hasNulls bool) *DateTimeColumn {
	data := make([]time.Time, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.timeVal
		}
	}
	return &DateTimeColumn{data: data, nulls: nulls}
}

func (c *DateTimeColumn) Len() int    { return len(c.data) }
func (c *DateTimeColumn) Dtype() Kind { return KindDateTime }

func (c *DateTimeColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *DateTimeColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return DateTime(c.data[i])
}

func (c *DateTimeColumn) Slice(start, end int) Column {
	col := &DateTimeColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// --- DecimalColumn ---

// DecimalColumn stores Decimal values without per-cell boxing.
type DecimalColumn struct {
	data  []Decimal
	nulls []bool
}

func buildDecimalColumn(vals []Value, hasNulls bool) *DecimalColumn {
	data := make([]Decimal, len(vals))
	var nulls []bool
	if hasNulls {
		nulls = make([]bool, len(vals))
	}
	for i, v := range vals {
		if v.IsNull() {
			nulls[i] = true
		} else {
			data[i] = v.decVal
		}
	}
	return &DecimalColumn{data: data, nulls: nulls}
}

func (c *DecimalColumn) Len() int    { return len(c.data) }
func (c *DecimalColumn) Dtype() Kind { return KindDecimal }

func (c *DecimalColumn) IsNull(i int) bool {
	return c.nulls != nil && c.nulls[i]
}

func (c *DecimalColumn) Get(i int) Value {
	if c.IsNull(i) {
		return Null()
	}
	return Dec(c.data[i])
}

func (c *DecimalColumn) Slice(start, end int) Column {
	col := &DecimalColumn{data: c.data[start:end]}
	if c.nulls != nil {
		col.nulls = c.nulls[start:end]
	}
	return col
}

// --- GenericColumn ---

// GenericColumn is the fallback for mixed-type or all-null columns.
// It stores []Value directly, preserving the original untyped behavior.
type GenericColumn struct {
	data []Value
}

func (c *GenericColumn) Len() int          { return len(c.data) }
func (c *GenericColumn) Get(i int) Value   { return c.data[i] }
func (c *GenericColumn) IsNull(i int) bool { return c.data[i].IsNull() }

func (c *GenericColumn) Dtype() Kind {
	var dominant Kind = KindNull
	for _, v := range c.data {
		if v.IsNull() {
			continue
		}
		if dominant == KindNull {
			dominant = v.Kind
		} else if dominant != v.Kind {
			return KindString
		}
	}
	return dominant
}

func (c *GenericColumn) Slice(start, end int) Column {
	return &GenericColumn{data: c.data[start:end]}
}
