// Package io provides reading and writing DataFrames to/from external formats.
//
// # Supported Formats
//
//   - CSV (read/write) — the most common data interchange format
//
// # Design Notes
//
// We follow pandas' philosophy: reading is lenient (try to infer types),
// writing is explicit (always quote strings, handle nulls consistently).
//
// # CSV Type Inference
//
// When reading CSV, every value is initially a string. We try to parse each
// column's values in order of specificity:
//  1. int64 (most specific — exact integer)
//  2. float64 (less specific — any decimal)
//  3. bool ("true"/"false"/"1"/"0")
//  4. null (empty cells)
//  5. string (fallback — keep as-is)
//
// This matches pandas' read_csv() behavior with dtype inference enabled.
package io

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LuizCdosSantos/goframe/dataframe"
	"github.com/LuizCdosSantos/goframe/series"
	"github.com/LuizCdosSantos/goframe/types"
)

// ReadCSVOptions configures CSV parsing behavior.
// Zero value provides sensible defaults matching pandas' read_csv() defaults.
type ReadCSVOptions struct {
	// Delimiter is the field separator (default: ',').
	// Use '\t' for TSV files.
	Delimiter rune

	// HasHeader indicates whether the first row contains column names (default: true).
	HasHeader bool

	// NullValues is the set of strings treated as null (default: {"", "NA", "null", "NULL", "N/A"}).
	// This mirrors pandas' na_values parameter.
	NullValues map[string]bool

	// InferTypes controls automatic type detection (default: true).
	// If false, all columns are stored as strings.
	InferTypes bool

	// MaxRows limits the number of data rows read (0 = unlimited).
	MaxRows int
}

// defaultOptions returns ReadCSVOptions with pandas-compatible defaults.
func defaultOptions() ReadCSVOptions {
	return ReadCSVOptions{
		Delimiter: ',',
		HasHeader: true,
		NullValues: map[string]bool{
			"":     true,
			"NA":   true,
			"null": true,
			"NULL": true,
			"N/A":  true,
			"nan":  true,
			"NaN":  true,
		},
		InferTypes: true,
	}
}

// ReadCSVFile reads a CSV file from disk and returns a DataFrame.
//
// This is the most common entry point, equivalent to pd.read_csv("file.csv").
//
// Example:
//
//	df, err := io.ReadCSVFile("data.csv", nil)
//	if err != nil { log.Fatal(err) }
//	fmt.Println(df)
func ReadCSVFile(path string, opts *ReadCSVOptions) (*dataframe.DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read_csv: cannot open %q: %w", path, err)
	}
	defer f.Close()
	return ReadCSV(f, opts)
}

// ReadCSV reads CSV from any io.Reader and returns a DataFrame.
// Useful for reading from HTTP responses, in-memory buffers, etc.
func ReadCSV(r io.Reader, opts *ReadCSVOptions) (*dataframe.DataFrame, error) {
	// Apply defaults for any unset options
	options := defaultOptions()
	if opts != nil {
		if opts.Delimiter != 0 {
			options.Delimiter = opts.Delimiter
		}
		if !opts.HasHeader {
			options.HasHeader = false
		}
		if opts.NullValues != nil {
			options.NullValues = opts.NullValues
		}
		if !opts.InferTypes {
			options.InferTypes = false
		}
		options.MaxRows = opts.MaxRows
	}

	// Step 1: Read all raw CSV rows into memory.
	// We read everything at once because type inference requires seeing all
	// values in a column — we can't determine the column type from one row.
	csvReader := csv.NewReader(r)
	csvReader.Comma = options.Delimiter
	csvReader.LazyQuotes = true       // be lenient about quoting errors
	csvReader.TrimLeadingSpace = true // strip leading whitespace from fields

	allRows, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read_csv: CSV parse error: %w", err)
	}

	if len(allRows) == 0 {
		return dataframe.New(nil, nil)
	}

	// Step 2: Extract column names from header row (if present).
	var colNames []string
	dataRows := allRows
	if options.HasHeader {
		if len(allRows) == 0 {
			return dataframe.New(nil, nil)
		}
		colNames = allRows[0]
		dataRows = allRows[1:]
	} else {
		// Generate synthetic column names: col_0, col_1, ...
		if len(allRows) > 0 {
			colNames = make([]string, len(allRows[0]))
			for i := range colNames {
				colNames[i] = fmt.Sprintf("col_%d", i)
			}
		}
	}

	// Respect MaxRows limit
	if options.MaxRows > 0 && len(dataRows) > options.MaxRows {
		dataRows = dataRows[:options.MaxRows]
	}

	nRows := len(dataRows)
	nCols := len(colNames)

	if nCols == 0 {
		return dataframe.New(nil, nil)
	}

	// Step 3: Transpose row-oriented data into column-oriented slices.
	// CSV is naturally row-oriented (one line = one record), but our DataFrame
	// is column-oriented (one Series per column). We pivot here.
	//
	// rawCols[j][i] = the string value at column j, row i
	rawCols := make([][]string, nCols)
	for j := range rawCols {
		rawCols[j] = make([]string, nRows)
	}
	for i, row := range dataRows {
		for j := range colNames {
			if j < len(row) {
				rawCols[j][i] = strings.TrimSpace(row[j])
			}
			// If row is shorter than header (malformed CSV), remaining cells are ""
		}
	}

	// Step 4: Convert each raw string column to a typed Series.
	cols := make(map[string]*series.Series, nCols)
	for j, name := range colNames {
		var s *series.Series
		if options.InferTypes {
			s = inferSeries(name, rawCols[j], options.NullValues)
		} else {
			// No inference — store everything as strings
			vals := make([]types.Value, len(rawCols[j]))
			for i, str := range rawCols[j] {
				if options.NullValues[str] {
					vals[i] = types.Null()
				} else {
					vals[i] = types.Str(str)
				}
			}
			s = series.New(vals, name)
		}
		cols[name] = s
	}

	return dataframe.New(cols, colNames)
}

// inferSeries determines the best type for a string column and returns a Series.
//
// We use a two-pass approach:
//  1. Probe pass: try to parse all values as the candidate type. If ANY non-null
//     value fails, the column can't be that type.
//  2. Convert pass: once we know the type, convert all values.
//
// Type priority: int64 > float64 > datetime64 > bool > string
// (We try the most specific type first and fall back as needed.)
func inferSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	if canBeInt(strs, nulls) {
		return parseIntSeries(name, strs, nulls)
	}
	if canBeFloat(strs, nulls) {
		return parseFloatSeries(name, strs, nulls)
	}
	// datetime before bool — date strings won't parse as bool
	if canBeDateTime(strs, nulls) {
		return parseDateTimeSeries(name, strs, nulls)
	}
	if canBeBool(strs, nulls) {
		return parseBoolSeries(name, strs, nulls)
	}
	return parseStringSeries(name, strs, nulls)
}

func canBeInt(strs []string, nulls map[string]bool) bool {
	for _, s := range strs {
		if nulls[s] {
			continue
		}
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func canBeFloat(strs []string, nulls map[string]bool) bool {
	for _, s := range strs {
		if nulls[s] {
			continue
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return false
		}
	}
	return true
}

func canBeBool(strs []string, nulls map[string]bool) bool {
	validBools := map[string]bool{
		"true": true, "false": true, "1": true, "0": true,
		"True": true, "False": true, "TRUE": true, "FALSE": true,
		"yes": true, "no": true, "Yes": true, "No": true,
	}
	for _, s := range strs {
		if nulls[s] {
			continue
		}
		if !validBools[s] {
			return false
		}
	}
	return true
}

// datetimeLayouts lists formats tried in order when inferring datetime columns.
var datetimeLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func canBeDateTime(strs []string, nulls map[string]bool) bool {
	for _, s := range strs {
		if nulls[s] {
			continue
		}
		parsed := false
		for _, layout := range datetimeLayouts {
			if _, err := time.Parse(layout, s); err == nil {
				parsed = true
				break
			}
		}
		if !parsed {
			return false
		}
	}
	return true
}

func parseDateTimeSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	vals := make([]types.Value, len(strs))
	for i, s := range strs {
		if nulls[s] {
			vals[i] = types.Null()
			continue
		}
		for _, layout := range datetimeLayouts {
			if t, err := time.Parse(layout, s); err == nil {
				vals[i] = types.DateTime(t)
				break
			}
		}
	}
	return series.New(vals, name)
}

func parseIntSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	vals := make([]types.Value, len(strs))
	for i, s := range strs {
		if nulls[s] {
			vals[i] = types.Null()
		} else {
			n, _ := strconv.ParseInt(s, 10, 64)
			vals[i] = types.Int(n)
		}
	}
	return series.New(vals, name)
}

func parseFloatSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	vals := make([]types.Value, len(strs))
	for i, s := range strs {
		if nulls[s] {
			vals[i] = types.Null()
		} else {
			f, _ := strconv.ParseFloat(s, 64)
			vals[i] = types.Float(f)
		}
	}
	return series.New(vals, name)
}

func parseBoolSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	truthy := map[string]bool{"true": true, "1": true, "True": true, "TRUE": true, "yes": true, "Yes": true}
	vals := make([]types.Value, len(strs))
	for i, s := range strs {
		if nulls[s] {
			vals[i] = types.Null()
		} else {
			vals[i] = types.Bool(truthy[s])
		}
	}
	return series.New(vals, name)
}

func parseStringSeries(name string, strs []string, nulls map[string]bool) *series.Series {
	vals := make([]types.Value, len(strs))
	for i, s := range strs {
		if nulls[s] {
			vals[i] = types.Null()
		} else {
			vals[i] = types.Str(s)
		}
	}
	return series.New(vals, name)
}

// --- CSV Writing ---

// WriteCSVOptions configures CSV output.
type WriteCSVOptions struct {
	// Delimiter is the output field separator (default: ',').
	Delimiter rune

	// NullValue is the string written for null values (default: "").
	// Use "NA" or "NaN" to match pandas' na_rep parameter.
	NullValue string

	// FloatFormat controls float formatting (default: 6 significant digits).
	// Use "" for Go's default %g formatting.
	FloatFormat string

	// WriteHeader controls whether to write the column header row (default: true).
	WriteHeader bool
}

// WriteCSVFile writes a DataFrame to a CSV file.
// Equivalent to df.to_csv("file.csv") in pandas.
func WriteCSVFile(df *dataframe.DataFrame, path string, opts *WriteCSVOptions) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("write_csv: cannot create %q: %w", path, err)
	}
	defer f.Close()
	return WriteCSV(df, f, opts)
}

// WriteCSV writes a DataFrame to any io.Writer in CSV format.
func WriteCSV(df *dataframe.DataFrame, w io.Writer, opts *WriteCSVOptions) error {
	// Apply defaults
	nullValue := ""
	delimiter := ','
	writeHeader := true
	if opts != nil {
		nullValue = opts.NullValue
		if opts.Delimiter != 0 {
			delimiter = opts.Delimiter
		}
		if !opts.WriteHeader {
			writeHeader = false
		}
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = delimiter

	cols := df.Columns()
	nRows := df.Len()

	// Write header row
	if writeHeader {
		if err := csvWriter.Write(cols); err != nil {
			return fmt.Errorf("write_csv: write header: %w", err)
		}
	}

	// Write data rows
	// We iterate row by row (even though data is columnar) because CSV is
	// row-oriented. For very wide DataFrames, column-first access would thrash
	// cache — a production implementation would batch this differently.
	row := make([]string, len(cols))
	for i := 0; i < nRows; i++ {
		for j, colName := range cols {
			val := df.MustCol(colName).ILoc(i)
			row[j] = valueToCSV(val, nullValue)
		}
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("write_csv: write row %d: %w", i, err)
		}
	}

	csvWriter.Flush()
	return csvWriter.Error()
}

// valueToCSV formats a Value for CSV output.
// Special cases:
//   - Null → nullValue (often "" or "NA")
//   - Float NaN → "NaN" (distinct from null)
//   - Float Inf → "Inf"/"-Inf"
func valueToCSV(v types.Value, nullValue string) string {
	if v.IsNull() {
		return nullValue
	}
	if f, ok := v.AsFloat(); ok {
		if math.IsNaN(f) {
			return "NaN"
		}
		if math.IsInf(f, 1) {
			return "Inf"
		}
		if math.IsInf(f, -1) {
			return "-Inf"
		}
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return v.String()
}
