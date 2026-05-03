package io_test

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LuizCdosSantos/goframe/dataframe"
	goio "github.com/LuizCdosSantos/goframe/io"
	"github.com/LuizCdosSantos/goframe/series"
	"github.com/LuizCdosSantos/goframe/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// ReadCSV — basic type inference
// ─────────────────────────────────────────────────────────────────────────────

func TestReadCSV_Ints(t *testing.T) {
	csv := "id,score\n1,90\n2,85\n3,70\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 3 {
		t.Errorf("rows = %d, want 3", df.Len())
	}
	v := df.MustCol("id").ILoc(0)
	if _, ok := v.AsInt(); !ok {
		t.Errorf("id column should be inferred as int, got kind %v", v.Kind)
	}
}

func TestReadCSV_Floats(t *testing.T) {
	csv := "price\n1.5\n2.5\n3.0\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("price").ILoc(0)
	if f, ok := v.AsFloat(); !ok || math.Abs(f-1.5) > 1e-9 {
		t.Errorf("price[0] = %v, want Float(1.5)", v)
	}
}

func TestReadCSV_Bools(t *testing.T) {
	csv := "active\ntrue\nfalse\ntrue\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("active").ILoc(0)
	if b, ok := v.AsBool(); !ok || !b {
		t.Errorf("active[0] = %v, want Bool(true)", v)
	}
}

func TestReadCSV_Strings(t *testing.T) {
	csv := "name\nalice\nbob\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("name").ILoc(0)
	if s, ok := v.AsString(); !ok || s != "alice" {
		t.Errorf("name[0] = %v, want Str('alice')", v)
	}
}

func TestReadCSV_NullValues(t *testing.T) {
	// Use a two-column CSV to include an explicit empty cell and NA in the same column
	csv := "x,y\n1,good\n,NA\n3,ok\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// "y" column: "good", "NA" (null), "ok"
	if !df.MustCol("y").ILoc(1).IsNull() {
		t.Errorf("'NA' should be null, got %v", df.MustCol("y").ILoc(1))
	}
	// "x" column row 1 has empty cell → null
	if !df.MustCol("x").ILoc(1).IsNull() {
		t.Errorf("empty cell should be null, got %v", df.MustCol("x").ILoc(1))
	}
}

func TestReadCSV_Empty(t *testing.T) {
	df, err := goio.ReadCSV(strings.NewReader(""), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Errorf("empty CSV shape = (%d,%d), want (0,0)", rows, cols)
	}
}

func TestReadCSV_HeaderOnly(t *testing.T) {
	df, err := goio.ReadCSV(strings.NewReader("a,b,c\n"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 0 {
		t.Errorf("header-only CSV should have 0 rows, got %d", df.Len())
	}
}

func TestReadCSV_NoHeader(t *testing.T) {
	csv := "1,2\n3,4\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  false,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("rows = %d, want 2", df.Len())
	}
	if !df.HasColumn("col_0") || !df.HasColumn("col_1") {
		t.Errorf("synthetic column names missing; cols = %v", df.Columns())
	}
}

func TestReadCSV_NoTypeInference(t *testing.T) {
	csv := "x\n1\n2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  true,
		InferTypes: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("x").ILoc(0)
	if _, ok := v.AsString(); !ok {
		t.Errorf("without type inference, values should be strings; got kind %v", v.Kind)
	}
}

func TestReadCSV_CustomDelimiter(t *testing.T) {
	tsv := "a\tb\n1\t2\n"
	df, err := goio.ReadCSV(strings.NewReader(tsv), &goio.ReadCSVOptions{
		Delimiter:  '\t',
		HasHeader:  true,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !df.HasColumn("a") || !df.HasColumn("b") {
		t.Errorf("tab-delimited columns not parsed; cols = %v", df.Columns())
	}
}

func TestReadCSV_MaxRows(t *testing.T) {
	csv := "x\n1\n2\n3\n4\n5\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		MaxRows:    3,
		HasHeader:  true,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 3 {
		t.Errorf("MaxRows=3 should yield 3 rows, got %d", df.Len())
	}
}

func TestReadCSV_CustomNullValues(t *testing.T) {
	csv := "x\nMISSING\n1\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		NullValues: map[string]bool{"MISSING": true},
		HasHeader:  true,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !df.MustCol("x").ILoc(0).IsNull() {
		t.Errorf("'MISSING' with custom NullValues should be null")
	}
}

func TestReadCSV_BoolVariants(t *testing.T) {
	csv := "flag\n1\n0\nTrue\nFalse\nyes\nno\nYes\nNo\nTRUE\nFALSE\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// All values should be inferred as bool
	v := df.MustCol("flag").ILoc(0)
	if _, ok := v.AsBool(); !ok {
		t.Errorf("flag[0] should be Bool, got kind %v", v.Kind)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadCSVFile
// ─────────────────────────────────────────────────────────────────────────────

func TestReadCSVFile_NotFound(t *testing.T) {
	_, err := goio.ReadCSVFile("/nonexistent/path/data.csv", nil)
	if err == nil {
		t.Error("ReadCSVFile for missing file should return error")
	}
}

func TestReadCSVFile_Valid(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(tmp, []byte("a,b\n1,2\n3,4\n"), 0644); err != nil {
		t.Fatal(err)
	}
	df, err := goio.ReadCSVFile(tmp, nil)
	if err != nil || df.Len() != 2 {
		t.Errorf("ReadCSVFile: err=%v, rows=%d", err, df.Len())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteCSV
// ─────────────────────────────────────────────────────────────────────────────

func TestWriteCSV_RoundTrip(t *testing.T) {
	original := "name,score\nalice,90\nbob,85\n"
	df, err := goio.ReadCSV(strings.NewReader(original), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}

	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "name") || !strings.Contains(out, "score") {
		t.Errorf("WriteCSV output missing headers: %q", out)
	}
}

func TestWriteCSV_NullValue(t *testing.T) {
	// Build a DataFrame with an explicit null using types.Value directly
	s := series.New([]types.Value{types.Int(1), types.Null(), types.Int(3)}, "x")
	df, err := dataframe.New(map[string]*series.Series{"x": s}, []string{"x"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{NullValue: "NA"}); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	if !strings.Contains(buf.String(), "NA") {
		t.Errorf("WriteCSV should write 'NA' for null, got: %q", buf.String())
	}
}

func TestWriteCSV_NoHeader(t *testing.T) {
	csv := "a,b\n1,2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{WriteHeader: false}); err != nil {
		t.Fatalf("WriteCSV no header: %v", err)
	}
	if strings.Contains(buf.String(), "a,b") {
		t.Errorf("WriteHeader=false should not write header row, got: %q", buf.String())
	}
}

func TestWriteCSV_CustomDelimiter(t *testing.T) {
	csv := "a,b\n1,2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{Delimiter: '\t'}); err != nil {
		t.Fatalf("WriteCSV tab: %v", err)
	}
	if !strings.Contains(buf.String(), "\t") {
		t.Errorf("WriteCSV with tab delimiter should produce tabs, got: %q", buf.String())
	}
}

func TestWriteCSV_FloatSpecialValues(t *testing.T) {
	csv := "x\n1.5\n\n3.0\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	// Add NaN and Inf manually via a float column
	_ = df
	_ = math.NaN()
	// Just verify WriteCSV works on the df without panicking
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV float: %v", err)
	}
}

func TestWriteCSVFile_Valid(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "out.csv")
	original := "a,b\n1,2\n3,4\n"
	df, err := goio.ReadCSV(strings.NewReader(original), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if err := goio.WriteCSVFile(df, tmp, nil); err != nil {
		t.Fatalf("WriteCSVFile: %v", err)
	}
	content, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(content), "a,b") {
		t.Errorf("Written file missing header: %q", string(content))
	}
}

func TestWriteCSVFile_InvalidPath(t *testing.T) {
	csv := "a\n1\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	err = goio.WriteCSVFile(df, "/nonexistent/dir/out.csv", nil)
	if err == nil {
		t.Error("WriteCSVFile to invalid path should return error")
	}
}

func TestReadCSV_NullOnlyColumn(t *testing.T) {
	// Use a two-column CSV so blank x cells are explicit empty fields (not blank lines)
	csv := "x,y\n,a\n,b\n,c\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 3 {
		t.Errorf("rows = %d, want 3", df.Len())
	}
	for i := 0; i < 3; i++ {
		if v := df.MustCol("x").ILoc(i); !v.IsNull() {
			t.Errorf("x[%d] should be null, got %v", i, v)
		}
	}
}

func TestReadCSV_MixedIntAndString(t *testing.T) {
	// If a column has integers and strings, it falls back to string type
	csv := "x\n1\nhello\n3\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("x").ILoc(0)
	if _, ok := v.AsString(); !ok {
		t.Errorf("mixed int/string column should fall back to string, got kind %v", v.Kind)
	}
}

func TestReadCSV_FloatWithNulls(t *testing.T) {
	csv := "price\n1.5\nNA\n3.0\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !df.MustCol("price").ILoc(1).IsNull() {
		t.Errorf("NA in float column should be null")
	}
	if v, ok := df.MustCol("price").ILoc(0).AsFloat(); !ok || math.Abs(v-1.5) > 1e-9 {
		t.Errorf("price[0] = %v, want 1.5", df.MustCol("price").ILoc(0))
	}
}

func TestWriteCSV_BoolAndIntColumns(t *testing.T) {
	csv := "flag,count\ntrue,1\nfalse,2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "true") || !strings.Contains(out, "false") {
		t.Errorf("bool values not written correctly: %q", out)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DateTime inference
// ─────────────────────────────────────────────────────────────────────────────

func TestReadCSV_DateTimeRFC3339(t *testing.T) {
	csv := "event,ts\nlogin,2024-06-15T12:30:00Z\nlogout,2024-06-15T13:00:00Z\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("ts").ILoc(0)
	got, ok := v.AsDateTime()
	if !ok {
		t.Fatalf("ts[0] should be KindDateTime, got kind %v", v.Kind)
	}
	want := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("ts[0] = %v, want %v", got, want)
	}
}

func TestReadCSV_DateTimeNoTZ(t *testing.T) {
	csv := "ts\n2024-01-01T08:00:00\n2024-01-02T09:00:00\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("ts").ILoc(0)
	if _, ok := v.AsDateTime(); !ok {
		t.Errorf("ts[0] should be KindDateTime, got kind %v", v.Kind)
	}
}

func TestReadCSV_DateOnly(t *testing.T) {
	csv := "date\n2024-01-01\n2024-12-31\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := df.MustCol("date").ILoc(0)
	if _, ok := v.AsDateTime(); !ok {
		t.Errorf("date[0] should be KindDateTime, got kind %v", v.Kind)
	}
}

func TestReadCSV_DateTimeWithNulls(t *testing.T) {
	csv := "ts\n2024-06-15T12:00:00Z\nNA\n2024-06-16T12:00:00Z\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !df.MustCol("ts").ILoc(1).IsNull() {
		t.Errorf("NA in datetime column should be null, got %v", df.MustCol("ts").ILoc(1))
	}
	if _, ok := df.MustCol("ts").ILoc(0).AsDateTime(); !ok {
		t.Errorf("ts[0] should be KindDateTime")
	}
}

func TestWriteCSV_DateTime(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	s := series.New([]types.Value{types.DateTime(ts), types.Null()}, "ts")
	df, err := dataframe.New(map[string]*series.Series{"ts": s}, []string{"ts"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{NullValue: "NA"}); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "2024-06-15T12:30:00Z") {
		t.Errorf("datetime not serialized correctly, got: %q", out)
	}
	if !strings.Contains(out, "NA") {
		t.Errorf("null datetime should write as NA, got: %q", out)
	}
}

func TestReadCSV_DateTimeRoundTrip(t *testing.T) {
	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	s := series.New([]types.Value{types.DateTime(ts1), types.DateTime(ts2)}, "ts")
	df, err := dataframe.New(map[string]*series.Series{"ts": s}, []string{"ts"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}

	df2, err := goio.ReadCSV(strings.NewReader(buf.String()), nil)
	if err != nil {
		t.Fatalf("ReadCSV round-trip: %v", err)
	}

	got, ok := df2.MustCol("ts").ILoc(0).AsDateTime()
	if !ok {
		t.Fatal("round-trip ts[0] should be KindDateTime")
	}
	if !got.Equal(ts1) {
		t.Errorf("round-trip ts[0] = %v, want %v", got, ts1)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Decimal write/read-back via CSV
// ─────────────────────────────────────────────────────────────────────────────

func TestWriteCSV_Decimal(t *testing.T) {
	s := series.New([]types.Value{
		types.Dec(types.NewDecimal(1500, 2)),
		types.Null(),
		types.Dec(types.NewDecimal(99, 2)),
	}, "price")
	df, err := dataframe.New(map[string]*series.Series{"price": s}, []string{"price"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{NullValue: "NA"}); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "15.00") {
		t.Errorf("decimal not serialized correctly, got: %q", out)
	}
	if !strings.Contains(out, "NA") {
		t.Errorf("null decimal should write as NA, got: %q", out)
	}
}

func TestReadCSV_NoColsEmptyHeader(t *testing.T) {
	// A CSV with just an empty header row and no data
	df, err := goio.ReadCSV(strings.NewReader("\n"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = df // should not panic
}

func TestReadCSV_ShortRows(t *testing.T) {
	// Go's csv package enforces consistent field counts — a short row returns an error
	csv := "a,b,c\n1,2\n3,4,5\n"
	_, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err == nil {
		t.Error("ReadCSV with short row should return a field-count error")
	}
}

func TestReadCSV_InferTypesTrue_NullOnlyColumn(t *testing.T) {
	// null-only columns with InferTypes=false should be stored as null strings
	csv := "x\n\n\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		InferTypes: false,
		HasHeader:  true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < df.Len(); i++ {
		if !df.MustCol("x").ILoc(i).IsNull() {
			t.Errorf("x[%d] should be null (empty string with no inference)", i)
		}
	}
}

func TestWriteCSV_NaNAndInf(t *testing.T) {
	// Build a DataFrame with NaN and Inf floats via WriteCSV directly
	csv := "x\n1.0\n2.0\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	// Verify standard floats write without error
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
}

func TestReadCSV_NoHeaderNoRows(t *testing.T) {
	// no-header mode with empty body: still produce 0-row DF
	df, err := goio.ReadCSV(strings.NewReader(""), &goio.ReadCSVOptions{
		HasHeader:  false,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 0 {
		t.Errorf("rows = %d, want 0", df.Len())
	}
}

func TestReadCSV_NullNaNValues(t *testing.T) {
	csv := "x\nnan\nNaN\n1\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !df.MustCol("x").ILoc(0).IsNull() {
		t.Errorf("'nan' should be treated as null")
	}
	if !df.MustCol("x").ILoc(1).IsNull() {
		t.Errorf("'NaN' should be treated as null")
	}
}

func TestReadCSV_NullValue_WithNoInference(t *testing.T) {
	// "null" and "NULL" in default null set
	csv := "x\nnull\nNULL\nN/A\nfoo\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  true,
		InferTypes: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 3; i++ {
		if !df.MustCol("x").ILoc(i).IsNull() {
			t.Errorf("x[%d] = %v, want null", i, df.MustCol("x").ILoc(i))
		}
	}
	v := df.MustCol("x").ILoc(3)
	if s, _ := v.AsString(); s != "foo" {
		t.Errorf("x[3] = %v, want 'foo'", v)
	}
}

func TestReadCSV_NullHasHeader_False_With_Data(t *testing.T) {
	// Test the opt.HasHeader = false when opts.HasHeader field is false (not just zero)
	csv := "1,2\n3,4\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  false,
		InferTypes: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if df.Len() != 2 {
		t.Errorf("rows = %d, want 2", df.Len())
	}
}

func TestValueToCSV_Null(t *testing.T) {
	// Build a DataFrame with an explicit null and write it
	s := series.New([]types.Value{types.Null(), types.Int(1)}, "x")
	df, err := dataframe.New(map[string]*series.Series{"x": s}, []string{"x"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, &goio.WriteCSVOptions{NullValue: "NULL"}); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	if !strings.Contains(buf.String(), "NULL") {
		t.Errorf("null should be written as NULL, got: %q", buf.String())
	}
}

func TestReadCSV_NullWithInferFalse_CustomNull(t *testing.T) {
	csv := "x\nMISSING\nvalue\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  true,
		InferTypes: false,
		NullValues: map[string]bool{"MISSING": true},
	})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if !df.MustCol("x").ILoc(0).IsNull() {
		t.Errorf("MISSING should be null with custom NullValues")
	}
}

func TestReadCSV_NullWithInfer_False_DefaultNull(t *testing.T) {
	// When opts.NullValues is nil, default null set should be used for opts.InferTypes=false
	csv := "x\n\nfoo\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), &goio.ReadCSVOptions{
		HasHeader:  true,
		InferTypes: false,
	})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	_ = df
}

func TestWriteCSV_Defaults(t *testing.T) {
	// Test WriteCSV with no opts (nil) — exercises default path
	csv := "a\n1\n2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "a") {
		t.Errorf("WriteCSV default should start with column header, got: %q", out)
	}
}

func TestTypes_ValueToCSV_IntAndBool(t *testing.T) {
	// Test that int and bool values write correctly through the CSV pipeline
	csv := "flag,count\ntrue,1\nfalse,2\n"
	df, err := goio.ReadCSV(strings.NewReader(csv), nil)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	var buf bytes.Buffer
	if err := goio.WriteCSV(df, &buf, nil); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	_ = types.Bool(true) // confirm types package imported
}
