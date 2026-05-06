// Package ops provides DataFrame operations that work on multiple DataFrames.
//
// # Joins (Merges)
//
// The join/merge operation is one of the most fundamental in data analysis.
// pandas calls it merge() or join(); SQL calls it JOIN.
//
// We implement the four standard join types:
//
//	Inner Join  — only rows with matching keys in BOTH DataFrames
//	Left Join   — all rows from left; null for unmatched right rows
//	Right Join  — all rows from right; null for unmatched left rows
//	Outer Join  — all rows from both; null for unmatched rows on either side
//
// # Algorithm
//
// We use a hash join algorithm:
//  1. Build a hash map from the right DataFrame's join key → row indices
//  2. Probe: for each left row, look up its key in the hash map
//  3. Emit output rows based on matches and join type
//
// Hash join is O(n + m) where n, m are the row counts — more efficient than
// the naive O(n*m) nested loop join. pandas uses this algorithm too.
//
// # Column Naming Conflicts
//
// If left and right have columns with the same name (other than the join key),
// we add suffixes "_left" and "_right" — identical to pandas' suffixes parameter.
package ops

import (
	"fmt"

	"github.com/LuizCFdosSantos/goframe/dataframe"
	"github.com/LuizCFdosSantos/goframe/series"
	"github.com/LuizCFdosSantos/goframe/types"
)

// JoinType specifies which rows to include in a join result.
type JoinType int

const (
	// InnerJoin returns only rows with matching keys in both DataFrames.
	// Equivalent to SQL INNER JOIN or pandas merge(how='inner').
	InnerJoin JoinType = iota

	// LeftJoin returns all left rows; right side is null for non-matches.
	// Equivalent to SQL LEFT JOIN or pandas merge(how='left').
	LeftJoin

	// RightJoin returns all right rows; left side is null for non-matches.
	// Equivalent to SQL RIGHT JOIN or pandas merge(how='right').
	RightJoin

	// OuterJoin returns all rows from both sides; null for non-matches.
	// Equivalent to SQL FULL OUTER JOIN or pandas merge(how='outer').
	OuterJoin
)

// MergeOptions configures the merge operation.
type MergeOptions struct {
	// How specifies the join type (default: InnerJoin).
	How JoinType

	// LeftSuffix is appended to conflicting left column names (default: "_left").
	LeftSuffix string

	// RightSuffix is appended to conflicting right column names (default: "_right").
	RightSuffix string
}

// Merge joins two DataFrames on a common key column.
// Equivalent to pd.merge(left, right, on="key") in pandas.
//
// Example — join orders with customers:
//
//	result, err := ops.Merge(orders, customers, "customer_id", nil)
//	// result has all columns from orders + all from customers (minus dup key)
func Merge(
	left, right *dataframe.DataFrame,
	on string, // the column name to join on (must exist in both)
	opts *MergeOptions,
) (*dataframe.DataFrame, error) {

	// Apply defaults
	how := InnerJoin
	leftSuffix, rightSuffix := "_left", "_right"
	if opts != nil {
		how = opts.How
		if opts.LeftSuffix != "" {
			leftSuffix = opts.LeftSuffix
		}
		if opts.RightSuffix != "" {
			rightSuffix = opts.RightSuffix
		}
	}

	// Validate join key exists in both DataFrames
	leftKey, err := left.Col(on)
	if err != nil {
		return nil, fmt.Errorf("merge: left DataFrame: %w", err)
	}
	rightKey, err := right.Col(on)
	if err != nil {
		return nil, fmt.Errorf("merge: right DataFrame: %w", err)
	}

	// Phase 1: Build hash map from right join key → list of row indices.
	// We use a list (not a single int) to handle duplicate keys in the right DF
	// (one-to-many joins, like a customer with multiple orders).
	rightIndex := make(map[string][]int, right.Len())
	for i := 0; i < right.Len(); i++ {
		key := rightKey.ILoc(i)
		if key.IsNull() {
			continue // null keys never match (SQL semantics)
		}
		keyStr := key.String()
		rightIndex[keyStr] = append(rightIndex[keyStr], i)
	}

	// Phase 2: Probe — iterate left rows and find matches in right.
	// Build a list of (leftRow, rightRow) pairs to emit.
	// -1 means "no match" — used for outer/left/right joins.
	type rowPair struct {
		leftRow  int // -1 = no left row (right-only row in right/outer join)
		rightRow int // -1 = no right row (left-only row in left/outer join)
	}
	var pairs []rowPair

	rightMatched := make([]bool, right.Len()) // tracks which right rows got matched

	for i := 0; i < left.Len(); i++ {
		key := leftKey.ILoc(i)
		if key.IsNull() {
			// Null keys never match
			if how == LeftJoin || how == OuterJoin {
				pairs = append(pairs, rowPair{leftRow: i, rightRow: -1})
			}
			continue
		}

		matches := rightIndex[key.String()]
		if len(matches) == 0 {
			// Left row has no match in right
			if how == LeftJoin || how == OuterJoin {
				pairs = append(pairs, rowPair{leftRow: i, rightRow: -1})
			}
		} else {
			// Emit one output row per right match (handles one-to-many)
			for _, rIdx := range matches {
				pairs = append(pairs, rowPair{leftRow: i, rightRow: rIdx})
				rightMatched[rIdx] = true
			}
		}
	}

	// Phase 3: For right/outer joins, add unmatched right rows.
	if how == RightJoin || how == OuterJoin {
		for rIdx := 0; rIdx < right.Len(); rIdx++ {
			if !rightMatched[rIdx] {
				pairs = append(pairs, rowPair{leftRow: -1, rightRow: rIdx})
			}
		}
	}

	// Phase 4: Construct output columns.
	nOut := len(pairs)

	// Determine output column set and handle name conflicts.
	// The join key column appears once (from left, or right if left is null).
	leftCols := left.Columns()
	rightCols := right.Columns()

	rightColsSet := make(map[string]bool)
	for _, c := range rightCols {
		rightColsSet[c] = true
	}

	// Build final column name mapping to avoid conflicts
	type colSource struct {
		df      *dataframe.DataFrame
		srcName string
		outName string
	}
	var outputCols []colSource

	// Add all left columns
	for _, lc := range leftCols {
		outName := lc
		if lc != on && rightColsSet[lc] {
			outName = lc + leftSuffix
		}
		outputCols = append(outputCols, colSource{left, lc, outName})
	}

	// Add right columns (excluding the join key — it's already from left)
	leftColsSet := make(map[string]bool)
	for _, c := range leftCols {
		leftColsSet[c] = true
	}
	for _, rc := range rightCols {
		if rc == on {
			continue // join key already included from left
		}
		outName := rc
		if leftColsSet[rc] {
			outName = rc + rightSuffix
		}
		outputCols = append(outputCols, colSource{right, rc, outName})
	}

	// Build output column data
	resultCols := make(map[string]*series.Series, len(outputCols))
	var resultOrder []string

	for _, colSpec := range outputCols {
		srcSeries := colSpec.df.MustCol(colSpec.srcName)
		outVals := make([]types.Value, nOut)

		for i, pair := range pairs {
			var srcRow int
			if colSpec.df == left {
				srcRow = pair.leftRow
			} else {
				srcRow = pair.rightRow
			}

			// Special case: join key column gets value from whichever side has a match
			if colSpec.srcName == on {
				if pair.leftRow >= 0 {
					outVals[i] = left.MustCol(on).ILoc(pair.leftRow)
				} else {
					outVals[i] = right.MustCol(on).ILoc(pair.rightRow)
				}
				continue
			}

			if srcRow == -1 {
				outVals[i] = types.Null() // no match → null
			} else {
				outVals[i] = srcSeries.ILoc(srcRow)
			}
		}

		resultCols[colSpec.outName] = series.New(outVals, colSpec.outName)
		resultOrder = append(resultOrder, colSpec.outName)
	}

	return dataframe.New(resultCols, resultOrder)
}

// Concat vertically concatenates DataFrames (stacks rows).
// Equivalent to pd.concat([df1, df2]) in pandas.
//
// All DataFrames must have the same columns. Missing columns in any DataFrame
// will be filled with nulls if allowMissingCols=true, otherwise returns an error.
//
// Example:
//
//	combined, err := ops.Concat([]*dataframe.DataFrame{jan, feb, mar}, false)
func Concat(dfs []*dataframe.DataFrame, allowMissingCols bool) (*dataframe.DataFrame, error) {
	if len(dfs) == 0 {
		return dataframe.New(nil, nil)
	}
	if len(dfs) == 1 {
		return dfs[0], nil
	}

	// Determine the union of all column names (in order of first appearance)
	allColsOrdered := []string{}
	allColsSet := map[string]bool{}
	for _, df := range dfs {
		for _, col := range df.Columns() {
			if !allColsSet[col] {
				allColsOrdered = append(allColsOrdered, col)
				allColsSet[col] = true
			}
		}
	}

	// Validate: if not allowMissingCols, every DF must have every column
	if !allowMissingCols {
		for i, df := range dfs {
			for _, col := range allColsOrdered {
				if !df.HasColumn(col) {
					return nil, fmt.Errorf(
						"concat: DataFrame[%d] is missing column %q; "+
							"set allowMissingCols=true to fill with nulls",
						i, col,
					)
				}
			}
		}
	}

	// Build result columns by concatenating each column's values
	resultCols := make(map[string]*series.Series, len(allColsOrdered))
	for _, colName := range allColsOrdered {
		var allVals []types.Value
		for _, df := range dfs {
			if df.HasColumn(colName) {
				allVals = append(allVals, df.MustCol(colName).Values()...)
			} else {
				// Fill missing column with nulls
				for j := 0; j < df.Len(); j++ {
					allVals = append(allVals, types.Null())
				}
			}
		}
		resultCols[colName] = series.New(allVals, colName)
	}

	return dataframe.New(resultCols, allColsOrdered)
}
