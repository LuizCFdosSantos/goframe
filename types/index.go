// Package types — Index implementation
//
// # What is an Index?
//
// In pandas, every Series and DataFrame has an Index — a sequence of labels
// for its rows. The default is a RangeIndex (0, 1, 2, ...) but you can use
// strings ("alice", "bob"), dates, or any hashable type.
//
// The Index is what makes operations like df.loc["alice"] work, and what
// enables alignment — when you add two Series with different row orders,
// pandas aligns them by their index labels before adding.
//
// # Our Implementation
//
// We implement a simple Index backed by:
//   1. A []Value slice for the ordered labels
//   2. A map[string]int for O(1) label → position lookup
//
// The map key is the string representation of the Value. This is a
// simplification — a production library would use a typed map per Kind.

package types

import (
	"fmt"
	"strings"
)

// Index holds ordered, labeled row identifiers.
//
// Invariant: len(labels) == len of any Series/DataFrame using this Index.
// Invariant: posMap[labels[i].String()] == i for all i (when labels are unique).
type Index struct {
	labels []Value
	posMap map[string]int // label.String() → position; only populated if unique
	unique bool           // false if any duplicate labels exist
}

// NewRangeIndex creates a default 0..n-1 integer index,
// equivalent to pandas' RangeIndex(n).
//
// This is the default index when you create a Series or DataFrame without
// specifying row labels — matching pandas' behavior.
func NewRangeIndex(n int) *Index {
	labels := make([]Value, n)
	posMap := make(map[string]int, n)
	for i := 0; i < n; i++ {
		labels[i] = Int(int64(i))
		posMap[fmt.Sprintf("%d", i)] = i
	}
	return &Index{labels: labels, posMap: posMap, unique: true}
}

// NewIndex creates an Index from a slice of Values.
//
// If any labels are duplicated, the posMap is NOT populated — label-based
// lookup will return an error for non-unique indexes, just like pandas raises
// when you try df.loc["x"] on a DataFrame with duplicate "x" rows.
func NewIndex(labels []Value) *Index {
	idx := &Index{
		labels: make([]Value, len(labels)),
		posMap: make(map[string]int, len(labels)),
		unique: true,
	}
	copy(idx.labels, labels)

	for i, lbl := range labels {
		key := lbl.String()
		if _, exists := idx.posMap[key]; exists {
			// Duplicate found — mark as non-unique and clear the map
			// to avoid misleading partial lookups.
			idx.unique = false
			idx.posMap = nil
			break
		}
		idx.posMap[key] = i
	}
	return idx
}

// NewStringIndex is a convenience constructor for string-labeled indexes.
// Equivalent to pd.Index(["a", "b", "c"]).
func NewStringIndex(labels []string) *Index {
	vals := make([]Value, len(labels))
	for i, s := range labels {
		vals[i] = Str(s)
	}
	return NewIndex(vals)
}

// Len returns the number of labels in the index.
func (idx *Index) Len() int {
	return len(idx.labels)
}

// Label returns the label at position i.
// Panics if i is out of bounds — use bounds-checked access in public APIs.
func (idx *Index) Label(i int) Value {
	return idx.labels[i]
}

// Locate returns the integer position of the given label.
// Returns -1 and an error if the label is not found or the index is non-unique.
//
// This is the underlying mechanism for df.loc[label] in pandas.
func (idx *Index) Locate(label Value) (int, error) {
	if !idx.unique {
		return -1, fmt.Errorf("index has duplicate labels; use integer position (iloc) instead")
	}
	pos, ok := idx.posMap[label.String()]
	if !ok {
		return -1, fmt.Errorf("label %s not found in index", label)
	}
	return pos, nil
}

// IsUnique returns true if all labels are distinct.
func (idx *Index) IsUnique() bool {
	return idx.unique
}

// Labels returns a copy of all labels as a slice.
// We return a copy to preserve the Index's immutability invariant.
func (idx *Index) Labels() []Value {
	out := make([]Value, len(idx.labels))
	copy(out, idx.labels)
	return out
}

// Slice returns a new Index containing only positions [start, end).
// Used when slicing a Series or DataFrame.
func (idx *Index) Slice(start, end int) *Index {
	return NewIndex(idx.labels[start:end])
}

// String returns a readable representation for debugging.
func (idx *Index) String() string {
	parts := make([]string, len(idx.labels))
	for i, lbl := range idx.labels {
		parts[i] = lbl.String()
	}
	return "Index([" + strings.Join(parts, ", ") + "])"
}
