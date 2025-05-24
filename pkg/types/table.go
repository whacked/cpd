// table.go
package types

import (
	"fmt"
)

// SchemaVersion represents a version of the table schema
type SchemaVersion struct {
	Version int
	Columns []ColumnInfo
}
type TableEvolution struct {
	version   int
	columns   []ColumnInfo   // always in canonical order
	name2idx  map[string]int // user name -> position
	autoAtPos map[int]bool   // true if slot i is still auto-generated
	alias     map[string]string
}

func NewTableEvolution() *TableEvolution {
	return &TableEvolution{
		version:   1,
		name2idx:  make(map[string]int),
		autoAtPos: make(map[int]bool),
		alias:     make(map[string]string),
	}
}

func promote(a, b ColumnType) ColumnType {
	if a == TypeString || b == TypeString {
		return TypeString
	}
	if a == TypeFloat || b == TypeFloat {
		return TypeFloat
	}
	return TypeFloat
}

// UpdateColumns is called with the columns slice that the *parser* built
// for ONE record.  The slice is already in the value order for that record.
func (te *TableEvolution) UpdateColumns(recordCols []ColumnInfo) bool {
	changed := false

	// --- first record ----------------------------------------------------
	if len(te.columns) == 0 {
		te.columns = make([]ColumnInfo, len(recordCols))
		copy(te.columns, recordCols)
		for i, col := range recordCols {
			if col.Name != "" {
				te.name2idx[col.Name] = i
			} else {
				te.autoAtPos[i] = true
			}
		}
		return true
	}

	// --- iterate over values of the new record ---------------------------
	for pos, rc := range recordCols {

		// 1.  Column fits inside current width
		if pos < len(te.columns) {

			cc := &te.columns[pos]

			// 1a.  Named key supplanting an auto slot in SAME position
			if rc.Name != "" && te.autoAtPos[pos] {
				te.alias[rc.Name] = fmt.Sprintf("field%d", pos)

				*cc = ColumnInfo{Name: rc.Name, Type: promote(cc.Type, rc.Type)}
				te.autoAtPos[pos] = false
				te.name2idx[rc.Name] = pos
				delete(te.name2idx, fmt.Sprintf("field%d", pos))
				changed = true
				continue
			}

			// 1b.  Same key already registered but maybe tighter type
			if rc.Name != "" && rc.Name == cc.Name {
				newT := promote(cc.Type, rc.Type)
				if newT != cc.Type {
					cc.Type = newT
					changed = true
				}
				continue
			}

			// 1c.  Key is new BUT slot still auto – steal it
			if rc.Name != "" && !te.autoAtPos[pos] && te.name2idx[rc.Name] == 0 {
				continue // same structure, just another nil – ignore
			}

		} else {
			// 2.  New value lies past current width -> extend
			te.columns = append(te.columns, rc)
			if rc.Name == "" {
				te.autoAtPos[pos] = true
			} else {
				te.name2idx[rc.Name] = pos
			}
			changed = true
		}
	}

	return changed
}

// convenience getters
func (te *TableEvolution) GetCurrentColumns() []ColumnInfo { return te.columns }
func (te *TableEvolution) Version() int                    { return te.version }

// positionAlreadyMapped checks if a position is already mapped to a named field
func positionAlreadyMapped(pos int, fmap map[string]int) bool {
	for _, v := range fmap {
		if v == pos {
			return true
		}
	}
	return false
}

// ValueFor returns the value for colName in the given row map,
// falling back to any recorded alias (old placeholder name).
func (te *TableEvolution) ValueFor(row map[string]interface{}, colName string) interface{} {
	if v, ok := row[colName]; ok {
		return v
	}
	if old, ok := te.alias[colName]; ok {
		return row[old]
	}
	return nil
}

/* NEW STUFF */

// ValuesWithColumns represents a record and its column information
type ValuesWithColumns struct {
	Values  []interface{}
	Columns []ColumnInfo
}
