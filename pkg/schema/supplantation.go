package schema

import (
	"github.com/whacked/yamdb/pkg/types"
)

// SUPPLANTATION LOGIC

// SupplantRecord takes a current schema and a new record, and returns:
// 1. The transformed record that matches the schema
// 2. The new schema after any supplantation
// 3. Whether the schema changed
func SupplantRecord(current types.ValuesWithColumns, new types.ValuesWithColumns) (types.ValuesWithColumns, []types.ColumnInfo, bool, error) {
	// If no current schema, just return the new record
	if len(current.Columns) == 0 {
		return new, new.Columns, true, nil
	}

	// Track name to position mapping
	name2idx := make(map[string]int)
	for i, col := range current.Columns {
		if col.Name != "" {
			name2idx[col.Name] = i
		}
	}

	// Add any new fields to the mapping
	changed := false
	for _, newCol := range new.Columns {
		if newCol.Name != "" {
			if _, ok := name2idx[newCol.Name]; !ok {
				name2idx[newCol.Name] = len(name2idx)
				changed = true
			}
		}
	}

	// Build new schema and transformed values in one pass
	newLength := max(len(name2idx), len(new.Columns), len(current.Columns))
	newValues := make([]interface{}, newLength)
	newSchema := make([]types.ColumnInfo, newLength)
	transformed := types.ValuesWithColumns{
		Values:  newValues,
		Columns: newSchema,
	}

	// Create a map of field names to values from the new record
	fieldValues := make(map[string]interface{})
	for i, col := range new.Columns {
		if col.Name == "" {
			// populate unnamed fields by position
			newValues[i] = new.Values[i]
			newSchema[i] = col
		} else {
			fieldValues[col.Name] = new.Values[i]
		}
	}

	colsToProcess := make([]types.ColumnInfo, len(new.Columns))
	copy(colsToProcess, new.Columns)

	// process named fields first
	for name, idx := range name2idx {
		// Find the column info for this field
		var colInfo types.ColumnInfo
		for i, col := range colsToProcess {
			if col.Name == name {
				colInfo = col
				colsToProcess[i] = colsToProcess[len(colsToProcess)-1]
				colsToProcess = colsToProcess[:len(colsToProcess)-1]
				break
			}
		}

		// If we found the column in the new record, use its type
		if colInfo.Name != "" {
			// Check if type changed
			if idx < len(current.Columns) {
				oldType := current.Columns[idx].Type
				maybeNewType := promote(oldType, colInfo.Type)
				if maybeNewType != colInfo.Type {
					colInfo.Type = maybeNewType
					changed = true
				}
			}
			newSchema[idx] = colInfo
		} else {
			// Otherwise use the type from the current schema
			for _, col := range current.Columns {
				if col.Name == name {
					newSchema[idx] = col
					break
				}
			}
		}

		// Route the value
		if val, ok := fieldValues[name]; ok {
			transformed.Values[idx] = val
		}
	}

	// process unnamed fields next
	for i, col := range colsToProcess {
		if i < len(current.Columns) {
			currentCol := current.Columns[i]
			newType := promote(currentCol.Type, col.Type)
			if newType != currentCol.Type {
				changed = true
			}
			newSchema[i] = types.ColumnInfo{Name: currentCol.Name, Type: newType}
		} else {
			newSchema[i] = col
			changed = true // New unnamed field added
		}
	}

	return transformed, newSchema, changed, nil
}

// promote returns the more general type between two types
func promote(a, b types.ColumnType) types.ColumnType {
	if a == types.TypeString || b == types.TypeString {
		return types.TypeString
	}
	if a == types.TypeFloat || b == types.TypeFloat {
		return types.TypeFloat
	}
	if a == types.TypeArray || b == types.TypeArray {
		return types.TypeArray
	}
	if a == types.TypeObject || b == types.TypeObject {
		return types.TypeObject
	}
	return types.TypeFloat
}
