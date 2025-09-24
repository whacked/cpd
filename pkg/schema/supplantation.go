package schema

import (
	"github.com/whacked/yamdb/pkg/types"
)

// SUPPLANTATION LOGIC

// SupplantRecord applies schema information to a record, ensuring it matches the schema
func SupplantRecord(current *types.RecordGroup, new types.ValuesWithColumns) (types.ValuesWithColumns, []types.ColumnInfo, bool, error) {
	// Convert RecordGroup to ValuesWithColumns for the working logic
	var currentRecord types.ValuesWithColumns
	if current == nil || len(current.Columns) == 0 {
		// If no current schema, just return the new record
		return new, new.Columns, true, nil
	} else {
		// Extract schema from RecordGroup
		currentRecord = types.ValuesWithColumns{
			Values:  make([]interface{}, len(current.Columns)),
			Columns: current.Columns,
		}
	}

	// Use the working logic from the git history (adapted)
	// Track name to position mapping
	name2idx := make(map[string]int)
	for i, col := range currentRecord.Columns {
		if col.Name != "" {
			name2idx[col.Name] = i
		}
	}

	// Add any new fields to the mapping, handling the case where
	// current schema has unnamed fields but new record has named fields
	changed := false
	nextNewIdx := len(currentRecord.Columns)
	for i, newCol := range new.Columns {
		if newCol.Name != "" {
			if _, ok := name2idx[newCol.Name]; !ok {
				// If we have unnamed columns in current schema and this is a named field,
				// map it to the next available position
				if i < len(currentRecord.Columns) && currentRecord.Columns[i].Name == "" {
					name2idx[newCol.Name] = i
				} else {
					// This is a new field that extends the schema
					name2idx[newCol.Name] = nextNewIdx
					nextNewIdx++
				}
				changed = true
			}
		}
	}

	// Build new schema and transformed values in one pass
	newLength := max(len(name2idx), len(new.Columns), len(currentRecord.Columns))
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
			if i < len(newValues) {
				newValues[i] = new.Values[i]
				newSchema[i] = col
			}
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
			if idx < len(currentRecord.Columns) {
				oldType := currentRecord.Columns[idx].Type
				maybeNewType := promote(oldType, colInfo.Type)
				if maybeNewType != colInfo.Type {
					colInfo.Type = maybeNewType
					changed = true
				}
			}
			if idx < len(newSchema) {
				newSchema[idx] = colInfo
			}
		} else {
			// Otherwise use the type from the current schema
			for _, col := range currentRecord.Columns {
				if col.Name == name {
					if idx < len(newSchema) {
						newSchema[idx] = col
					}
					break
				}
			}
		}

		// Route the value
		if val, ok := fieldValues[name]; ok {
			if idx < len(transformed.Values) {
				transformed.Values[idx] = val
			}
		}
	}

	// process unnamed fields next
	for i, col := range colsToProcess {
		if i < len(currentRecord.Columns) {
			currentCol := currentRecord.Columns[i]
			newType := promote(currentCol.Type, col.Type)
			if newType != currentCol.Type {
				changed = true
			}
			if i < len(newSchema) {
				newSchema[i] = types.ColumnInfo{Name: currentCol.Name, Type: newType}
			}
		} else {
			if i < len(newSchema) {
				newSchema[i] = col
				changed = true // New unnamed field added
			}
		}
	}

	return transformed, newSchema, changed, nil
}

// SupplantRecordWithJoinTables applies schema and join table information to a record
func SupplantRecordWithJoinTables(current *types.RecordGroup, record types.ValuesWithColumns, joinTables map[string]map[string]int) (types.ValuesWithColumns, []types.ColumnInfo, bool, error) {
	// First apply schema supplantation
	supplanted, columns, changed, err := SupplantRecord(current, record)
	if err != nil {
		return supplanted, columns, changed, err
	}

	// Then apply join table mappings
	for i, col := range columns {
		if joinTable, ok := joinTables[col.Name]; ok {
			value := supplanted.Values[i]
			if strValue, ok := value.(string); ok {
				if id, ok := joinTable[strValue]; ok {
					supplanted.Values[i] = id
					changed = true
				}
			}
		}
	}

	return supplanted, columns, changed, nil
}

// max returns the maximum of the given integers
func max(nums ...int) int {
	if len(nums) == 0 {
		return 0
	}
	maxNum := nums[0]
	for _, num := range nums[1:] {
		if num > maxNum {
			maxNum = num
		}
	}
	return maxNum
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
