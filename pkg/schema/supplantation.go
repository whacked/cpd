package schema

import (
	"fmt"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/types"
)

// SUPPLANTATION LOGIC

// SupplantRecord takes a current schema and a new record, and returns:
// 1. The transformed record that matches the schema
// 2. The new schema after any supplantation
// 3. Whether the schema changed
func SupplantRecord(currentSchema []types.ColumnInfo, record string) (types.ValuesWithColumns, []types.ColumnInfo, bool, error) {
	// Parse the new record
	parsed, err := codec.ParseRecordToValuesWithColumns(record)
	if err != nil {
		return types.ValuesWithColumns{}, nil, false, fmt.Errorf("failed to parse record: %v", err)
	}

	// If no current schema, just return the parsed record
	if len(currentSchema) == 0 {
		return parsed, parsed.Columns, true, nil
	}

	// Create a new schema that will be our result
	newSchema := make([]types.ColumnInfo, len(currentSchema))
	copy(newSchema, currentSchema)

	// Track which positions are auto-generated
	autoAtPos := make(map[int]bool)
	for i, col := range currentSchema {
		if col.Name == "" {
			autoAtPos[i] = true
		}
	}

	// Track name to position mapping
	name2idx := make(map[string]int)
	for i, col := range currentSchema {
		if col.Name != "" {
			name2idx[col.Name] = i
		}
	}

	// Track aliases (old auto-generated names to new names)
	aliases := make(map[string]string)

	changed := false

	// Process each field in the new record
	for i, newCol := range parsed.Columns {
		// If we're beyond the current schema width, extend it
		if i >= len(newSchema) {
			newSchema = append(newSchema, newCol)
			if newCol.Name == "" {
				autoAtPos[i] = true
			} else {
				name2idx[newCol.Name] = i
			}
			changed = true
			continue
		}

		// Get the current column at this position
		currCol := &newSchema[i]

		// Case 1: Named field supplanting an auto slot
		if newCol.Name != "" && autoAtPos[i] {
			aliases[newCol.Name] = fmt.Sprintf("field%d", i)
			*currCol = types.ColumnInfo{
				Name: newCol.Name,
				Type: promote(currCol.Type, newCol.Type),
			}
			autoAtPos[i] = false
			name2idx[newCol.Name] = i
			delete(name2idx, fmt.Sprintf("field%d", i))
			changed = true
			continue
		}

		// Case 2: Same named field, maybe different type
		if newCol.Name != "" && newCol.Name == currCol.Name {
			newType := promote(currCol.Type, newCol.Type)
			if newType != currCol.Type {
				currCol.Type = newType
				changed = true
			}
			continue
		}

		// Case 3: New named field but position taken
		if newCol.Name != "" && !autoAtPos[i] {
			// Skip this field - it will be handled by the next record
			continue
		}
	}

	// Transform the values to match the new schema
	transformed := types.ValuesWithColumns{
		Values:  make([]interface{}, len(newSchema)),
		Columns: newSchema,
	}

	// Copy values from parsed record to transformed record
	for i, col := range parsed.Columns {
		if i < len(transformed.Values) {
			transformed.Values[i] = parsed.Values[i]
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
	return types.TypeFloat
}
