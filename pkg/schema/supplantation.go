package schema

import (
	"fmt"

	"github.com/whacked/yamdb/pkg/types"
)

// SUPPLANTATION LOGIC

// SupplantRecord applies schema information to a record, ensuring it matches the schema
func SupplantRecord(current *types.RecordGroup, record types.ValuesWithColumns) (types.ValuesWithColumns, []types.ColumnInfo, bool, error) {
	// If no current schema, use the record's schema
	if current == nil || len(current.Columns) == 0 {
		return record, record.Columns, true, nil
	}

	// Create new values array matching current schema
	newValues := make([]interface{}, len(current.Columns))
	changed := false

	// Map record values to schema columns
	for i, col := range current.Columns {
		// Find matching column in record
		var value interface{}
		for j, recordCol := range record.Columns {
			if recordCol.Name == col.Name {
				value = record.Values[j]
				break
			}
		}

		// If value not found, use nil
		if value == nil {
			changed = true
			continue
		}

		// Convert value to match schema type
		switch col.Type {
		case types.TypeFloat:
			switch v := value.(type) {
			case float64:
				newValues[i] = v
			case int:
				newValues[i] = float64(v)
			case string:
				// Try to parse as float
				var f float64
				_, err := fmt.Sscanf(v, "%f", &f)
				if err == nil {
					newValues[i] = f
					changed = true
				} else {
					newValues[i] = nil
					changed = true
				}
			default:
				newValues[i] = nil
				changed = true
			}
		case types.TypeString:
			switch v := value.(type) {
			case string:
				newValues[i] = v
			default:
				newValues[i] = fmt.Sprintf("%v", v)
				changed = true
			}
		case types.TypeArray:
			switch v := value.(type) {
			case []interface{}:
				newValues[i] = v
			default:
				newValues[i] = []interface{}{v}
				changed = true
			}
		case types.TypeObject:
			switch v := value.(type) {
			case map[string]interface{}:
				newValues[i] = v
			default:
				newValues[i] = map[string]interface{}{"value": v}
				changed = true
			}
		default:
			newValues[i] = value
		}
	}

	return types.ValuesWithColumns{
		Values:  newValues,
		Columns: current.Columns,
	}, current.Columns, changed, nil
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
