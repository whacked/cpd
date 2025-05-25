package codec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/whacked/yamdb/pkg/types"
)

// RecordToJSONL converts a RecordWithMetadata to a JSONL string, only including the record data
func RecordToJSONL(record types.RecordWithMetadata) (string, error) {
	// Marshal only the record data to JSON
	jsonBytes, err := json.Marshal(record.Record)
	if err != nil {
		return "", fmt.Errorf("error marshaling to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// JSONLToRecord converts a JSONL string back to a RecordWithMetadata by inferring column types
func JSONLToRecord(jsonl string) (types.RecordWithMetadata, error) {
	// Unmarshal the raw data
	var record map[string]interface{}
	if err := json.Unmarshal([]byte(jsonl), &record); err != nil {
		return types.RecordWithMetadata{}, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	// Infer column types from the record data
	columns := make([]types.ColumnInfo, 0, len(record))
	for name, value := range record {
		columns = append(columns, types.ColumnInfo{
			Name: name,
			Type: InferType(value),
		})
	}

	return types.RecordWithMetadata{
		Record:  record,
		Columns: columns,
	}, nil
}

// InferType determines the ColumnType of a value
func InferType(val interface{}) types.ColumnType {
	// note we simplify number parsing to all floats
	// to preserve compatibility with the built-in JSON parser
	switch v := val.(type) {
	case int, int64:
		return types.TypeFloat
	case float32, float64:
		return types.TypeFloat
	case string:
		if matched, _ := regexp.MatchString(`^\d+$`, v); matched {
			return types.TypeFloat
		}
		if matched, _ := regexp.MatchString(`^\d*\.\d+$`, v); matched {
			return types.TypeFloat
		}
		return types.TypeString
	case []interface{}:
		return types.TypeArray
	case map[string]interface{}:
		return types.TypeObject
	default:
		return types.TypeString
	}
}

// PrintRecordGroupAsJSONL prints a group of records in JSON Lines format
func PrintRecordGroupAsJSONL(group *types.RecordGroup) {
	fmt.Println("\n=== JSON Lines Format ===")
	for _, record := range group.Records {
		// Create an ordered map to hold the record data
		recordMap := make(map[string]interface{})
		orderedKeys := make([]string, 0, len(group.Columns))

		// Add each field to the map using column names as keys
		for j, col := range group.Columns {
			var val interface{}
			if j < len(record.Values) {
				val = record.Values[j]
			}

			// Use column name as key, or generate one if empty
			key := col.Name
			if key == "" {
				key = fmt.Sprintf("field_%d", j)
			}
			recordMap[key] = val
			orderedKeys = append(orderedKeys, key)
		}

		// Create a custom encoder that preserves order
		buf := new(strings.Builder)
		enc := json.NewEncoder(buf)
		enc.SetIndent("", "")

		// Write the opening brace
		buf.WriteString("{")

		// Write each field in order
		for j, key := range orderedKeys {
			if j > 0 {
				buf.WriteString(",")
			}
			// Marshal the key
			keyBytes, _ := json.Marshal(key)
			buf.Write(keyBytes)
			buf.WriteString(":")
			// Marshal the value
			valBytes, _ := json.Marshal(recordMap[key])
			buf.Write(valBytes)
		}

		// Write the closing brace
		buf.WriteString("}")

		fmt.Println(buf.String())
	}
	fmt.Println()
}
