package codec

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/whacked/yamdb/pkg/types"
	jsoniter "github.com/json-iterator/go"
)

// ExtractOrderedKeys takes a JSON-compatible string containing key-value pairs and extracts
// the keys in their original order of appearance. Since standard JSON parsing is order-agnostic,
// this function provides a way to preserve the original ordering of keys from the input string.
// The input string should be a valid JSON object.
//
// Returns a slice of strings containing the keys in order, or an error if parsing fails.
func ExtractOrderedKeys(input string) ([]string, error) {
	any := jsoniter.Get([]byte(input))
	if any.ValueType() != jsoniter.ObjectValue {
		return nil, fmt.Errorf("expected JSON object, got %v", any.ValueType())
	}

	// Get all keys at once
	keys := any.Keys()
	return keys, nil
}

func GetDataLines(filename string) ([]string, error) {

	// Second pass: read file line by line to get data section order
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var inDataSection bool
	var dataLines []string
	var dataIndent int

	for scanner.Scan() {
		line := scanner.Text()
		// Remove trailing comments and YAML record prefix
		if idx := strings.Index(line, "#"); idx != -1 {
			line = line[:idx]
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		// Find data section
		if !inDataSection && strings.HasPrefix(line, "data:") {
			inDataSection = true
			dataIndent = len(line) - len(strings.TrimLeft(line, " "))
			continue
		}

		if inDataSection {
			indent := len(line) - len(strings.TrimLeft(line, " "))
			if indent <= dataIndent && trimmed != "" {
				// End of data section
				break
			}
			if trimmed != "" {
				dataLines = append(dataLines,
					strings.TrimPrefix(
						strings.TrimSpace(line),
						"- "),
				)
			}
		}
	}
	return dataLines, nil
}

func ParseRecordToValuesWithColumns(record string) (types.ValuesWithColumns, error) {
	// First try to parse as JSON object
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(record), &obj); err == nil {
		// Object case - use order from input
		keys, err := ExtractOrderedKeys(record)
		if err != nil {
			return types.ValuesWithColumns{}, fmt.Errorf("failed to extract ordered keys: %v", err)
		}

		values := make([]interface{}, len(keys))
		columns := make([]types.ColumnInfo, len(keys))

		for i, key := range keys {
			val := obj[key]
			values[i] = val
			columns[i] = types.ColumnInfo{
				Name: key,
				Type: InferType(val),
			}
		}

		return types.ValuesWithColumns{
			Values:  values,
			Columns: columns,
		}, nil
	}

	// Try to parse as JSON array
	var arr []interface{}
	if err := json.Unmarshal([]byte(record), &arr); err == nil {
		values := make([]interface{}, len(arr))
		columns := make([]types.ColumnInfo, len(arr))

		for i, val := range arr {
			values[i] = val
			columns[i] = types.ColumnInfo{
				Name: "", // No names for array elements
				Type: InferType(val),
			}
		}

		return types.ValuesWithColumns{
			Values:  values,
			Columns: columns,
		}, nil
	}

	// Try to parse as CSV
	reader := csv.NewReader(strings.NewReader(record))
	fields, err := reader.Read()
	if err == nil {
		values := make([]interface{}, len(fields))
		columns := make([]types.ColumnInfo, len(fields))

		for i, field := range fields {
			field = strings.TrimSpace(field)
			var value interface{}
			var colType types.ColumnType

			if field == "" {
				value = nil
				colType = types.TypeString
			} else {
				// Try to parse as number - use float64 to match JSON behavior
				if matched, _ := regexp.MatchString(`^\d+$`, field); matched {
					value, _ = strconv.ParseFloat(field, 64) // Changed from Atoi to ParseFloat
					colType = types.TypeFloat
				} else if matched, _ := regexp.MatchString(`^\d*\.\d+$`, field); matched {
					value, _ = strconv.ParseFloat(field, 64)
					colType = types.TypeFloat
				} else {
					value = field
					colType = types.TypeString
				}
			}

			values[i] = value
			columns[i] = types.ColumnInfo{
				Name: "", // No names for CSV fields
				Type: colType,
			}
		}

		return types.ValuesWithColumns{
			Values:  values,
			Columns: columns,
		}, nil
	}

	return types.ValuesWithColumns{}, fmt.Errorf("failed to parse record as JSON object, array, or CSV")
}
