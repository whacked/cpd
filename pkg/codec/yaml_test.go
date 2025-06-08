package codec

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/whacked/yamdb/pkg/io/jsonl"
	"github.com/whacked/yamdb/pkg/relational"
)

func readTestFile(t *testing.T, filename string) string {
	content, err := os.ReadFile(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filename, err)
	}
	return string(content)
}
func RemoveCommentsAndBlankLinesFromYaml(yamlStr string) string {
	var nonEmptyLines []string
	for _, line := range strings.Split(yamlStr, "\n") {
		// Remove trailing comments
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}

		// Remove trailing whitespace
		line = strings.TrimRight(line, " \t")

		// Skip empty lines and lines that are just comments
		if line == "" || strings.TrimSpace(line) == "" || strings.TrimLeft(line, " \t#") == "" {
			continue
		}

		nonEmptyLines = append(nonEmptyLines, line)
	}
	return strings.Join(nonEmptyLines, "\n")
}

func RemoveCommentsAndBlankLinesFromJsonl(jsonlStr string) string {
	var nonEmptyLines []string
	for _, line := range strings.Split(jsonlStr, "\n") {
		// Remove trailing comments
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}

		// Remove trailing whitespace
		line = strings.TrimRight(line, " \t")

		// Skip empty lines and lines that are just comments
		if line == "" || strings.TrimSpace(line) == "" || strings.TrimLeft(line, " \t/") == "" {
			continue
		}

		nonEmptyLines = append(nonEmptyLines, line)
	}
	return strings.Join(nonEmptyLines, "\n")
}

// YamlJsonlLineToQuotedJsonlLine takes a line like
//   - {name: gamma-1, temperature: 21.0, status: ok}
//
// and turns it into
// {"name": "gamma-1", "temperature": 21.0, "status": "ok"}
// special processing for "null"
func YamlJsonlLineToQuotedJsonlLine(line string) string {
	// Remove leading "- " if present
	line = strings.TrimPrefix(line, "- ")

	// Remove outer braces
	line = strings.TrimSpace(strings.Trim(line, "{}"))

	// Split into key-value pairs
	pairs := strings.Split(line, ",")

	result := make(map[string]interface{})

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Handle special cases
		switch value {
		case "null":
			result[key] = nil
		case "true", "false":
			result[key] = value == "true"
		default:
			// Try parsing as number
			if num, err := strconv.ParseFloat(value, 64); err == nil {
				result[key] = num
			} else {
				// Treat as string, removing any existing quotes
				value = strings.Trim(value, `"'`)
				result[key] = value
			}
		}
	}

	// Convert to JSON
	jsonBytes, _ := json.Marshal(result)
	return string(jsonBytes)
}

// YamlJsonlLineToQuotedJsonlArray takes a line like
//   - ["system initialized", "on", 1]
//
// and turns it into
// ["system initialized", "on", 1]
func YamlJsonlLineToQuotedJsonlArray(line string) string {
	// Remove leading "- " if present
	line = strings.TrimPrefix(line, "- ")

	// Remove outer brackets and trim
	line = strings.TrimSpace(strings.Trim(line, "[]"))

	// Split into values
	values := strings.Split(line, ",")

	// Parse each value
	var parsedValues []interface{}
	for _, value := range values {
		value = strings.TrimSpace(value)

		// Handle special cases
		switch value {
		case "null":
			parsedValues = append(parsedValues, nil)
		case "true", "false":
			parsedValues = append(parsedValues, value == "true")
		default:
			// Try parsing as number
			if num, err := strconv.ParseFloat(value, 64); err == nil {
				parsedValues = append(parsedValues, num)
			} else {
				// Treat as string, removing any existing quotes
				value = strings.Trim(value, `"'`)
				parsedValues = append(parsedValues, value)
			}
		}
	}

	// Convert to JSON
	jsonBytes, _ := json.Marshal(parsedValues)
	return string(jsonBytes)
}

// ProcessorToOriginalJsonl converts a processor's records to original JSONL format
func ProcessorToOriginalJsonl(processor *JSONLProcessor) string {
	var lines []string

	orderedKeys := make([]string, len(processor.OrderedColumns))
	for i, column := range processor.OrderedColumns {
		orderedKeys[i] = column.Name
	}

	for _, record := range processor.RecordHistory {
		recordMap := make(map[string]interface{})
		for _, column := range processor.OrderedColumns {
			if value, ok := record.Get(column.Name); ok {
				recordMap[column.Name] = value
			}
		}
		lines = append(lines, fmt.Sprintf("- %s", PrintRecordAsJSONL(recordMap, orderedKeys)))
	}
	return strings.Join(lines, "\n")
}

// ProcessorToRawJSONL converts a processor's records to raw JSONL format
func ProcessorToRawJSONL(processor *JSONLProcessor) string {
	var lines []string
	for _, record := range processor.RecordHistory {
		// Skip special records
		if _, hasSchema := record.Get("_schema"); hasSchema {
			continue
		}
		if _, hasMeta := record.Get("_meta"); hasMeta {
			continue
		}

		// Create ordered output record
		outputRecord := make(map[string]interface{})
		var orderedKeys []string

		// Get schema properties if available
		if schema, ok := processor.Schema.(map[string]interface{}); ok {
			if props, ok := schema["properties"].(map[string]interface{}); ok {
				// First add fields in schema order
				for field := range props {
					if value, exists := record.Get(field); exists {
						outputRecord[field] = value
						orderedKeys = append(orderedKeys, field)
					}
				}
			}
		}

		// Then add any fields not in schema
		for el := record.Front(); el != nil; el = el.Next() {
			field := el.Key
			if !strings.HasPrefix(field, "_") {
				// Check if field exists in schema properties
				if schema, ok := processor.Schema.(map[string]interface{}); ok {
					if props, ok := schema["properties"].(map[string]interface{}); ok {
						if _, exists := props[field]; !exists {
							outputRecord[field] = el.Value
							orderedKeys = append(orderedKeys, field)
						}
					} else {
						outputRecord[field] = el.Value
						orderedKeys = append(orderedKeys, field)
					}
				} else {
					outputRecord[field] = el.Value
					orderedKeys = append(orderedKeys, field)
				}
			}
		}

		// Marshal preserving key order
		jsonBytes, _ := json.Marshal(outputRecord)
		lines = append(lines, string(jsonBytes))
	}
	return strings.Join(lines, "\n")
}

// ProcessorToCompactedJSONL converts a processor's records to compacted JSONL format
func ProcessorToCompactedJSONL(processor *JSONLProcessor) string {

	deriver := relational.NewTableDeriver()
	err := deriver.ProcessHistory(processor.RecordHistory)
	if err != nil {
		// this is a shit return value
		fmt.Printf("ERROR: Failed to process history: %v\n", err)
		return ""
	}

	fieldInfo := deriver.GetFieldInfo()
	fmt.Printf("fieldInfo: %+v\n", fieldInfo)

	valueConverters := make(map[string]func(interface{}) interface{})
	for field, info := range fieldInfo {
		fmt.Printf("field: %q, info: %+v\n", field, info)

		if info.Category == relational.FieldCategoryOneToMany {
			fmt.Printf("  category: %q\n", relational.GetCategoryString(info.Category))
			fmt.Printf("  isArray: %v\n", info.IsArray)
			fmt.Printf("  uniqueValues: %d\n", info.UniqueValues)
			fmt.Printf("  totalRecords: %d\n", info.TotalRecords)
			// demo: render the mapping table in yaml
			fmt.Printf("\n########### DEMO ############\n%+v:\n", field)
			fmt.Printf("  %+v\n", info)

			fieldStats := deriver.FieldStats[field]
			idLookup := make(map[string]interface{})
			for key := range fieldStats.Values.Keys() {
				idLookup[key] = len(idLookup) + 1
				fmt.Printf("  %+v: %+v\n", key, idLookup[key])
			}
			fmt.Printf("#############################\n\n")

			valueConverters[field] = func(value interface{}) interface{} {
				return idLookup[value.(string)]
			}
		} else {
			valueConverters[field] = nil
		}
	}

	// TODO: this should be abstracted

	var lines []string

	fmt.Printf("columns: %v\n", processor.OrderedColumns)
	for _, record := range processor.RecordHistory {
		// Skip special records
		if _, hasSchema := record.Get("_schema"); hasSchema {
			continue
		}
		if _, hasMeta := record.Get("_meta"); hasMeta {
			continue
		}

		// Create ordered values array matching _columns
		var values []interface{}
		for _, col := range processor.OrderedColumns {
			if value, ok := record.Get(col.Name); ok {

				maybeConverter := valueConverters[col.Name]
				if maybeConverter != nil && value != nil {
					fmt.Printf("  converting %+v\n", value)
					value = maybeConverter(value)
				}

				values = append(values, value)
			} else {
				values = append(values, nil)
			}
		}

		// Marshal as array
		jsonBytes, _ := json.Marshal(values)
		lines = append(lines, string(jsonBytes))
	}
	return strings.Join(lines, "\n")
}

// ProcessorToExpandedJSONL converts a processor's records to expanded JSONL format
func ProcessorToExpandedJSONL(processor *JSONLProcessor) string {
	var lines []string

	for _, record := range processor.RecordHistory {
		// Skip special records
		if _, hasSchema := record.Get("_schema"); hasSchema {
			continue
		}
		if _, hasMeta := record.Get("_meta"); hasMeta {
			continue
		}

		// Create expanded record with column names
		expandedRecord := make(map[string]interface{})
		for _, col := range processor.OrderedColumns {
			if value, ok := record.Get(col.Name); ok {
				expandedRecord[col.Name] = value
			}
		}

		// Marshal preserving key order
		jsonBytes, _ := json.Marshal(expandedRecord)
		lines = append(lines, string(jsonBytes))
	}
	return strings.Join(lines, "\n")
}

func TestJSONLToYAMLConversion(t *testing.T) {
	tests := []struct {
		name                string
		jsonlFile           string
		yamlFile            string
		serializerFn        func(*JSONLProcessor) string
		yamlLineProcessorFn func(string) string
	}{
		/*
			{
				name:                "basic conversion",
				jsonlFile:           "basic.jsonl",
				yamlFile:            "basic.yaml",
				serializerFn:        ProcessorToRawJSONL,
				yamlLineProcessorFn: YamlJsonlLineToQuotedJsonlLine,
			},
			// */

		/*
			{
				// comment order is wrong!
				name:                "compacted conversion",
				jsonlFile:           "compacted.jsonl",
				yamlFile:            "compacted.yaml",
				serializerFn:        ProcessorToCompactedJSONL,
				yamlLineProcessorFn: YamlJsonlLineToQuotedJsonlArray,
			},
			// */

		// /*
		{
			name:                "meta version conversion",
			jsonlFile:           "meta_version.jsonl",
			yamlFile:            "meta_version.yaml",
			serializerFn:        ProcessorToExpandedJSONL,
			yamlLineProcessorFn: YamlJsonlLineToQuotedJsonlLine,
		},
		// */
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Read test files
			jsonlInput := readTestFile(t, tt.jsonlFile)

			// Create JSONL reader
			reader := jsonl.NewReader(io.NopCloser(strings.NewReader(jsonlInput)))
			defer reader.Close()

			// Read all records
			records, err := reader.ReadAll()
			assert.NoError(t, err)

			processor := NewJSONLProcessor()

			// Process each record
			for _, record := range records {
				_, err := processor.ProcessRecord(record)
				assert.NoError(t, err)
			}

			// Convert to YAML using the specified serializer
			receivedJsonl := tt.serializerFn(processor)

			// Get data lines from YAML input and convert to JSONL format
			dataLines, err := GetDataLines(filepath.Join("testdata", tt.yamlFile))
			assert.NoError(t, err)

			var expectedJsonlLines []string
			for _, line := range dataLines {
				fmt.Println("line: ", line)
				jsonlLine := tt.yamlLineProcessorFn(line)
				expectedJsonlLines = append(expectedJsonlLines, jsonlLine)
			}
			expectedJsonl := strings.Join(expectedJsonlLines, "\n")

			fmt.Println("================================================")
			fmt.Println("receivedJsonl")
			fmt.Println(receivedJsonl)
			fmt.Println("--------------------------------")
			fmt.Println("expectedJsonl")
			fmt.Println(expectedJsonl)
			fmt.Println(". . . . . . . . . . . . . . . .")

			assert.Equal(t, receivedJsonl, expectedJsonl)
		})
	}
}

/*
func TestYAMLToJSONLConversion(t *testing.T) {
	tests := []struct {
		name      string
		yamlFile  string
		jsonlFile string
		mode      string // "raw", "expanded", or "compacted"
	}{
		{
			name:      "raw conversion",
			yamlFile:  "basic.yaml",
			jsonlFile: "basic.jsonl",
			mode:      "raw",
		},
		{
			name:      "compacted conversion",
			yamlFile:  "compacted.yaml",
			jsonlFile: "compacted.jsonl",
			mode:      "compacted",
		},
		{
			name:      "expanded conversion",
			yamlFile:  "meta_version.yaml",
			jsonlFile: "meta_version.jsonl",
			mode:      "expanded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Read test files
			yamlInput := readTestFile(t, tt.yamlFile)
			expectedJSONL := readTestFile(t, tt.jsonlFile)

			// Parse YAML
			var yamlData map[string]interface{}
			err := yaml.Unmarshal([]byte(yamlInput), &yamlData)
			assert.NoError(t, err)

			// Create processor
			processor := NewJSONLProcessor()

			// Process version and meta if present
			if version, ok := yamlData["_version"].(int); ok {
				processor.Version = version
			}
			if meta, ok := yamlData["_meta"].(map[string]interface{}); ok {
				processor.Meta = meta
			}

			// Process schema if present
			if schema, ok := yamlData["_schemas"].(map[string]interface{}); ok {
				processor.Schema = schema
			}

			// Process data records
			if data, ok := yamlData["data"].([]interface{}); ok {
				for _, record := range data {
					if recordMap, ok := record.(map[string]interface{}); ok {
						_, err := processor.ProcessRecord(recordMap)
						assert.NoError(t, err)
					}
				}
			}

			// Convert back to JSONL based on mode
			var jsonlOutput string
			switch tt.mode {
			case "raw":
				jsonlOutput = ProcessorToRawJSONL(processor)
			case "compacted":
				jsonlOutput = ProcessorToCompactedJSONL(processor)
			case "expanded":
				jsonlOutput = ProcessorToExpandedJSONL(processor)
			}

			assert.Equal(t, strings.TrimSpace(expectedJSONL), strings.TrimSpace(jsonlOutput))
		})
	}
}
*/
