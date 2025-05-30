package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/whacked/yamdb/pkg/codec"
	jio "github.com/whacked/yamdb/pkg/io/jsonl"
	scm "github.com/whacked/yamdb/pkg/schema"
	"github.com/whacked/yamdb/pkg/types"
	"gopkg.in/yaml.v3"
)

// ColumnType represents the inferred type of a column
type ColumnType int

const (
	TypeString ColumnType = iota
	TypeInt
	TypeFloat
)

// formatRecordAsTable formats a record and its metadata as a table row
func formatRecordAsTable(record types.RecordWithMetadata, index int) []string {
	// Start with the index
	row := []string{fmt.Sprintf("%d", index)}

	// Add each field value, with nil handling
	for _, col := range record.Columns {
		val := record.Record[col.Name]
		if val == nil {
			row = append(row, "<nil>")
		} else {
			// Truncate long values to 20 chars
			strVal := fmt.Sprintf("%v", val)
			if len(strVal) > 20 {
				strVal = strVal[:17] + "..."
			}
			row = append(row, strVal)
		}
	}
	return row
}

// printTableHeader prints the table header with column names and types
func printTableHeader(columns []types.ColumnInfo) []string {
	header := []string{"index"}
	for _, col := range columns {
		header = append(header, fmt.Sprintf("%s (%s)", col.Name, types.ColumnTypeToString(col.Type)))
	}
	return header
}

// Global debug flag
var debugMode = true

func printCompactColumnsInfo(columns []types.ColumnInfo) {
	names := make([]string, len(columns))
	for i, col := range columns {
		if col.Name == "" {
			names[i] = fmt.Sprintf("F:%d", i)
		} else {
			names[i] = col.Name
		}
	}
	fmt.Printf("%s", strings.Join(names, ","))
}

// printRecordLine prints a single record in a fixed-width format
func printRecordLine(index int, record types.ValuesWithColumns, columns []types.ColumnInfo) {
	// Print index in fixed width
	fmt.Printf("%4d | ", index)

	// Print each field value with fixed width
	for i, _ := range columns {
		var strVal string
		if i < len(record.Values) {
			val := record.Values[i]
			if val == nil {
				strVal = "<nil>"
			} else {
				strVal = fmt.Sprintf("%v", val)
				if len(strVal) > 20 {
					strVal = strVal[:17] + "..."
				}
			}
		} else {
			strVal = "<nil>"
		}
		// Pad to 20 chars
		fmt.Printf("%-20s | ", strVal)
	}
	printCompactColumnsInfo(columns)
	fmt.Println()
}

// printRecordGroup prints a group of records with the same structure
func printRecordGroup(group *types.RecordGroup) {
	if !debugMode {
		// Original table display code
		table := tablewriter.NewWriter(os.Stdout)
		table.Configure(func(cfg *tablewriter.Config) {
			cfg.Header.Formatting.AutoFormat = tw.Off
		})

		// Set up headers
		headers := make([]any, len(group.Columns)+1)
		headers[0] = "index"
		for i, col := range group.Columns {
			headers[i+1] = fmt.Sprintf("%s (%s)", col.Name, types.ColumnTypeToString(col.Type))
		}
		table.Header(headers...)

		// Add rows
		for i, record := range group.Records {
			row := make([]string, len(headers))
			row[0] = fmt.Sprintf("%d", group.StartIndex+i)

			for j, _ := range group.Columns {
				var val interface{}
				if j < len(record.Values) {
					val = record.Values[j]
				}
				if val == nil {
					row[j+1] = "<nil>"
				} else {
					strVal := fmt.Sprintf("%v", val)
					if len(strVal) > 20 {
						strVal = strVal[:17] + "..."
					}
					row[j+1] = strVal
				}
			}
			table.Append(row)
		}

		table.Render()
	} else {
		// Debug mode: print header
		fmt.Printf("\n=== Record Group (starting at index %d) ===\n", group.StartIndex)
		fmt.Printf("INDX | ")
		for i, col := range group.Columns {
			var fieldName string
			if col.Name == "" {
				fieldName = fmt.Sprintf("field %d (%s)", i, types.ColumnTypeToString(col.Type))
			} else {
				fieldName = fmt.Sprintf("%s (%s)", col.Name, types.ColumnTypeToString(col.Type))
			}
			fmt.Printf("%-20s | ", fieldName)
		}
		fmt.Println()
		fmt.Println(strings.Repeat("-", 25+len(group.Columns)*23))

		// Print each record
		for i, record := range group.Records {
			printRecordLine(group.StartIndex+i, record, group.Columns)
		}
		fmt.Println()
	}
}

func ExtractOrderedKeys(input string) ([]string, error) {
	input = strings.TrimPrefix(strings.TrimSpace(input), "- ")

	dec := json.NewDecoder(strings.NewReader(input))

	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected '{', got %v", t)
	}

	var keys []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key := t.(string)
		keys = append(keys, key)

		// skip value
		_, err = dec.Token()
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// parseYamlWithOrder parses YAML while preserving field order
func parseYamlWithOrder(filename string) (map[string]interface{}, []string, error) {
	// First pass: parse YAML normally
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading file: %v", err)
	}

	var result map[string]interface{}
	if err := yaml.Unmarshal(data, &result); err != nil {
		return nil, nil, fmt.Errorf("error parsing YAML: %v", err)
	}

	// Second pass: read file line by line to get data section order
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var inDataSection bool
	var dataLines []string
	var dataIndent int

	for scanner.Scan() {
		line := scanner.Text()

		// Remove trailing comments
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
				dataLines = append(dataLines, line)
			}
		}
	}
	// Check if we have all the required keys
	requiredKeys := []string{"_schemas", "_keys", "_codecs"}
	for _, key := range requiredKeys {
		if _, ok := result[key]; !ok {
			return nil, nil, fmt.Errorf("missing required key '%s' in YAML", key)
		}
	}

	return result, dataLines, nil
}

// recordGroupToJSONSchema converts a RecordGroup to a JSON Schema
func recordGroupToJSONSchema(group *types.RecordGroup) string {
	// Start building the schema
	schema := map[string]interface{}{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type":    "array",
		"items": map[string]interface{}{
			"type":       "object",
			"properties": make(map[string]interface{}),
			"required":   make([]string, 0),
		},
	}

	// Add each column as a property
	props := schema["items"].(map[string]interface{})["properties"].(map[string]interface{})
	required := schema["items"].(map[string]interface{})["required"].([]string)

	for _, col := range group.Columns {
		// Skip unnamed columns
		if col.Name == "" {
			continue
		}

		// Determine JSON Schema type
		var jsonType string
		switch col.Type {
		case types.TypeString:
			jsonType = "string"
		case types.TypeFloat:
			jsonType = "number"
		case types.TypeInt:
			jsonType = "integer"
		case types.TypeArray:
			jsonType = "array"
		default:
			jsonType = "string"
		}

		// Add property definition
		props[col.Name] = map[string]interface{}{
			"type": jsonType,
		}

		// Add to required fields
		required = append(required, col.Name)
	}

	// Marshal to JSON with indentation
	jsonBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating schema: %v", err)
	}

	return string(jsonBytes)
}

// recordGroupToSQLiteDDL converts a RecordGroup to SQLite table DDL
func recordGroupToSQLiteDDL(group *types.RecordGroup) string {
	// Start building the DDL
	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE records (\n")

	// Check if id field already exists
	hasID := false
	for _, col := range group.Columns {
		if col.Name == "id" {
			hasID = true
			break
		}
	}

	// Add id column first if it doesn't exist
	if !hasID {
		ddl.WriteString("    id INTEGER PRIMARY KEY AUTOINCREMENT")
		if len(group.Columns) > 0 {
			ddl.WriteString(",\n")
		}
	}

	// Add each column
	for i, col := range group.Columns {
		if i > 0 {
			ddl.WriteString(",\n")
		}

		// Generate column name
		colName := col.Name
		if colName == "" {
			colName = fmt.Sprintf("field_%d", i)
		}

		// Determine SQLite type
		var sqlType string
		switch col.Type {
		case types.TypeString:
			sqlType = "TEXT"
		case types.TypeFloat:
			sqlType = "REAL"
		case types.TypeInt:
			sqlType = "INTEGER"
		case types.TypeArray:
			sqlType = "TEXT" // Store arrays as JSON strings
		default:
			sqlType = "TEXT"
		}

		// Add PRIMARY KEY for id column if it exists in schema
		if colName == "id" {
			ddl.WriteString(fmt.Sprintf("    %s %s PRIMARY KEY AUTOINCREMENT", colName, sqlType))
		} else {
			ddl.WriteString(fmt.Sprintf("    %s %s", colName, sqlType))
		}
	}

	ddl.WriteString("\n);")
	return ddl.String()
}

func runYamlDemo() {

	// Read and parse YAML with order preservation
	result, dataLines, err := parseYamlWithOrder("tests/example-1.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// print the first 5 lines of dataLines
	for i := 0; i < 5; i++ {
		fmt.Println(dataLines[i])
	}

	// Get the schema
	schemas, ok := result["_schemas"].(map[string]interface{})
	if !ok {
		log.Fatal("No _schemas found in YAML")
	}

	dataSchema, ok := schemas["data"].(map[string]interface{})
	if !ok {
		log.Fatal("No schema found for 'data' table")
	}

	itemsSchema, ok := dataSchema["items"].(map[string]interface{})
	if !ok {
		log.Fatal("No items schema found in 'data' table schema")
	}

	// Create JSON Schema validator
	schemaBytes, err := json.Marshal(itemsSchema)
	if err != nil {
		log.Fatalf("Error marshaling schema: %v", err)
	}

	fmt.Println(string(schemaBytes))

	schema, err := jsonschema.CompileString("schema.json", string(schemaBytes))
	if err != nil {
		log.Fatalf("Error compiling schema: %v", err)
	}
	fmt.Println(schema)

	// Get the data array
	dataArray, ok := result["data"].([]interface{})
	if !ok {
		log.Fatal("No 'data' array found in YAML")
	}

	if len(dataArray) == 0 {
		log.Fatal("Data array is empty")
	}

	// Initialize current schema and records for the new processing
	current := types.ValuesWithColumns{
		Values:  make([]interface{}, 0),
		Columns: make([]types.ColumnInfo, 0),
	}
	var records []types.ValuesWithColumns
	var group *types.RecordGroup

	// Process each data line
	for i, line := range dataLines {

		// trim the leading yaml array entry prefix
		cleanedLine := strings.TrimPrefix(strings.TrimSpace(line), "- ")
		fmt.Printf("\n=== record %d | %s\n", i, cleanedLine)
		fmt.Printf("---     current: %v\n", current)

		// Parse the record
		parsed, err := codec.ParseRecordToValuesWithColumns(cleanedLine)
		if err != nil {
			log.Printf("Warning: Failed to parse record at line %d: %v", i, err)
			continue
		}

		fmt.Printf("---      parsed: %v\n", parsed)

		// Supplant the record with current schema
		transformed, newSchema, changed, err := scm.SupplantRecord(current, parsed)
		if err != nil {
			log.Printf("Warning: Failed to supplant record at line %d: %v", i, err)
			continue
		}

		// fmt.Printf("--- transformed: %v\n", transformed)

		// Update current schema if it changed
		if changed {
			// fmt.Printf("--- new schema: %v\n", newSchema)
			current.Columns = newSchema
		}
		// Add the transformed record
		records = append(records, transformed)

		// Print the current state after each record
		group = &types.RecordGroup{
			Columns:    current.Columns,
			Records:    records,
			StartIndex: 0,
		}
	}

	// printRecordGroup(group)

	fmt.Printf("--- final schema: %v\n", current.Columns)

	// Print JSON Schema
	fmt.Println("\n=== JSON Schema ===")
	fmt.Println(recordGroupToJSONSchema(group))

	// Print SQLite DDL
	fmt.Println("\n=== SQLite DDL ===")
	fmt.Println(recordGroupToSQLiteDDL(group))

	codec.PrintRecordGroupAsJSONL(group)
}

func runJsonlDemo(filepath string) {
	expandAndCarrySpecialFields := true // Set to false to disable state expansion

	// Open the JSONL file
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a JSONL reader
	reader, err := jio.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}

	// Create a JSONL processor
	processor := codec.NewJSONLProcessor()

	// Create color functions
	recordHeader := color.New(color.FgHiCyan, color.Bold).SprintFunc()
	sectionHeader := color.New(color.FgHiYellow).SprintFunc()
	warnMsg := color.New(color.FgHiRed).SprintFunc()
	jsonOutput := color.New(color.FgHiWhite).SprintFunc()

	// Process each record
	i := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf(warnMsg("Warning: Failed to read record at index %d: %v"), i, err)
			continue
		}

		// Print record separator
		fmt.Printf("\n%s\n", strings.Repeat("=", 80))
		fmt.Printf("%s\n", recordHeader(fmt.Sprintf("Record %d", i)))
		fmt.Printf("%s\n", strings.Repeat("-", 80))

		// Pretty print the raw record
		jsonBytes, _ := json.MarshalIndent(record, "  ", "  ")
		fmt.Printf("%s:\n%s\n", sectionHeader("Raw JSON"), jsonOutput(string(jsonBytes)))

		result, err := processor.ProcessRecord(record)
		if err != nil {
			log.Printf(warnMsg("Warning: Failed to process record at index %d: %v"), i, err)
			continue
		}

		// Print the result with current state
		fmt.Printf("\n%s:\n", sectionHeader("Processed Result"))
		fmt.Printf("%s\n", strings.Repeat("-", 80))

		if result.Version != nil {
			fmt.Printf("%s: %d\n", sectionHeader("Version Update"), *result.Version)
			fmt.Printf("%s: %d\n", sectionHeader("Current Version"), processor.Version)
		}

		if result.Schema != nil {
			fmt.Printf("\n%s:\n", sectionHeader("Schema Update"))
			schemaBytes, _ := json.MarshalIndent(result.Schema, "  ", "  ")
			fmt.Printf("%s\n", jsonOutput(string(schemaBytes)))
			fmt.Printf("\n%s:\n", sectionHeader("Current Schema"))
			currentSchemaBytes, _ := json.MarshalIndent(processor.Schema, "  ", "  ")
			fmt.Printf("%s\n", jsonOutput(string(currentSchemaBytes)))
		}

		if result.Meta != nil {
			fmt.Printf("\n%s:\n", sectionHeader("Meta Update"))
			metaBytes, _ := json.MarshalIndent(result.Meta, "  ", "  ")
			fmt.Printf("%s\n", jsonOutput(string(metaBytes)))
			fmt.Printf("\n%s:\n", sectionHeader("Current Meta"))
			currentMetaBytes, _ := json.MarshalIndent(processor.Meta, "  ", "  ")
			fmt.Printf("%s\n", jsonOutput(string(currentMetaBytes)))
		}

		if result.Data != nil {
			fmt.Printf("\n%s:\n", sectionHeader("Data Record"))
			dataBytes, _ := json.MarshalIndent(*result.Data, "  ", "  ")
			fmt.Printf("%s\n", jsonOutput(string(dataBytes)))
			fmt.Printf("\n%s:\n", sectionHeader("Applied State"))
			fmt.Printf("  %s: %d\n", sectionHeader("Version"), *result.Version)
			if result.Schema != nil {
				fmt.Printf("  %s: present\n", sectionHeader("Schema"))
			}
			if len(result.Meta) > 0 {
				fmt.Printf("  %s: %d fields\n", sectionHeader("Meta"), len(result.Meta))
			}
		}

		i++
	}

	// Output final processed history as JSONL
	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("%s:\n", sectionHeader("Final Processed History (JSONL)"))
	fmt.Printf("%s\n", strings.Repeat("-", 80))
	for _, record := range processor.RecordHistory {
		// Skip special records
		if _, hasSchema := record["_schema"]; hasSchema {
			continue
		}
		if _, hasMeta := record["_meta"]; hasMeta {
			continue
		}

		// Create ordered output record
		outputRecord := make(map[string]interface{})
		var orderedKeys []string

		if expandAndCarrySpecialFields {
			// Add version if set
			if processor.Version > 0 {
				outputRecord["_version"] = processor.Version
				orderedKeys = append(orderedKeys, "_version")
			}

			// Add meta if present
			if len(processor.Meta) > 0 {
				outputRecord["_meta"] = processor.Meta
				orderedKeys = append(orderedKeys, "_meta")
			}
		}

		// Get schema properties if available
		if schema, ok := processor.Schema.(map[string]interface{}); ok {
			if props, ok := schema["properties"].(map[string]interface{}); ok {
				// First add fields in schema order
				for field := range props {
					if value, exists := record[field]; exists {
						outputRecord[field] = value
						orderedKeys = append(orderedKeys, field)
					}
				}
			}
		}

		// Then add any fields not in schema
		for field, value := range record {
			if !strings.HasPrefix(field, "_") {
				// Check if field exists in schema properties
				if schema, ok := processor.Schema.(map[string]interface{}); ok {
					if props, ok := schema["properties"].(map[string]interface{}); ok {
						if _, exists := props[field]; !exists {
							outputRecord[field] = value
							orderedKeys = append(orderedKeys, field)
						}
					} else {
						outputRecord[field] = value
						orderedKeys = append(orderedKeys, field)
					}
				} else {
					outputRecord[field] = value
					orderedKeys = append(orderedKeys, field)
				}
			}
		}

		// Manually build JSONL output preserving key order
		var jsonl strings.Builder
		jsonl.WriteString("{")
		for i, key := range orderedKeys {
			if i > 0 {
				jsonl.WriteString(",")
			}
			// Marshal the key
			keyBytes, _ := json.Marshal(key)
			jsonl.Write(keyBytes)
			jsonl.WriteString(":")
			// Marshal the value (using built-in marshaller for nested structures)
			valBytes, _ := json.Marshal(outputRecord[key])
			jsonl.Write(valBytes)
		}
		jsonl.WriteString("}")
		fmt.Println(jsonOutput(jsonl.String()))
	}
}

func runJsonToYamlDemo(filepath string) {
	// Open the JSONL file
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a JSONL reader
	reader, err := jio.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}

	// Create a JSONL processor
	processor := codec.NewJSONLProcessor()

	// Process each record
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: Failed to read record: %v", err)
			continue
		}

		_, err = processor.ProcessRecord(record)
		if err != nil {
			log.Printf("Warning: Failed to process record: %v", err)
			continue
		}
	}

	// Start building YAML output
	var yamlBuilder strings.Builder

	// Write _schemas section
	yamlBuilder.WriteString("_schemas:\n")
	yamlBuilder.WriteString("  data:\n")
	yamlBuilder.WriteString("    type: array\n")
	yamlBuilder.WriteString("    items:\n")
	yamlBuilder.WriteString("      type: object\n")
	yamlBuilder.WriteString("      additionalProperties: false\n")

	// Get schema properties if available
	if schema, ok := processor.Schema.(map[string]interface{}); ok {
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			// Write required fields
			yamlBuilder.WriteString("      required: [")
			first := true
			for field := range props {
				if !first {
					yamlBuilder.WriteString(", ")
				}
				yamlBuilder.WriteString(fmt.Sprintf("%q", field))
				first = false
			}
			yamlBuilder.WriteString("]\n")

			// Write properties
			yamlBuilder.WriteString("      properties:\n")
			for field, prop := range props {
				yamlBuilder.WriteString(fmt.Sprintf("        %q:\n", field))
				if propMap, ok := prop.(map[string]interface{}); ok {
					if typeVal, ok := propMap["type"]; ok {
						switch t := typeVal.(type) {
						case string:
							yamlBuilder.WriteString(fmt.Sprintf("          type: %q\n", t))
						case []interface{}:
							yamlBuilder.WriteString("          type: [")
							for i, v := range t {
								if i > 0 {
									yamlBuilder.WriteString(", ")
								}
								yamlBuilder.WriteString(fmt.Sprintf("%q", v))
							}
							yamlBuilder.WriteString("]\n")
						}
					}
					if enum, ok := propMap["enum"]; ok {
						yamlBuilder.WriteString("          enum: [")
						if enumArr, ok := enum.([]interface{}); ok {
							for i, v := range enumArr {
								if i > 0 {
									yamlBuilder.WriteString(", ")
								}
								yamlBuilder.WriteString(fmt.Sprintf("%q", v))
							}
						}
						yamlBuilder.WriteString("]\n")
					}
				}
			}
		}
	}

	// Write _keys section (empty for now)
	yamlBuilder.WriteString("\n_keys:\n  # foreign key table\n\n")

	// Write _codecs section (empty for now)
	yamlBuilder.WriteString("_codecs:\n  _:\n    category: toLowerCase\n  data:\n    # codecs will be added here\n\n")

	// Write data section
	yamlBuilder.WriteString("data:\n")
	for _, record := range processor.RecordHistory {
		// Skip special records
		if _, hasSchema := record["_schema"]; hasSchema {
			continue
		}
		if _, hasMeta := record["_meta"]; hasMeta {
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
					if value, exists := record[field]; exists {
						outputRecord[field] = value
						orderedKeys = append(orderedKeys, field)
					}
				}
			}
		}

		// Then add any fields not in schema
		for field, value := range record {
			if !strings.HasPrefix(field, "_") {
				// Check if field exists in schema properties
				if schema, ok := processor.Schema.(map[string]interface{}); ok {
					if props, ok := schema["properties"].(map[string]interface{}); ok {
						if _, exists := props[field]; !exists {
							outputRecord[field] = value
							orderedKeys = append(orderedKeys, field)
						}
					} else {
						outputRecord[field] = value
						orderedKeys = append(orderedKeys, field)
					}
				} else {
					outputRecord[field] = value
					orderedKeys = append(orderedKeys, field)
				}
			}
		}

		// Write record as YAML
		yamlBuilder.WriteString("  - ")
		if len(orderedKeys) == 1 {
			// Single value, write directly
			valBytes, _ := yaml.Marshal(outputRecord[orderedKeys[0]])
			yamlBuilder.Write(valBytes)
		} else {
			// Multiple values, write as object
			yamlBuilder.WriteString("{")
			for i, key := range orderedKeys {
				if i > 0 {
					yamlBuilder.WriteString(",")
				}
				// Marshal the key
				keyBytes, _ := json.Marshal(key)
				yamlBuilder.Write(keyBytes)
				yamlBuilder.WriteString(":")
				// Marshal the value
				valBytes, _ := json.Marshal(outputRecord[key])
				yamlBuilder.Write(valBytes)
			}
			yamlBuilder.WriteString("}")
		}
		yamlBuilder.WriteString("\n")
	}

	fmt.Println(yamlBuilder.String())
}

func main() {
	// runYamlDemo()

	// Get filepath from args or use default
	filepath := "tests/example-2.jsonl"
	if len(os.Args) > 1 {
		lastArg := os.Args[len(os.Args)-1]
		if strings.HasSuffix(lastArg, ".jsonl") {
			// Check if file exists
			if _, err := os.Stat(lastArg); err != nil {
				fmt.Printf("Error: file %s not found\n", lastArg)
				os.Exit(1)
			}
			filepath = lastArg
		}
	}

	runJsonlDemo(filepath)
	// runJsonToYamlDemo(filepath)
}
