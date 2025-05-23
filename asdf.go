package main

import (
    "encoding/csv"
    "fmt"
    "log"
    "os"
    "regexp"
    "strconv"
    "strings"
    "encoding/json"
    "gopkg.in/yaml.v3"
    "github.com/santhosh-tekuri/jsonschema/v5"
    "github.com/whacked/yamdb/pkg/types"
    "github.com/whacked/yamdb/pkg/codec"
    "github.com/olekukonko/tablewriter"
    "github.com/olekukonko/tablewriter/tw"
    "bufio"
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

// RecordGroup represents a group of records with the same structure
type RecordGroup struct {
    Columns []types.ColumnInfo
    Records []types.RecordWithMetadata
    StartIndex int
}

// Global debug flag
var debugMode = true

func printCompactColumnsInfo(columns []types.ColumnInfo) {
    names := make([]string, len(columns))
    for i, col := range columns {
        names[i] = col.Name
    }
    fmt.Printf("%s", strings.Join(names, ","))
}

// printRecordLine prints a single record in a fixed-width format
func printRecordLine(index int, record types.RecordWithMetadata, columns []types.ColumnInfo) {
    // Print index in fixed width
    fmt.Printf("%4d | ", index)
    
    // Print each field value with fixed width
    for _, col := range columns {
        val := record.Record[col.Name]
        var strVal string
        if val == nil {
            strVal = "<nil>"
        } else {
            strVal = fmt.Sprintf("%v", val)
            if len(strVal) > 20 {
                strVal = strVal[:17] + "..."
            }
        }
        // Pad to 20 chars
        fmt.Printf("%-20s | ", strVal)
    }
    printCompactColumnsInfo(columns)
    fmt.Println()
}

// printRecordGroup prints a group of records with the same structure
func printRecordGroup(group *RecordGroup) {
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
            
            for j, col := range group.Columns {
                val := record.Record[col.Name]
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
        for _, col := range group.Columns {
            fmt.Printf("%-20s | ", col.Name)
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

func main() {
    // Read and parse YAML with order preservation
    result, dataLines, err := parseYamlWithOrder("tests/example-1.yaml")
    if err != nil {
        log.Fatal(err)
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

    // Create table evolution tracker
    tableEvol := types.NewTableEvolution()

    // Process all records
    var recordsWithMeta []types.RecordWithMetadata
    var currentGroup *RecordGroup

    for recordIndex, rawRecord := range dataArray {
        var recordWithMeta types.RecordWithMetadata
        var columns []types.ColumnInfo

        dataLine := dataLines[recordIndex]
        fmt.Println("line: ", dataLine)


        // Handle different record formats
        switch v := rawRecord.(type) {
        case map[string]interface{}:
            // Object case - use order from dataLines
            record := v
            columns = make([]types.ColumnInfo, 0, len(v))

            // Get the corresponding data line
            if recordIndex < len(dataLines) {
                // Extract field names in order from the line
                // This is a simplified version - you might need more sophisticated parsing
                fields, err := ExtractOrderedKeys(dataLine)
                if err != nil {
                    log.Printf("Error extracting ordered keys at record %d: %v", recordIndex, err)
                    continue
                }

                fmt.Printf("--- ORDERED KEYS: %+v\n", fields)

                for _, field := range fields {
                    if val, exists := v[field]; exists {
                        columns = append(columns, types.ColumnInfo{
                            Name: field,
                            Type: codec.InferType(val),
                        })
                    }
                }
            }

            recordWithMeta = types.RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        case string:
            // CSV string case
            reader := csv.NewReader(strings.NewReader(v))
            fields, err := reader.Read()
            if err != nil {
                log.Printf("Error parsing CSV at record %d: %v", recordIndex, err)
                continue
            }

            // Infer types and create record
            record := make(map[string]interface{})
            columns = make([]types.ColumnInfo, len(fields))

            for i, field := range fields {
                field = strings.TrimSpace(field)
                var value interface{}
                var colType types.ColumnType

                // Handle empty or whitespace-only fields as nil
                if field == "" {
                    value = nil
                    colType = types.TypeString
                } else {
                    // Try to parse as number
                    if matched, _ := regexp.MatchString(`^\d+$`, field); matched {
                        value, _ = strconv.Atoi(field)
                        colType = types.TypeInt
                    } else if matched, _ := regexp.MatchString(`^\d*\.\d+$`, field); matched {
                        value, _ = strconv.ParseFloat(field, 64)
                        colType = types.TypeFloat
                    } else {
                        value = field
                        colType = types.TypeString
                    }
                }

                record[fmt.Sprintf("field%d", i)] = value
                columns[i] = types.ColumnInfo{
                    Name: fmt.Sprintf("field%d", i),
                    Type: colType,
                    AutoGenerated: true,
                }
            }

            recordWithMeta = types.RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        case []interface{}:
            // Array case
            record := make(map[string]interface{})
            columns = make([]types.ColumnInfo, len(v))

            for i, val := range v {
                record[fmt.Sprintf("field%d", i)] = val
                columns[i] = types.ColumnInfo{
                    Name: fmt.Sprintf("field%d", i),
                    Type: codec.InferType(val),
                }
            }

            recordWithMeta = types.RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        default:
            log.Printf("Unexpected record type at index %d: %T", recordIndex, v)
            continue
        }

        fmt.Printf("[DEBUG] Current columns: ")
        for i, col := range columns {
            if i > 0 {
                fmt.Print(", ")
            }
            fmt.Printf("%s (%s)", col.Name, types.ColumnTypeToString(col.Type))
        }
        fmt.Println()

        // Update table evolution
        structureChanged := tableEvol.UpdateColumns(columns)
        
        // If structure changed or this is the first record, start a new group
        if structureChanged || currentGroup == nil {
            fmt.Printf("[INFO] Table structure updated at record %d\n", recordIndex)

            // Print previous group if it exists
            if currentGroup != nil {
                printRecordGroup(currentGroup)
            }
            
            // Start new group
            currentGroup = &RecordGroup{
                Columns: tableEvol.GetCurrentColumns(),
                Records: make([]types.RecordWithMetadata, 0),
                StartIndex: recordIndex,
            }
        }

        // Add record to current group
        currentGroup.Records = append(currentGroup.Records, recordWithMeta)
        recordsWithMeta = append(recordsWithMeta, recordWithMeta)

        // Record the structure used for this record
        tableEvol.RecordStructure(recordIndex, columns)
    }

    // Print final group if it exists
    if currentGroup != nil {
        printRecordGroup(currentGroup)
    }
}
