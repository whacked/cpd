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
)

// ColumnType represents the inferred type of a column
type ColumnType int

const (
    TypeString ColumnType = iota
    TypeInt
    TypeFloat
)

func main() {
    // Read the YAML file
    data, err := os.ReadFile("tests/example-1.yaml")
    if err != nil {
        log.Fatalf("Error reading file: %v", err)
    }

    // Parse YAML
    var result map[string]interface{}
    if err := yaml.Unmarshal(data, &result); err != nil {
        log.Fatalf("Error parsing YAML: %v", err)
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

    // Get the data array
    dataArray, ok := result["data"].([]interface{})
    if !ok {
        log.Fatal("No 'data' array found in YAML")
    }

    if len(dataArray) == 0 {
        log.Fatal("Data array is empty")
    }

    // Process all records
    var recordsWithMeta []types.RecordWithMetadata
    var firstRecordColumns []types.ColumnInfo

    for recordIndex, rawRecord := range dataArray {
        var recordWithMeta types.RecordWithMetadata

        // Handle different record formats
        switch v := rawRecord.(type) {
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
            columns := make([]types.ColumnInfo, len(fields))

            for i, field := range fields {
                field = strings.TrimSpace(field)
                var value interface{}
                var colType types.ColumnType

                // Handle empty or whitespace-only fields as nil
                if field == "" {
                    value = nil
                    colType = types.TypeString // null is still considered a string type in schema
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
                }
            }

            recordWithMeta = types.RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        case []interface{}:
            // Array case
            record := make(map[string]interface{})
            columns := make([]types.ColumnInfo, len(v))

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

        case map[string]interface{}:
            // Object case
            record := v
            columns := make([]types.ColumnInfo, 0, len(v))

            for k, val := range v {
                columns = append(columns, types.ColumnInfo{
                    Name: k,
                    Type: codec.InferType(val),
                })
            }

            recordWithMeta = types.RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        default:
            log.Printf("Unexpected record type at index %d: %T", recordIndex, v)
            continue
        }

        // Store the first record's columns for reference
        if recordIndex == 0 {
            firstRecordColumns = recordWithMeta.Columns
        }

        recordsWithMeta = append(recordsWithMeta, recordWithMeta)
    }

    // Print results (limited to 10 lines)
    fmt.Println("\nInferred Structure (from first record):")
    fmt.Println("-------------------------------------")
    for i, col := range firstRecordColumns {
        if i >= 10 {
            fmt.Println("... (truncated)")
            break
        }
        fmt.Printf("Column %d: %s (%s)\n", i, col.Name, types.ColumnTypeToString(col.Type))
    }

    fmt.Printf("\nProcessed %d records\n", len(recordsWithMeta))
    fmt.Println("\nFirst few records:")
    fmt.Println("-----------------")
    
    // Print first few records
    for i, recordWithMeta := range recordsWithMeta {
        if i >= 3 { // Show first 3 records
            fmt.Println("... (more records)")
            break
        }
        fmt.Printf("\nRecord %d:\n", i+1)
        lineCount := 0
        for k, v := range recordWithMeta.Record {
            if lineCount >= 10 {
                fmt.Println("... (truncated)")
                break
            }
            fmt.Printf("  %s: %v\n", k, v)
            lineCount++
        }

        // Validate against schema
        if err := schema.Validate(recordWithMeta.Record); err != nil {
            fmt.Printf("  Schema validation failed: %v\n", err)
        } else {
            fmt.Println("  Schema validation passed")
        }
    }

    // Print JSONL output
    fmt.Println("\nJSONL Output:")
    fmt.Println("-------------")
    for _, recordWithMeta := range recordsWithMeta {
        jsonl, err := codec.RecordToJSONL(recordWithMeta)
        if err != nil {
            fmt.Printf("Error converting record to JSONL: %v\n", err)
            continue
        }
        fmt.Println(jsonl)
    }
}
