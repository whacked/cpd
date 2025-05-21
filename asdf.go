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
)

// ColumnType represents the inferred type of a column
type ColumnType int

const (
    TypeString ColumnType = iota
    TypeInt
    TypeFloat
)

// ColumnInfo holds metadata about a column
type ColumnInfo struct {
    Name string
    Type ColumnType
}

// RecordWithMetadata holds a record and its column information
type RecordWithMetadata struct {
    Record map[string]interface{}
    Columns []ColumnInfo
}

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
    var recordsWithMeta []RecordWithMetadata
    var firstRecordColumns []ColumnInfo

    for recordIndex, rawRecord := range dataArray {
        var recordWithMeta RecordWithMetadata

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
            columns := make([]ColumnInfo, len(fields))

            for i, field := range fields {
                field = strings.TrimSpace(field)
                var value interface{}
                var colType ColumnType

                // Handle empty or whitespace-only fields as nil
                if field == "" {
                    value = nil
                    colType = TypeString // null is still considered a string type in schema
                } else {
                    // Try to parse as number
                    if matched, _ := regexp.MatchString(`^\d+$`, field); matched {
                        value, _ = strconv.Atoi(field)
                        colType = TypeInt
                    } else if matched, _ := regexp.MatchString(`^\d*\.\d+$`, field); matched {
                        value, _ = strconv.ParseFloat(field, 64)
                        colType = TypeFloat
                    } else {
                        value = field
                        colType = TypeString
                    }
                }

                record[fmt.Sprintf("field%d", i)] = value
                columns[i] = ColumnInfo{
                    Name: fmt.Sprintf("field%d", i),
                    Type: colType,
                }
            }

            recordWithMeta = RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        case []interface{}:
            // Array case
            record := make(map[string]interface{})
            columns := make([]ColumnInfo, len(v))

            for i, val := range v {
                record[fmt.Sprintf("field%d", i)] = val
                columns[i] = ColumnInfo{
                    Name: fmt.Sprintf("field%d", i),
                    Type: inferType(val),
                }
            }

            recordWithMeta = RecordWithMetadata{
                Record:  record,
                Columns: columns,
            }

        case map[string]interface{}:
            // Object case
            record := v
            columns := make([]ColumnInfo, 0, len(v))

            for k, val := range v {
                columns = append(columns, ColumnInfo{
                    Name: k,
                    Type: inferType(val),
                })
            }

            recordWithMeta = RecordWithMetadata{
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
        fmt.Printf("Column %d: %s (%s)\n", i, col.Name, colTypeToString(col.Type))
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
        jsonl, err := recordToJSONL(recordWithMeta)
        if err != nil {
            fmt.Printf("Error converting record to JSONL: %v\n", err)
            continue
        }
        fmt.Println(jsonl)
    }
}

func inferType(val interface{}) ColumnType {
    switch v := val.(type) {
    case int, int64:
        return TypeInt
    case float32, float64:
        return TypeFloat
    case string:
        if matched, _ := regexp.MatchString(`^\d+$`, v); matched {
            return TypeInt
        }
        if matched, _ := regexp.MatchString(`^\d*\.\d+$`, v); matched {
            return TypeFloat
        }
        return TypeString
    default:
        return TypeString
    }
}

func colTypeToString(t ColumnType) string {
    switch t {
    case TypeInt:
        return "int"
    case TypeFloat:
        return "float"
    default:
        return "string"
    }
}

// recordToJSONL converts a RecordWithMetadata to a JSONL string, only including the record data
func recordToJSONL(record RecordWithMetadata) (string, error) {
    // Marshal only the record data to JSON
    jsonBytes, err := json.Marshal(record.Record)
    if err != nil {
        return "", fmt.Errorf("error marshaling to JSON: %w", err)
    }

    return string(jsonBytes), nil
}

// jsonlToRecord converts a JSONL string back to a RecordWithMetadata by inferring column types
func jsonlToRecord(jsonl string) (RecordWithMetadata, error) {
    // Unmarshal the raw data
    var record map[string]interface{}
    if err := json.Unmarshal([]byte(jsonl), &record); err != nil {
        return RecordWithMetadata{}, fmt.Errorf("error unmarshaling JSON: %w", err)
    }

    // Infer column types from the record data
    columns := make([]ColumnInfo, 0, len(record))
    for name, value := range record {
        columns = append(columns, ColumnInfo{
            Name: name,
            Type: inferType(value),
        })
    }

    return RecordWithMetadata{
        Record:  record,
        Columns: columns,
    }, nil
}

// stringToColumnType converts a string type name to ColumnType
func stringToColumnType(typeStr string) ColumnType {
    switch typeStr {
    case "int":
        return TypeInt
    case "float":
        return TypeFloat
    default:
        return TypeString
    }
}