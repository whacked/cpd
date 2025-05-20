package main

import (
    "fmt"
    "log"
    "os"
    "gopkg.in/yaml.v3"
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

    // Pretty print the result
    fmt.Println("Parsed YAML structure:")
    fmt.Println("----------------------")
    
    // Print version if present
    if version, ok := result["_version"].(int); ok {
        fmt.Printf("Version: %d\n", version)
    }

    // Print schemas if present
    if schemas, ok := result["_schemas"].(map[string]interface{}); ok {
        fmt.Println("\nSchemas:")
        for name, schema := range schemas {
            fmt.Printf("  %s:\n", name)
            if schemaMap, ok := schema.(map[string]interface{}); ok {
                for k, v := range schemaMap {
                    fmt.Printf("    %s: %v\n", k, v)
                }
            }
        }
    }

    // Print data records if present
    if data, ok := result["data"].([]interface{}); ok {
        fmt.Println("\nRecords:")
        for i, record := range data {
            fmt.Printf("  Record %d:\n", i+1)
            if recordMap, ok := record.(map[string]interface{}); ok {
                for k, v := range recordMap {
                    fmt.Printf("    %s: %v\n", k, v)
                }
            }
        }
    }
} 