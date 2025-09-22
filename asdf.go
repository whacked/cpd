package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/demos"
)

const (
	defaultJsonlPath    = "tests/example-2.jsonl"
	defaultMetaYamlPath = "pkg/codec/testdata/meta_version.yaml"
)

// detectFileFormat reads the beginning of a file to determine if it's JSONL or YAML format
func detectFileFormat(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	maxLines := 10 // Check first 10 lines to make determination

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineCount++

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check for JSONL format: line starts with {
		if strings.HasPrefix(line, "{") {
			return "jsonl", nil
		}

		// Check for YAML format indicators
		// Look for key-value patterns like "key: value" or "key:"
		yamlPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\s*:\s*`)
		if yamlPattern.MatchString(line) {
			return "yaml", nil
		}

		// Check for YAML array indicators
		if strings.HasPrefix(line, "-") {
			return "yaml", nil
		}

		// Check for specific CPD YAML markers
		if strings.HasPrefix(line, "_columns:") ||
			strings.HasPrefix(line, "_schemas:") ||
			strings.HasPrefix(line, "_version:") ||
			strings.HasPrefix(line, "_meta:") {
			return "yaml", nil
		}

		if lineCount >= maxLines {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading file: %w", err)
	}

	// If we can't determine, default to YAML (more common for CPD files)
	return "yaml", nil
}

func main() {
	// Define demo selection flag
	demoType := flag.String("demo", "", "Demo to run (yaml|jsonl|json2yaml|meta|cpd)")

	// Define path flags with defaults
	jsonlPath := flag.String("jsonl", defaultJsonlPath, "Path to JSONL file")
	metaYamlPath := flag.String("meta", defaultMetaYamlPath, "Path to meta version YAML file")
	cpdFilePath := flag.String("cpd-file", "", "Path to CPD file (auto-detects format)")
	joinTablesFlag := flag.String("join-tables", "", "Comma-separated list of fields to use as join tables")
	toSqlFlag := flag.Bool("toSql", false, "Convert CPD file to SQLite statements")

	// Parse flags
	flag.Parse()

	// Handle toSql flag - this takes precedence over demo type
	if *toSqlFlag {
		if *cpdFilePath == "" {
			fmt.Println("Error: -cpd-file flag is required for -toSql")
			os.Exit(1)
		}

		// Read file
		fileData, err := os.ReadFile(*cpdFilePath)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			os.Exit(1)
		}

		// Convert to SQLite
		sql, err := codec.CPDToSQLite(strings.NewReader(string(fileData)))
		if err != nil {
			fmt.Printf("Error converting to SQLite: %v\n", err)
			os.Exit(1)
		}

		fmt.Print(sql)
		return
	}

	// Validate demo type
	if *demoType == "" {
		fmt.Println("Error: -demo flag is required")
		fmt.Println("Available demos:")
		fmt.Println("  yaml     - Run YAML demo")
		fmt.Println("  jsonl    - Run JSONL demo")
		fmt.Println("  json2yaml - Run JSON to YAML conversion demo")
		fmt.Println("  meta     - Run meta version demo")
		fmt.Println("  cpd      - Run CPD conversion demo (requires -cpd-file)")
		fmt.Println("")
		fmt.Println("Or use -toSql with -cpd-file to convert CPD to SQLite statements")
		os.Exit(1)
	}

	// Run selected demo
	switch *demoType {
	case "yaml":
		demos.RunYamlDemo()
	case "jsonl":
		demos.RunJsonlDemo(*jsonlPath, 0)
	case "json2yaml":
		demos.RunJsonToYamlDemo(*jsonlPath)
	case "meta":
		demos.MetaVersionDemo(*metaYamlPath)
	case "cpd":
		if *cpdFilePath == "" {
			fmt.Println("Error: -cpd-file flag is required for CPD demo")
			fmt.Println("The file format will be auto-detected (JSONL or YAML)")
			os.Exit(1)
		}

		// Auto-detect file format
		format, err := detectFileFormat(*cpdFilePath)
		if err != nil {
			fmt.Printf("Error detecting file format: %v\n", err)
			os.Exit(1)
		}

		// Read file
		fileData, err := os.ReadFile(*cpdFilePath)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(format)
		if format == "jsonl" {
			// Convert JSONL to YAML
			var yamlResult string
			if *joinTablesFlag != "" {
				// Parse join tables from flag
				joinTableFields := strings.Split(*joinTablesFlag, ",")
				joinTables := make(map[string]map[string]int)
				for _, field := range joinTableFields {
					field = strings.TrimSpace(field)
					if field != "" {
						joinTables[field] = make(map[string]int)
					}
				}

				yamlResult, err = codec.JSONLToCPDWithJoinTables(strings.NewReader(string(fileData)), joinTables)
			} else {
				yamlResult, err = codec.JSONLToCPD(strings.NewReader(string(fileData)))
			}

			if err != nil {
				fmt.Printf("Error converting JSONL to YAML: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("=== JSONL to YAML Conversion ===")
			fmt.Printf("Detected format: %s\n", format)
			if *joinTablesFlag != "" {
				fmt.Printf("Using join tables: %s\n", *joinTablesFlag)
			}
			fmt.Println(yamlResult)
		} else {
			// Convert YAML to JSONL
			jsonlResult, err := codec.CPDToJSONL(strings.NewReader(string(fileData)))
			if err != nil {
				fmt.Printf("Error converting YAML to JSONL: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("=== YAML to JSONL Conversion ===")
			fmt.Printf("Detected format: %s\n", format)
			fmt.Println(jsonlResult)
		}

	default:
		fmt.Printf("Error: Unknown demo type '%s'\n", *demoType)
		os.Exit(1)
	}
}
