package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"gopkg.in/yaml.v3"
)

// Global variables
var (
	verbosityLevel int
	showHelp       bool
	showVersion    bool
	sqlMode        bool
	joinTables     string
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

// detectFormatFromExtension determines format from file extension
func detectFormatFromExtension(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".jsonl", ".json":
		return "jsonl"
	case ".yaml", ".yml":
		return "yaml"
	default:
		return ""
	}
}

// isSparseJSONL determines if a JSONL file contains sparse records
// with special-key-only lines (keys starting with underscore)
func isSparseJSONL(data string) bool {
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		// Parse as JSON to extract keys
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue // Skip malformed lines
		}

		// Check if this record contains ONLY special keys (starting with _)
		if len(record) == 0 {
			continue
		}

		hasOnlySpecialKeys := true
		hasAtLeastOneKey := false

		for key := range record {
			hasAtLeastOneKey = true
			if !strings.HasPrefix(key, "_") {
				hasOnlySpecialKeys = false
				break
			}
		}

		// If we found a record with only special keys, it's scattered JSONL
		if hasAtLeastOneKey && hasOnlySpecialKeys {
			return true
		}
	}

	return false
}

func printUsage() {
	fmt.Println("ydb - CommonPayloadData toolkit")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  ydb [flags] <file>")
	fmt.Println()
	fmt.Println("Description:")
	fmt.Println("  Convert between JSONL and CPD YAML formats")
	fmt.Println("  File extension determines conversion direction:")
	fmt.Println("    .jsonl/.json → CPD YAML")
	fmt.Println("    .yaml/.yml   → JSONL")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  ydb data.jsonl                           # Convert to CPD YAML")
	fmt.Println("  ydb data.yaml                            # Convert to JSONL")
	fmt.Println("  ydb data.yaml -sql                       # Generate SQLite DDL/INSERT")
	fmt.Println("  ydb data.jsonl -join-tables category,device  # Force join tables")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -h, --help           Show help information")
	fmt.Println("  -V, --version        Show version information")
	fmt.Println("  -v, --verbose        Verbose output")
	fmt.Println("  -vvv                 Extra verbose output")
	fmt.Println("  -sql                 Generate SQLite DDL and INSERT statements")
	fmt.Println("  -join-tables string  Comma-separated list of fields to force as join tables")
}

func parseFlags() {
	// Custom flag parsing to handle -vvv and mixed flag/file order
	args := os.Args[1:]
	var fileArg string

	i := 0
	for i < len(args) {
		arg := args[i]

		switch {
		case arg == "-h" || arg == "--help":
			showHelp = true
			i++
		case arg == "-V" || arg == "--version":
			showVersion = true
			i++
		case arg == "-v" || arg == "--verbose":
			verbosityLevel = 1
			i++
		case arg == "-vvv":
			verbosityLevel = 3
			i++
		case arg == "-sql":
			sqlMode = true
			i++
		case arg == "-join-tables":
			if i+1 < len(args) {
				joinTables = args[i+1]
				i += 2 // Skip both current and next argument
			} else {
				fmt.Println("Error: -join-tables requires a value")
				os.Exit(1)
			}
		case strings.HasPrefix(arg, "-"):
			fmt.Printf("Error: Unknown flag %s\n", arg)
			os.Exit(1)
		default:
			// This is the file argument
			if fileArg == "" {
				fileArg = arg
				i++
			} else {
				fmt.Printf("Error: Multiple file arguments not supported: %s\n", arg)
				os.Exit(1)
			}
		}
	}

	// Update os.Args to contain only the program name and file argument
	if fileArg != "" {
		os.Args = []string{os.Args[0], fileArg}
	} else {
		os.Args = []string{os.Args[0]}
	}
}


func main() {
	// Parse custom flags first
	parseFlags()

	// Handle help and version flags
	if showHelp {
		printUsage()
		return
	}

	if showVersion {
		fmt.Println("ydb version 0.0.1")
		return
	}

	// Check for file argument
	if len(os.Args) < 2 {
		fmt.Println("Error: file argument is required")
		printUsage()
		os.Exit(1)
	}

	inputFile := os.Args[1]

	// Check if file exists
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		fmt.Printf("Error: file '%s' does not exist\n", inputFile)
		os.Exit(1)
	}

	// Read input file
	fileData, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Detect format from extension first, then content
	format := detectFormatFromExtension(inputFile)
	if format == "" {
		format, err = detectFileFormat(inputFile)
		if err != nil {
			fmt.Printf("Error detecting file format: %v\n", err)
			os.Exit(1)
		}
	}

	// Set global verbosity level for codec package
	codec.VerbosityLevel = verbosityLevel

	if verbosityLevel > 0 {
		fmt.Fprintf(os.Stderr, "Detected format: %s\n", format)
		if joinTables != "" {
			fmt.Fprintf(os.Stderr, "Using join tables: %s\n", joinTables)
		}
		if sqlMode {
			fmt.Fprintf(os.Stderr, "SQL mode enabled\n")
		}
	}

	var result string
	var expandCarryForward bool

	// Handle SQL mode for YAML/CPD files
	if sqlMode {
		if format != "yaml" {
			fmt.Printf("Error: SQL mode only supported for YAML/CPD files\n")
			os.Exit(1)
		}
		result, err = codec.CPDToSQLite(strings.NewReader(string(fileData)))
		if err != nil {
			fmt.Printf("Error converting to SQLite: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Regular conversion mode
		if format == "jsonl" {
			// Detect if this is sparse JSONL (contains special-key-only records)
			if isSparseJSONL(string(fileData)) {
				// Use the carry-forward processor to combine sparse records
				expandCarryForward = true
			} else {
				// Convert JSONL to CPD YAML
				if joinTables != "" {
					// Parse join tables from flag
					joinTableFields := strings.Split(joinTables, ",")
					joinTablesMap := make(map[string]map[string]int)
					for _, field := range joinTableFields {
						field = strings.TrimSpace(field)
						if field != "" {
							joinTablesMap[field] = make(map[string]int)
						}
					}
					result, err = codec.JSONLToCPDWithJoinTables(strings.NewReader(string(fileData)), joinTablesMap)
				} else {
					result, err = codec.JSONLToCPD(strings.NewReader(string(fileData)))
				}
			}
		} else {
			// Convert CPD YAML to JSONL
			result, err = codec.CPDToJSONLUnified(strings.NewReader(string(fileData)))
		}

		// If we need to expand and carry forward special fields
		if expandCarryForward {
			processor := codec.NewJSONLProcessor()
			scanner := bufio.NewScanner(strings.NewReader(string(fileData)))
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" || strings.HasPrefix(line, "//") {
					continue
				}

				// Parse the line as YAML to preserve order
				var node yaml.Node
				if err := yaml.Unmarshal([]byte(line), &node); err != nil {
					fmt.Printf("Error parsing JSONL line: %v\n", err)
					os.Exit(1)
				}

				record := orderedmapjson.NewAnyOrderedMap()
				if err := yamlutil.ConvertNodeToOrderedMap(&node, record); err != nil {
					fmt.Printf("Error converting to ordered map: %v\n", err)
					os.Exit(1)
				}

				if _, err := processor.ProcessRecord(record); err != nil {
					fmt.Printf("Error processing record: %v\n", err)
					os.Exit(1)
				}
			}

			// Convert to expanded JSONL with carry-forward
			result = processor.ToExpandedJSONL(true)
		}

		if err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}
	}

	// Output result to stdout
	fmt.Print(result)
}
