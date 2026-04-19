package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/io/stream"
)

// Global variables
var (
	verbosityLevel int
	showHelp       bool
	showVersion    bool
	showExamples   bool
	showGenDocs    bool
	sqlMode        bool
	joinTables     string
	timeColumns    string
	dataColumns    string
	despace        bool
	toJSONL        bool
	toParquet      bool
	outputFile     string
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
	fmt.Println("cpd - CommonPayloadData toolkit")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  cpd [flags] <file>")
	fmt.Println()
	fmt.Println("Description:")
	fmt.Println("  Convert between JSONL and CPD YAML formats")
	fmt.Println("  File extension determines conversion direction:")
	fmt.Println("    .jsonl/.json → CPD YAML")
	fmt.Println("    .yaml/.yml   → JSONL")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  cpd data.jsonl                           # Convert to CPD YAML")
	fmt.Println("  cpd data.yaml                            # Convert to JSONL")
	fmt.Println("  cpd data.yaml -sql                       # Generate SQLite DDL/INSERT")
	fmt.Println("  cpd data.jsonl -join-tables category,device  # Force join tables")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -h, --help           Show help information")
	fmt.Println("  --examples           Show example I/O with live conversion output")
	fmt.Println("  --gen-docs           Output Markdown documentation for README autogeneration")
	fmt.Println("  -V, --version        Show version information")
	fmt.Println("  -v, --verbose        Verbose output")
	fmt.Println("  -vvv                 Extra verbose output")
	fmt.Println("  -sql                 Generate SQLite DDL and INSERT statements")
	fmt.Println("  --to-jsonl           Force streaming JSONL output with carry-forward")
	fmt.Println("  --to-parquet         Convert to Parquet format (binary output)")
	fmt.Println("  -o, --output FILE    Write output to FILE (for binary formats)")
	fmt.Println("  -join-tables LIST    Comma-separated list of fields to force as join tables")
	fmt.Println("  -time-columns LIST   Comma-separated time column candidates (default: time,timestamp)")
	fmt.Println("  -data-columns LIST   Comma-separated fields to extract as columns (no join table)")
	fmt.Println("  -despace             Remove spaces after commas in data rows for compact output")
	fmt.Println()
	fmt.Println("Stdin examples:")
	fmt.Println("  cat data.yaml | cpd                          # YAML → expanded JSONL")
	fmt.Println("  cpd sparse.jsonl | cpd                       # sparse → expanded → YAML")
	fmt.Println("  cat sparse.jsonl | cpd --to-jsonl            # sparse → expanded (streaming)")
	fmt.Println("  cpd data.yaml --to-parquet -o out.parquet    # YAML → Parquet file")
	fmt.Println("  cpd data.yaml --to-parquet > out.parquet     # YAML → Parquet (stdout)")
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
		case arg == "--examples":
			showExamples = true
			i++
		case arg == "--gen-docs":
			showGenDocs = true
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
		case arg == "--to-jsonl":
			toJSONL = true
			i++
		case arg == "--to-parquet":
			toParquet = true
			i++
		case arg == "-o", arg == "--output":
			if i+1 < len(args) {
				outputFile = args[i+1]
				i += 2
			} else {
				fmt.Println("Error: -o/--output requires a value")
				os.Exit(1)
			}
		case arg == "-join-tables":
			if i+1 < len(args) {
				joinTables = args[i+1]
				i += 2 // Skip both current and next argument
			} else {
				fmt.Println("Error: -join-tables requires a value")
				os.Exit(1)
			}
		case arg == "-time-columns":
			if i+1 < len(args) {
				timeColumns = args[i+1]
				i += 2
			} else {
				fmt.Println("Error: -time-columns requires a value")
				os.Exit(1)
			}
		case arg == "-data-columns":
			if i+1 < len(args) {
				dataColumns = args[i+1]
				i += 2
			} else {
				fmt.Println("Error: -data-columns requires a value")
				os.Exit(1)
			}
		case arg == "-despace":
			despace = true
			i++
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
		fmt.Println(version)
		return
	}

	if showExamples {
		printExamples()
		return
	}

	if showGenDocs {
		printGenDocs()
		return
	}

	// Determine input source: stdin or file
	var fileData []byte
	var format string
	var err error
	var inputFile string

	if len(os.Args) < 2 {
		// No file argument, read from stdin
		if verbosityLevel > 0 {
			fmt.Fprintf(os.Stderr, "Reading from stdin...\n")
		}

		format, fileData, err = stream.DetectFormat(os.Stdin)
		if err != nil {
			fmt.Printf("Error detecting format from stdin: %v\n", err)
			os.Exit(1)
		}

		if verbosityLevel > 0 {
			fmt.Fprintf(os.Stderr, "Detected format from stdin: %s\n", format)
		}
	} else {
		// File argument provided
		inputFile = os.Args[1]

		// Check if file exists
		if _, err := os.Stat(inputFile); os.IsNotExist(err) {
			fmt.Printf("Error: file '%s' does not exist\n", inputFile)
			os.Exit(1)
		}

		// Read input file
		fileData, err = os.ReadFile(inputFile)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			os.Exit(1)
		}

		// Detect format from extension first, then content
		format = detectFormatFromExtension(inputFile)
		if format == "" {
			format, err = detectFileFormat(inputFile)
			if err != nil {
				fmt.Printf("Error detecting file format: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Set global verbosity level for codec package
	codec.VerbosityLevel = verbosityLevel

	if despace {
		codec.ArraySeparator = ","
	}

	// Parse and set time columns if specified
	if timeColumns != "" {
		var timeColumnList []string
		for _, col := range strings.Split(timeColumns, ",") {
			if trimmed := strings.TrimSpace(col); trimmed != "" {
				timeColumnList = append(timeColumnList, trimmed)
			}
		}
		codec.TimeColumns = timeColumnList
	}

	// Parse and set data columns if specified
	var dataColumnList []string
	if dataColumns != "" {
		for _, col := range strings.Split(dataColumns, ",") {
			if trimmed := strings.TrimSpace(col); trimmed != "" {
				dataColumnList = append(dataColumnList, trimmed)
			}
		}
		codec.DataColumns = dataColumnList
	}

	// Check for collisions between -join-tables and -data-columns
	if joinTables != "" && dataColumns != "" {
		joinTableSet := make(map[string]bool)
		for _, col := range strings.Split(joinTables, ",") {
			if trimmed := strings.TrimSpace(col); trimmed != "" {
				joinTableSet[trimmed] = true
			}
		}
		var collisions []string
		for _, col := range dataColumnList {
			if joinTableSet[col] {
				collisions = append(collisions, col)
			}
		}
		if len(collisions) > 0 {
			fmt.Printf("Error: fields cannot be in both -join-tables and -data-columns: %s\n", strings.Join(collisions, ", "))
			os.Exit(1)
		}
	}

	if verbosityLevel > 0 {
		fmt.Fprintf(os.Stderr, "Detected format: %s\n", format)
		if joinTables != "" {
			fmt.Fprintf(os.Stderr, "Using join tables: %s\n", joinTables)
		}
		if timeColumns != "" {
			fmt.Fprintf(os.Stderr, "Using time columns: %s\n", timeColumns)
		}
		if dataColumns != "" {
			fmt.Fprintf(os.Stderr, "Using data columns: %s\n", dataColumns)
		}
		if sqlMode {
			fmt.Fprintf(os.Stderr, "SQL mode enabled\n")
		}
	}

	var result string
	var binaryResult []byte
	var expandCarryForward bool

	// Handle Parquet mode for YAML/CPD files
	if toParquet {
		if format != "yaml" {
			fmt.Printf("Error: Parquet output only supported for YAML/CPD files\n")
			os.Exit(1)
		}
		binaryResult, err = codec.CPDToParquet(strings.NewReader(string(fileData)))
		if err != nil {
			fmt.Printf("Error converting to Parquet: %v\n", err)
			os.Exit(1)
		}

		if outputFile != "" {
			// Write to file
			if err := os.WriteFile(outputFile, binaryResult, 0644); err != nil {
				fmt.Printf("Error writing to file %s: %v\n", outputFile, err)
				os.Exit(1)
			}
			if verbosityLevel > 0 {
				fmt.Fprintf(os.Stderr, "Wrote %d bytes to %s\n", len(binaryResult), outputFile)
			}
		} else {
			// Write to stdout
			os.Stdout.Write(binaryResult)
		}
		return
	}

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
			if toJSONL {
				// Force streaming JSONL output with carry-forward
				// This is idempotent: works on both sparse and expanded JSONL
				expandCarryForward = true
			} else {
				// Detect if this is sparse JSONL (contains special-key-only records)
				if isSparseJSONL(string(fileData)) {
					// Use the carry-forward processor to combine sparse records
					expandCarryForward = true
				} else {
					// Convert expanded JSONL to CPD YAML
					if joinTables != "" {
						// Parse join tables from flag, preserving order
						joinTableFields := strings.Split(joinTables, ",")
						joinTablesMap := make(map[string]map[string]int)
						var joinTableOrder []string
						for _, field := range joinTableFields {
							field = strings.TrimSpace(field)
							if field != "" {
								joinTablesMap[field] = make(map[string]int)
								joinTableOrder = append(joinTableOrder, field)
							}
						}
						codec.JoinTableOrder = joinTableOrder
						result, err = codec.JSONLToCPDWithJoinTables(strings.NewReader(string(fileData)), joinTablesMap)
					} else {
						result, err = codec.JSONLToCPD(strings.NewReader(string(fileData)))
					}
				}
			}
		} else {
			// Convert CPD YAML to JSONL (always expanded)
			result, err = codec.CPDToJSONLUnified(strings.NewReader(string(fileData)))
		}

		// If we need to expand and carry forward special fields
		if expandCarryForward {
			result, err = expandSparseJSONLToString(fileData)
			if err != nil {
				fmt.Printf("Error expanding JSONL: %v\n", err)
				os.Exit(1)
			}
		}

		if err != nil {
			fmt.Printf("Error converting file: %v\n", err)
			os.Exit(1)
		}
	}

	// Output result to stdout
	fmt.Print(result)
}
