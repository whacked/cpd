package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/demos"
)

const (
	defaultJsonlPath    = "tests/example-2.jsonl"
	defaultMetaYamlPath = "pkg/codec/testdata/meta_version.yaml"
	defaultCpdYamlPath  = "pkg/codec/testdata/commonpayloaddata.cpd.yaml"
	defaultCpdJsonlPath = "pkg/codec/testdata/commonpayloaddata.jsonl"
)

func main() {
	// Define demo selection flag
	demoType := flag.String("demo", "", "Demo to run (yaml|jsonl|json2yaml|meta|cpd)")

	// Define path flags with defaults
	jsonlPath := flag.String("jsonl", defaultJsonlPath, "Path to JSONL file")
	metaYamlPath := flag.String("meta", defaultMetaYamlPath, "Path to meta version YAML file")
	cpdYamlPath := flag.String("cpd-yaml", defaultCpdYamlPath, "Path to CPD YAML file")
	cpdJsonlPath := flag.String("cpd-jsonl", defaultCpdJsonlPath, "Path to CPD JSONL file")
	cpdDirection := flag.String("direction", "", "CPD conversion direction (yaml2jsonl|jsonl2yaml)")

	// Parse flags
	flag.Parse()

	// Validate demo type
	if *demoType == "" {
		fmt.Println("Error: -demo flag is required")
		fmt.Println("Available demos:")
		fmt.Println("  yaml     - Run YAML demo")
		fmt.Println("  jsonl    - Run JSONL demo")
		fmt.Println("  json2yaml - Run JSON to YAML conversion demo")
		fmt.Println("  meta     - Run meta version demo")
		fmt.Println("  cpd      - Run CPD conversion demo (requires -direction)")
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
		if *cpdDirection == "" {
			fmt.Println("Error: -direction flag is required for CPD demo")
			fmt.Println("Available directions:")
			fmt.Println("  yaml2jsonl - Convert YAML to JSONL")
			fmt.Println("  jsonl2yaml - Convert JSONL to YAML")
			os.Exit(1)
		}

		switch *cpdDirection {
		case "yaml2jsonl":
			// Read YAML file
			yamlData, err := os.ReadFile(*cpdYamlPath)
			if err != nil {
				fmt.Printf("Error reading YAML file: %v\n", err)
				os.Exit(1)
			}

			// Convert to JSONL
			jsonlResult, err := codec.CPDToJSONL(strings.NewReader(string(yamlData)))
			if err != nil {
				fmt.Printf("Error converting YAML to JSONL: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("=== YAML to JSONL Conversion ===")
			fmt.Println(jsonlResult)

		case "jsonl2yaml":
			// Read JSONL file
			jsonlData, err := os.ReadFile(*cpdJsonlPath)
			if err != nil {
				fmt.Printf("Error reading JSONL file: %v\n", err)
				os.Exit(1)
			}

			// Convert to YAML
			yamlResult, err := codec.JSONLToCPD(strings.NewReader(string(jsonlData)))
			if err != nil {
				fmt.Printf("Error converting JSONL to YAML: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("=== JSONL to YAML Conversion ===")
			fmt.Println(yamlResult)

		default:
			fmt.Printf("Error: Unknown CPD direction '%s'\n", *cpdDirection)
			os.Exit(1)
		}

	default:
		fmt.Printf("Error: Unknown demo type '%s'\n", *demoType)
		os.Exit(1)
	}
}
