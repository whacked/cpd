package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/whacked/yamdb/pkg/demos"
)

const (
	defaultJsonlPath    = "tests/example-2.jsonl"
	defaultMetaYamlPath = "pkg/codec/testdata/meta_version.yaml"
)

func main() {
	// Define demo selection flag
	demoType := flag.String("demo", "", "Demo to run (yaml|jsonl|json2yaml|meta)")

	// Define path flags with defaults
	jsonlPath := flag.String("jsonl", defaultJsonlPath, "Path to JSONL file")
	metaYamlPath := flag.String("meta", defaultMetaYamlPath, "Path to meta version YAML file")

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
	default:
		fmt.Printf("Error: Unknown demo type '%s'\n", *demoType)
		os.Exit(1)
	}
}
