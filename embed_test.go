package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/whacked/yamdb/pkg/codec"
)

func TestEmbeddedExamplesMatchGoldenOutputs(t *testing.T) {
	idx, err := loadExamplesIndex()
	if err != nil {
		t.Fatalf("load examples index: %v", err)
	}

	for _, ex := range idx.Examples {
		t.Run(ex.Name, func(t *testing.T) {
			inputData, err := examplesFS.ReadFile("examples/" + ex.Input)
			if err != nil {
				t.Fatalf("read input: %v", err)
			}

			output, err := runExample(ex, inputData)
			if err != nil {
				t.Fatalf("run example: %v", err)
			}

			if ex.Expected == "" {
				t.Fatalf("example %q is missing expected output", ex.Name)
			}
			expectedData, err := examplesFS.ReadFile("examples/" + ex.Expected)
			if err != nil {
				t.Fatalf("read expected output: %v", err)
			}

			expected := string(expectedData)
			if output != expected {
				t.Fatalf("output mismatch\nwant:\n%s\ngot:\n%s", expected, output)
			}
		})
	}
}

func TestEmbeddedExamplesRemainConvertible(t *testing.T) {
	idx, err := loadExamplesIndex()
	if err != nil {
		t.Fatalf("load examples index: %v", err)
	}

	for _, ex := range idx.Examples {
		t.Run(ex.Name, func(t *testing.T) {
			inputData, err := examplesFS.ReadFile("examples/" + ex.Input)
			if err != nil {
				t.Fatalf("read input: %v", err)
			}

			output, err := runExample(ex, inputData)
			if err != nil {
				t.Fatalf("run example: %v", err)
			}

			switch ex.OutputFormat {
			case "yaml":
				if _, err := codec.CPDToJSONLUnified(strings.NewReader(output)); err != nil {
					t.Fatalf("expected YAML output to expand to JSONL: %v", err)
				}
			case "jsonl":
				for i, line := range nonEmptyLines(output) {
					var record map[string]interface{}
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatalf("expected JSONL output line %d to be valid JSON: %v", i+1, err)
					}
				}
			default:
				t.Fatalf("unsupported output format %q", ex.OutputFormat)
			}
		})
	}
}

func nonEmptyLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
