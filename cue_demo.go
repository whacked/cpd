//go:build cue

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/whacked/yamdb/pkg/codec"
	"gopkg.in/yaml.v3"
)

// cueExpandToYAML expands a CUE schema string to a JSON Schema rendered as YAML.
func cueExpandToYAML(src string) (string, error) {
	schema, err := codec.ExpandCUESchema(src)
	if err != nil {
		return "", err
	}
	// Round-trip through JSON to flatten any unexportable types before YAML marshal.
	jsonBytes, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	var plain interface{}
	if err := json.Unmarshal(jsonBytes, &plain); err != nil {
		return "", fmt.Errorf("unmarshal: %w", err)
	}
	out, err := yaml.Marshal(plain)
	if err != nil {
		return "", fmt.Errorf("yaml: %w", err)
	}
	return string(out), nil
}

// cueDemoSection renders one CUE schema example as a markdown section.
func cueDemoSection(ex codec.CUESchemaExample) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "### %s\n\n%s\n\n", ex.Name, ex.Description)

	src, err := codec.CUEExamplesFS.ReadFile(ex.Filename)
	if err != nil {
		fmt.Fprintf(&sb, "> error reading example: %v\n\n", err)
		return sb.String()
	}

	fmt.Fprintf(&sb, "```cue\n%s```\n\nexpands to:\n\n", src)

	expanded, err := cueExpandToYAML(string(src))
	if err != nil {
		fmt.Fprintf(&sb, "> expansion error: %v\n\n", err)
		return sb.String()
	}
	fmt.Fprintf(&sb, "```yaml\n%s```\n\n", expanded)
	return sb.String()
}

// cueArrayShorthandSection shows the data...: key suffix with the embedded CPD example.
func cueArrayShorthandSection() string {
	var sb strings.Builder
	fmt.Fprintln(&sb, "### Array shorthand: `data...:` key suffix")
	fmt.Fprintln(&sb)
	fmt.Fprintln(&sb, "Append `...` to a table name in `_schemas` to declare that the schema")
	fmt.Fprintln(&sb, "describes a **single item**; the table is automatically wrapped as")
	fmt.Fprintln(&sb, "`{type: array, items: <schema>}`. Table names must not naturally end in `...`.")
	fmt.Fprintln(&sb)

	cpd, err := examplesFS.ReadFile("examples/cue_schemas.cpd.yaml")
	if err != nil {
		fmt.Fprintf(&sb, "> error reading example: %v\n\n", err)
		return sb.String()
	}
	fmt.Fprintf(&sb, "```yaml\n%s```\n\n", cpd)

	output, err := codec.CPDToJSONLUnified(strings.NewReader(string(cpd)))
	if err != nil {
		fmt.Fprintf(&sb, "> conversion error: %v\n\n", err)
		return sb.String()
	}
	fmt.Fprintf(&sb, "`cat cue_schemas.cpd.yaml | cpd -tags cue`\n\n```jsonl\n%s```\n\n", output)
	return sb.String()
}

// printCUESchemaExamples prints the CUE schema reference section.
// Each syntax feature is produced by a dedicated function; all data comes from embedded files.
func printCUESchemaExamples() {
	fmt.Println("## CUE Schema Syntax")
	fmt.Println()
	fmt.Println("When a `_schemas` value is a YAML block scalar (string), it is parsed as CUE")
	fmt.Println("and converted to JSON Schema before validation. Requires `-tags cue` build.")
	fmt.Println()

	for _, ex := range codec.CUESchemaExamples {
		out := cueDemoSection(ex)
		if _, err := fmt.Print(out); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}

	fmt.Print(cueArrayShorthandSection())
}
