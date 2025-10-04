package codec

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_SparseJSONL_To_YAML_To_ExpandedJSONL verifies the round-trip:
// sparse JSONL → expanded JSONL → YAML → expanded JSONL
// The final output should match the intermediate expanded JSONL
func TestRoundTrip_SparseJSONL_To_YAML_To_ExpandedJSONL(t *testing.T) {
	// This test expects null values to be preserved
	defer func() { OmitMissingColumns = true }()
	OmitMissingColumns = false

	// Read sparse JSONL
	sparseData, err := os.ReadFile("testdata/meta_version.jsonl")
	require.NoError(t, err)

	// Step 1: sparse → expanded JSONL
	expandedJSONL := processJSONLWithCarryForward(t, string(sparseData))

	// Step 2: expanded JSONL → YAML
	yaml, err := JSONLToCPD(strings.NewReader(expandedJSONL))
	require.NoError(t, err, "should convert expanded JSONL to YAML")

	// Step 3: YAML → expanded JSONL
	finalJSONL, err := CPDToJSONLUnified(strings.NewReader(yaml))
	require.NoError(t, err, "should convert YAML back to JSONL")

	// Normalize for comparison
	expected := normalizeJSONL(expandedJSONL)
	actual := normalizeJSONL(finalJSONL)

	assert.Equal(t, expected, actual, "round-trip should preserve data")
}

// TestRoundTrip_YAML_To_ExpandedJSONL_To_YAML verifies:
// YAML → expanded JSONL → YAML → expanded JSONL
// The two JSONL outputs should match
func TestRoundTrip_YAML_To_ExpandedJSONL_To_YAML(t *testing.T) {
	// Read YAML
	yamlData, err := os.ReadFile("testdata/meta_version.yaml")
	require.NoError(t, err)

	// Step 1: YAML → expanded JSONL
	jsonl1, err := CPDToJSONLUnified(strings.NewReader(string(yamlData)))
	require.NoError(t, err)

	// Step 2: expanded JSONL → YAML
	yaml2, err := JSONLToCPD(strings.NewReader(jsonl1))
	require.NoError(t, err)

	// Step 3: YAML → expanded JSONL again
	jsonl2, err := CPDToJSONLUnified(strings.NewReader(yaml2))
	require.NoError(t, err)

	// The two JSONL outputs should match
	expected := normalizeJSONL(jsonl1)
	actual := normalizeJSONL(jsonl2)

	assert.Equal(t, expected, actual, "YAML round-trip should preserve JSONL output")
}

// TestRoundTrip_MixedMode_YAML_Conversion verifies mixed-mode YAML
// (arrays and objects) converts correctly to JSONL and back
func TestRoundTrip_MixedMode_YAML(t *testing.T) {
	// Read mixed-mode YAML
	yamlData, err := os.ReadFile("testdata/mixed_mode.yaml")
	require.NoError(t, err)

	// Step 1: YAML → JSONL
	jsonl, err := CPDToJSONLUnified(strings.NewReader(string(yamlData)))
	require.NoError(t, err)

	// Verify we got records
	lines := strings.Split(strings.TrimSpace(jsonl), "\n")
	assert.GreaterOrEqual(t, len(lines), 3, "should have multiple records")

	// Step 2: JSONL → YAML
	yaml2, err := JSONLToCPD(strings.NewReader(jsonl))
	require.NoError(t, err)

	// Step 3: YAML → JSONL again
	jsonl2, err := CPDToJSONLUnified(strings.NewReader(yaml2))
	require.NoError(t, err)

	// The two JSONL outputs should match
	expected := normalizeJSONL(jsonl)
	actual := normalizeJSONL(jsonl2)

	assert.Equal(t, expected, actual, "mixed-mode round-trip should preserve data")
}

// TestRoundTrip_ExpandedJSONL_Stability verifies that expanded JSONL
// remains stable through multiple conversions
func TestRoundTrip_ExpandedJSONL_Stability(t *testing.T) {
	// Read expanded JSONL
	expandedData, err := os.ReadFile("testdata/meta_version.expanded.jsonl")
	require.NoError(t, err)

	input := string(expandedData)

	// Apply carry-forward multiple times
	output1 := processJSONLWithCarryForward(t, input)
	output2 := processJSONLWithCarryForward(t, output1)
	output3 := processJSONLWithCarryForward(t, output2)

	// All should be identical
	norm1 := normalizeJSONL(output1)
	norm2 := normalizeJSONL(output2)
	norm3 := normalizeJSONL(output3)

	assert.Equal(t, norm1, norm2, "second pass should match first")
	assert.Equal(t, norm2, norm3, "third pass should match second")
}

// TestRoundTrip_Pipeline_Simulation simulates the CLI pipeline:
// sparse.jsonl | ydb --to-jsonl | ydb --to-jsonl | ydb | ydb
func TestRoundTrip_Pipeline_Simulation(t *testing.T) {
	// This test expects null values to be preserved
	defer func() { OmitMissingColumns = true }()
	OmitMissingColumns = false

	// Read sparse JSONL
	sparseData, err := os.ReadFile("testdata/meta_version.jsonl")
	require.NoError(t, err)

	// Pass 1: sparse | ydb --to-jsonl  (sparse → expanded)
	pass1 := processJSONLWithCarryForward(t, string(sparseData))

	// Pass 2: expanded | ydb --to-jsonl  (should be idempotent)
	pass2 := processJSONLWithCarryForward(t, pass1)

	// Pass 3: expanded | ydb  (expanded → YAML)
	yaml1, err := JSONLToCPD(strings.NewReader(pass2))
	require.NoError(t, err)

	// Pass 4: yaml | ydb  (YAML → expanded)
	jsonl1, err := CPDToJSONLUnified(strings.NewReader(yaml1))
	require.NoError(t, err)

	// Verify idempotency at each stage
	assert.Equal(t, normalizeJSONL(pass1), normalizeJSONL(pass2),
		"expanded JSONL should be idempotent")

	assert.Equal(t, normalizeJSONL(pass2), normalizeJSONL(jsonl1),
		"YAML round-trip should preserve expanded JSONL")
}

// TestRoundTrip_PreservesFieldOrder verifies field order is preserved
// through conversions (important for usability)
func TestRoundTrip_PreservesFieldOrder(t *testing.T) {
	input := strings.Join([]string{
		`{"_version":1}`,
		`{"_meta":{"location":"lab1"}}`,
		`{"name":"test","temperature":25.5,"status":"ok"}`,
	}, "\n")

	expanded := processJSONLWithCarryForward(t, input)

	// Check that _version and _meta.* come before payload fields
	lines := strings.Split(strings.TrimSpace(expanded), "\n")
	require.Len(t, lines, 1)

	record := lines[0]
	versionIdx := strings.Index(record, `"_version"`)
	metaIdx := strings.Index(record, `"_meta.location"`)
	nameIdx := strings.Index(record, `"name"`)

	require.NotEqual(t, -1, versionIdx)
	require.NotEqual(t, -1, metaIdx)
	require.NotEqual(t, -1, nameIdx)

	assert.Less(t, versionIdx, nameIdx, "_version should come before payload fields")
	assert.Less(t, metaIdx, nameIdx, "_meta fields should come before payload fields")
}

// normalizeJSONL removes empty lines and trims whitespace for comparison
func normalizeJSONL(jsonl string) string {
	lines := strings.Split(strings.TrimSpace(jsonl), "\n")
	var normalized []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "//") {
			// Parse JSON and re-serialize with sorted keys for consistent comparison
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
				// Re-marshal with sorted keys
				canonical, err := json.Marshal(obj)
				if err == nil {
					normalized = append(normalized, string(canonical))
					continue
				}
			}
			// If parsing fails, use original line
			normalized = append(normalized, trimmed)
		}
	}
	return strings.Join(normalized, "\n")
}
