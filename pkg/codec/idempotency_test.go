package codec

import (
	"os"
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/whacked/yamdb/pkg/io/yamlutil"
)

// TestCarryForwardIdempotency verifies that applying carry-forward to expanded JSONL
// produces identical output (idempotent operation)
func TestCarryForwardIdempotency(t *testing.T) {
	// Read the expanded JSONL test file
	expandedData, err := os.ReadFile("testdata/meta_version.expanded.jsonl")
	require.NoError(t, err, "should read expanded test file")

	// Process it through carry-forward
	output1 := processJSONLWithCarryForward(t, string(expandedData))

	// Process the output again
	output2 := processJSONLWithCarryForward(t, output1)

	// Should be identical
	assert.Equal(t, output1, output2, "carry-forward should be idempotent on expanded JSONL")
}

// TestSparseToExpandedMatches verifies that sparse JSONL produces same output
// as running carry-forward on the already-expanded version
func TestSparseToExpandedMatches(t *testing.T) {
	// Read sparse JSONL
	sparseData, err := os.ReadFile("testdata/meta_version.jsonl")
	require.NoError(t, err)

	// Read expected expanded output
	expectedData, err := os.ReadFile("testdata/meta_version.expanded.jsonl")
	require.NoError(t, err)

	// Process sparse through carry-forward
	actualOutput := processJSONLWithCarryForward(t, string(sparseData))

	// Normalize whitespace for comparison
	expected := strings.TrimSpace(string(expectedData))
	actual := strings.TrimSpace(actualOutput)

	assert.Equal(t, expected, actual, "sparse→expanded should match expected output")
}

// TestExpandedPassThrough verifies that expanded JSONL passes through unchanged
func TestExpandedPassThrough(t *testing.T) {
	expandedData, err := os.ReadFile("testdata/meta_version.expanded.jsonl")
	require.NoError(t, err)

	output := processJSONLWithCarryForward(t, string(expandedData))

	expected := strings.TrimSpace(string(expandedData))
	actual := strings.TrimSpace(output)

	assert.Equal(t, expected, actual, "expanded JSONL should pass through unchanged")
}

// TestCarryForwardPreservesPayloadFields verifies that payload fields
// are not lost during carry-forward processing
func TestCarryForwardPreservesPayloadFields(t *testing.T) {
	input := strings.Join([]string{
		`{"_version":1}`,
		`{"_meta":{"location":"lab1"}}`,
		`{"name":"test","temperature":25.5,"status":"ok","custom_field":"value"}`,
	}, "\n")

	output := processJSONLWithCarryForward(t, input)

	// Output should have all fields
	assert.Contains(t, output, `"name":"test"`)
	assert.Contains(t, output, `"temperature":25.5`)
	assert.Contains(t, output, `"status":"ok"`)
	assert.Contains(t, output, `"custom_field":"value"`)
	assert.Contains(t, output, `"_version":1`)
	assert.Contains(t, output, `"_meta.location":"lab1"`)
}

// TestCarryForwardHandlesNullMeta verifies null metadata is handled correctly
func TestCarryForwardHandlesNullMeta(t *testing.T) {
	input := strings.Join([]string{
		`{"_meta":{"location":"lab1","flavor":"vanilla"}}`,
		`{"name":"record1","value":1}`,
		`{"_meta":{"location":null}}`,
		`{"name":"record2","value":2}`,
	}, "\n")

	output := processJSONLWithCarryForward(t, input)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	require.Len(t, lines, 2, "should have 2 data records")

	// First record should have location
	assert.Contains(t, lines[0], `"_meta.location":"lab1"`)
	assert.Contains(t, lines[0], `"_meta.flavor":"vanilla"`)

	// Second record should have null location and vanilla flavor
	assert.Contains(t, lines[1], `"_meta.location":null`)
	assert.Contains(t, lines[1], `"_meta.flavor":"vanilla"`)
}

// TestCarryForwardHandlesVersionChange verifies version changes are tracked
func TestCarryForwardHandlesVersionChange(t *testing.T) {
	input := strings.Join([]string{
		`{"_version":1}`,
		`{"name":"record1"}`,
		`{"_version":2}`,
		`{"name":"record2"}`,
		`{"name":"record3"}`,
	}, "\n")

	output := processJSONLWithCarryForward(t, input)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	require.Len(t, lines, 3, "should have 3 data records")

	assert.Contains(t, lines[0], `"_version":1`)
	assert.Contains(t, lines[1], `"_version":2`)
	assert.Contains(t, lines[2], `"_version":2`, "version should carry forward")
}

// TestCarryForwardHandlesEmptyMeta verifies empty metadata objects
func TestCarryForwardHandlesEmptyMeta(t *testing.T) {
	input := strings.Join([]string{
		`{"_meta":{"location":"lab1","device":"sensor1"}}`,
		`{"name":"record1"}`,
		`{"_meta":{}}`,
		`{"name":"record2"}`,
	}, "\n")

	output := processJSONLWithCarryForward(t, input)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	require.Len(t, lines, 2, "should have 2 data records")

	// First record has metadata
	assert.Contains(t, lines[0], `"_meta.location":"lab1"`)
	assert.Contains(t, lines[0], `"_meta.device":"sensor1"`)

	// Second record should still have metadata (empty _meta doesn't clear it)
	assert.Contains(t, lines[1], `"_meta.location":"lab1"`)
	assert.Contains(t, lines[1], `"_meta.device":"sensor1"`)
}

// TestCarryForwardWithNestedMetadata verifies nested metadata flattening
func TestCarryForwardWithNestedMetadata(t *testing.T) {
	input := strings.Join([]string{
		`{"_meta":{"device":{"id":"sensor23","type":"temp"}}}`,
		`{"name":"reading","value":25.5}`,
	}, "\n")

	output := processJSONLWithCarryForward(t, input)

	assert.Contains(t, output, `"_meta.device.id":"sensor23"`)
	assert.Contains(t, output, `"_meta.device.type":"temp"`)
	assert.Contains(t, output, `"name":"reading"`)
	assert.Contains(t, output, `"value":25.5`)
}

// Helper function to process JSONL through carry-forward processor
func processJSONLWithCarryForward(t *testing.T, input string) string {
	t.Helper()

	processor := NewJSONLProcessor()
	scanner := strings.Split(strings.TrimSpace(input), "\n")

	for _, line := range scanner {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		// Parse the line as YAML to preserve order
		var node yaml.Node
		err := yaml.Unmarshal([]byte(line), &node)
		require.NoError(t, err, "should parse line: %s", line)

		record := orderedmapjson.NewAnyOrderedMap()
		err = yamlutil.ConvertNodeToOrderedMap(&node, record)
		require.NoError(t, err, "should convert to ordered map")

		_, err = processor.ProcessRecord(record)
		require.NoError(t, err, "should process record")
	}

	return processor.ToExpandedJSONL(true)
}
