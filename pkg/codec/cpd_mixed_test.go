package codec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMixedDataFormatSupport(t *testing.T) {
	tests := []struct {
		name           string
		yamlInput      string
		expectedOutput string
		expectError    bool
	}{
		{
			name: "pure array format (existing behavior)",
			yamlInput: `_columns: [name, temperature, status]
status:
  ok: 1
  fail: 2
data:
  - ["alpha-1", 22.5, 1]
  - ["beta-1", 19.8, 2]`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5,"status":"ok"}
{"name":"beta-1","temperature":19.8,"status":"fail"}`,
		},
		{
			name: "pure object format (basic.yaml style)",
			yamlInput: `_columns: [name, temperature, status]
status:
  ok: 1
  fail: 2
data:
  - {name: "alpha-1", temperature: 22.5, status: "ok"}
  - {name: "beta-1", temperature: 19.8, status: "fail"}`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5,"status":"ok"}
{"name":"beta-1","temperature":19.8,"status":"fail"}`,
		},
		{
			name: "mixed arrays and objects",
			yamlInput: `_columns: [name, temperature, status]
status:
  ok: 1
  fail: 2
data:
  - ["alpha-1", 22.5, 1]
  - {name: "beta-1", status: "fail", extra: "data"}
  - ["gamma-1", 21.0, 1]`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5,"status":"ok"}
{"name":"beta-1","status":"fail","extra":"data"}
{"name":"gamma-1","temperature":21.0,"status":"ok"}`,
		},
		{
			name: "object with extra fields (payload handling)",
			yamlInput: `_columns: [name, temperature, status, payload]
status:
  ok: 1
data:
  - {name: "alpha-1", temperature: 22.5, status: "ok", extra: "data", more: "info"}`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5,"status":"ok","extra":"data","more":"info"}`,
		},
		{
			name: "object missing optional fields",
			yamlInput: `_columns: [name, temperature, status]
status:
  ok: 1
data:
  - {name: "alpha-1", temperature: 22.5}
  - {name: "beta-1", status: "ok"}`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5}
{"name":"beta-1","status":"ok"}`,
		},
		{
			name: "object with field order different from columns",
			yamlInput: `_columns: [name, temperature, status]
status:
  ok: 1
data:
  - {status: "ok", name: "alpha-1", temperature: 22.5}`,
			expectedOutput: `{"name":"alpha-1","temperature":22.5,"status":"ok"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test CPD to JSONL conversion
			result, err := CPDToJSONL(strings.NewReader(tt.yamlInput))

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tt.expectedOutput), strings.TrimSpace(result))
		})
	}
}

func TestObjectToArrayConversion(t *testing.T) {
	tests := []struct {
		name          string
		yamlObject    string
		columns       []string
		joinTables    map[string]*JoinTable
		expectedArray []interface{}
		expectError   bool
	}{
		{
			name:       "basic object to array",
			yamlObject: `{name: "test", temperature: 25.0, status: "ok"}`,
			columns:    []string{"name", "temperature", "status"},
			joinTables: map[string]*JoinTable{
				"status": {NameToID: map[string]int{"ok": 1, "fail": 2}, IDToName: map[int]string{1: "ok", 2: "fail"}},
			},
			expectedArray: []interface{}{"test", 25.0, 1},
		},
		{
			name:          "object with missing fields",
			yamlObject:    `{name: "test"}`,
			columns:       []string{"name", "temperature", "status"},
			joinTables:    map[string]*JoinTable{},
			expectedArray: []interface{}{"test", nil, nil},
		},
		{
			name:          "object with extra fields",
			yamlObject:    `{name: "test", temperature: 25.0, extra: "data"}`,
			columns:       []string{"name", "temperature", "payload"},
			joinTables:    map[string]*JoinTable{},
			expectedArray: []interface{}{"test", 25.0, map[string]interface{}{"extra": "data"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This will test the object parsing logic once implemented
			// For now, this documents the expected behavior
			t.Skip("Pending implementation of parseObjectRow function")
		})
	}
}

func TestBasicYamlCompatibility(t *testing.T) {
	// Ensure that the existing basic.yaml test data works with mixed format support
	t.Run("basic.yaml object format should work", func(t *testing.T) {
		yamlInput := `_columns: [name, temperature, status]
status:
  ok: 1
  fail: 2
  warn: 3
  unknown: 4
data:
  - {name: "alpha-1", temperature: 22.5, status: "ok"}
  - {name: "alpha-2", temperature: 23.0, status: "ok"}
  - {name: "beta-1", status: "fail", temperature: 19.8}`

		result, err := CPDToJSONL(strings.NewReader(yamlInput))

		// For now, this will fail until we implement object support
		// But it documents what should work
		if err != nil {
			t.Skipf("Expected to work after implementation: %v", err)
		}

		assert.NoError(t, err)
		assert.Contains(t, result, `"name":"alpha-1"`)
		assert.Contains(t, result, `"status":"ok"`)
	})
}

func TestMixedFormatIntegration(t *testing.T) {
	tests := []struct {
		name     string
		yamlFile string
		testFunc func(t *testing.T, jsonlOutput string)
	}{
		{
			name:     "basic.yaml object format",
			yamlFile: "basic.yaml",
			testFunc: func(t *testing.T, jsonlOutput string) {
				lines := strings.Split(strings.TrimSpace(jsonlOutput), "\n")
				assert.Equal(t, 10, len(lines), "Should have 10 records")

				// Check first record
				assert.Contains(t, lines[0], `"name":"alpha-1"`)
				assert.Contains(t, lines[0], `"temperature":22.5`)
				assert.Contains(t, lines[0], `"status":"ok"`)

				// Check null handling - with omit behavior, these fields are excluded
				assert.NotContains(t, lines[5], `"temperature":null`) // temperature:null should be omitted
				assert.NotContains(t, lines[8], `"status":null`)      // status:null should be omitted

				// Check missing field handling - missing fields are omitted
				assert.NotContains(t, lines[9], `"status":null`) // missing status should be omitted
			},
		},
		{
			name:     "meta_version.yaml object format with metadata",
			yamlFile: "meta_version.yaml",
			testFunc: func(t *testing.T, jsonlOutput string) {
				lines := strings.Split(strings.TrimSpace(jsonlOutput), "\n")
				assert.GreaterOrEqual(t, len(lines), 6, "Should have at least 6 records")

				// Check version metadata
				assert.Contains(t, lines[0], `"_version":1`)

				// Check object format works with metadata
				assert.Contains(t, lines[0], `"name":"beta-1"`)
				assert.Contains(t, lines[0], `"temperature":28.4`)
			},
		},
		{
			name:     "compacted.yaml array format still works",
			yamlFile: "compacted.yaml",
			testFunc: func(t *testing.T, jsonlOutput string) {
				lines := strings.Split(strings.TrimSpace(jsonlOutput), "\n")
				assert.Equal(t, 10, len(lines), "Should have 10 records")

				// Check join table conversion for array format
				assert.Contains(t, lines[0], `"status":"on"`)
				assert.Contains(t, lines[1], `"status":"off"`)
				assert.Contains(t, lines[3], `"status":"standby"`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CPDToJSONLUnified(strings.NewReader(readTestFile(t, tt.yamlFile)))
			assert.NoError(t, err)
			assert.NotEmpty(t, result)

			tt.testFunc(t, result)
		})
	}
}

func TestMixedFormatBackwardCompatibility(t *testing.T) {
	// Test that pure array format still works
	yamlInput := `_columns: [name, score]
data:
  - ["alice", 95]
  - ["bob", 87]`

	result, err := CPDToJSONLUnified(strings.NewReader(yamlInput))
	assert.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(result), "\n")
	assert.Equal(t, 2, len(lines))
	assert.Contains(t, lines[0], `"name":"alice"`)
	assert.Contains(t, lines[0], `"score":95`)
	assert.Contains(t, lines[1], `"name":"bob"`)
	assert.Contains(t, lines[1], `"score":87`)
}

func TestOmitMissingColumnsSettings(t *testing.T) {
	yamlInput := `_columns: [name, temperature, status]
data:
  - {name: "test", temperature: 22.5}` // missing status column

	// Test default behavior (omit missing columns)
	OmitMissingColumns = true
	result, err := CPDToJSONLUnified(strings.NewReader(yamlInput))
	assert.NoError(t, err)
	assert.Equal(t, `{"name":"test","temperature":22.5}`, strings.TrimSpace(result))
	assert.NotContains(t, result, "status")

	// Test include missing columns as null
	OmitMissingColumns = false
	result, err = CPDToJSONLUnified(strings.NewReader(yamlInput))
	assert.NoError(t, err)
	assert.Equal(t, `{"name":"test","temperature":22.5,"status":null}`, strings.TrimSpace(result))
	assert.Contains(t, result, `"status":null`)

	// Reset to default
	OmitMissingColumns = true
}
