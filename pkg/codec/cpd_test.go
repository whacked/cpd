package codec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCPDToJSONL(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name: "basic conversion",
			input: CommonPayloadDataSchema + `
_columns:
  - time
  - tags
  - payload
tags:
  temperature: 1
  humidity: 2
  light: 3
data:
  - ["2024-06-11 00:20:43.123+00:00", [1, 2], {temp_c: 23.4, humidity: 45.2}]
  - ["2024-06-12T12:00:00Z", [], {lux: 320, motion_detected: false}]`,
			want: `{"time":"2024-06-11 00:20:43.123+00:00","tags":["temperature","humidity"],"temp_c":23.4,"humidity":45.2}
{"time":"2024-06-12T12:00:00Z","tags":[],"lux":320,"motion_detected":false}
`,
			wantErr: false,
		},
		{
			name: "missing data section",
			input: `_schemas:
  data:
    type: array
tags:
  temperature: 1`,
			wantErr: false,
		},
		{
			name: "invalid row format",
			input: `_schemas:
  data:
    type: array
data:
  - ["2024-06-11 00:20:43.123+00:00", [1, 2]]`,
			wantErr: true,
		},
		{
			name: "unknown tag ID",
			input: `_schemas:
  data:
    type: array
tags:
  temperature: 1
data:
  - ["2024-06-11 00:20:43.123+00:00", [1, 2, 999], {temp_c: 23.4}]`,
			wantErr: true,
		},
		{
			name: "multi-document with metadata propagation",
			input: `---
_version: 1
_meta:
  location: lab1
  device:
    id: sensor23
tags:
  temperature: 1
  humidity: 2
data:
  - ["2024-06-11 00:20:43.123+00:00", [1], {temp_c: 23.4}]
  - ["2024-06-12T12:00:00Z", [2], {humidity: 45.2}]
---
_meta:
  location: lab2
data:
  - ["2024-06-13T12:00:00Z", [1, 2], {temp_c: 24.5, humidity: 46.0}]`,
			want: `{"_version":1,"_meta.location":"lab1","_meta.device.id":"sensor23","time":"2024-06-11 00:20:43.123+00:00","tags":["temperature"],"temp_c":23.4}
{"_version":1,"_meta.location":"lab1","_meta.device.id":"sensor23","time":"2024-06-12T12:00:00Z","tags":["humidity"],"humidity":45.2}
{"_version":1,"_meta.location":"lab2","_meta.device.id":"sensor23","time":"2024-06-13T12:00:00Z","tags":["temperature","humidity"],"temp_c":24.5,"humidity":46.0}
`,
			wantErr: false,
		},
		{
			name: "multi-document with version change",
			input: `---
_version: 1
_meta:
  location: lab1
tags:
  temperature: 1
data:
  - ["2024-06-11 00:20:43.123+00:00", [1], {temp_c: 23.4}]
---
_version: 2
_meta:
  location: lab2
data:
  - ["2024-06-12T12:00:00Z", [1], {temp_c: 24.5}]`,
			want: `{"_version":1,"_meta.location":"lab1","time":"2024-06-11 00:20:43.123+00:00","tags":["temperature"],"temp_c":23.4}
{"_version":2,"_meta.location":"lab2","time":"2024-06-12T12:00:00Z","tags":["temperature"],"temp_c":24.5}
`,
			wantErr: false,
		},
		{
			name: "multi-document with nested metadata",
			input: `---
_meta:
  location: lab1
  device:
    id: sensor23
    type: temperature
tags:
  temperature: 1
data:
  - ["2024-06-11 00:20:43.123+00:00", [1], {temp_c: 23.4}]
---
_meta:
  device:
    type: humidity
data:
  - ["2024-06-12T12:00:00Z", [1], {temp_c: 24.5}]`,
			want: `{"_meta.location":"lab1","_meta.device.id":"sensor23","_meta.device.type":"temperature","time":"2024-06-11 00:20:43.123+00:00","tags":["temperature"],"temp_c":23.4}
{"_meta.location":"lab1","_meta.device.id":"sensor23","_meta.device.type":"humidity","time":"2024-06-12T12:00:00Z","tags":["temperature"],"temp_c":24.5}
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CPDToJSONL(strings.NewReader(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("CPDToJSONL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestJSONLToCPD(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name: "basic conversion",
			input: `{"time":"2024-06-11 00:20:43.123+00:00","tags":["temperature","humidity"],"temp_c":23.4,"humidity":45.2}
{"time":"2024-06-12T12:00:00Z","tags":[],"lux":320,"motion_detected":false}`,
			want: `_schemas:
  data:
    type: array
    items:
      type: array
      minItems: 3
      maxItems: 3
      items:
        - type: string
          description: ISO8601 / RFC3339 string
        - type: array
          items:
            type: integer
          uniqueItems: true
        - type: object
_columns:
  - time
  - tags
  - payload
tags:
  humidity: 1
  temperature: 2
data:
  - ["2024-06-11 00:20:43.123+00:00", [2, 1], {temp_c: 23.4, humidity: 45.2}]
  - ["2024-06-12T12:00:00Z", [], {lux: 320, motion_detected: false}]
`,
			wantErr: false,
		},
		{
			name:    "missing time field",
			input:   `{"tags":["temperature"],"temp_c":23.4}`,
			wantErr: true,
		},
		{
			name:    "invalid time field",
			input:   `{"time":123,"tags":["temperature"],"temp_c":23.4}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONLToCPD(strings.NewReader(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONLToCPD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Compare YAML documents ignoring whitespace and order
				assert.YAMLEq(t, tt.want, got)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	// Test round-trip conversion
	input := `{"time":"2024-06-11 00:20:43.123+00:00","tags":["temperature","humidity"],"temp_c":23.4,"humidity":45.2}
{"time":"2024-06-12T12:00:00Z","tags":[],"lux":320,"motion_detected":false}`

	// Convert to CPD
	cpd, err := JSONLToCPD(strings.NewReader(input))
	assert.NoError(t, err)

	// Convert back to JSONL
	jsonl, err := CPDToJSONL(strings.NewReader(cpd))
	assert.NoError(t, err)

	// Compare with original
	assert.Equal(t, input, jsonl)
}

func TestFileBasedConversion(t *testing.T) {
	// Read test files
	yamlPath := filepath.Join("testdata", "commonpayloaddata.cpd.yaml")
	jsonlPath := filepath.Join("testdata", "commonpayloaddata.jsonl")

	yamlData, err := os.ReadFile(yamlPath)
	assert.NoError(t, err, "Failed to read YAML test file")

	jsonlData, err := os.ReadFile(jsonlPath)
	assert.NoError(t, err, "Failed to read JSONL test file")

	// Test YAML -> JSONL conversion
	jsonlResult, err := CPDToJSONL(strings.NewReader(string(yamlData)))
	assert.NoError(t, err, "Failed to convert YAML to JSONL")

	// Normalize both JSONL strings for comparison
	normalizedExpected := strings.TrimSpace(string(jsonlData))
	normalizedResult := strings.TrimSpace(jsonlResult)
	assert.Equal(t, normalizedExpected, normalizedResult, "YAML to JSONL conversion mismatch")

	// Test JSONL -> YAML conversion
	yamlResult, err := JSONLToCPD(strings.NewReader(string(jsonlData)))
	assert.NoError(t, err, "Failed to convert JSONL to YAML")

	// Compare YAML documents ignoring whitespace and order
	assert.YAMLEq(t, string(yamlData), yamlResult, "JSONL to YAML conversion mismatch")

	// Test round-trip conversion
	// YAML -> JSONL -> YAML
	jsonlRoundTrip, err := CPDToJSONL(strings.NewReader(yamlResult))
	assert.NoError(t, err, "Failed in round-trip conversion (YAML->JSONL->YAML)")
	assert.Equal(t, normalizedExpected, strings.TrimSpace(jsonlRoundTrip), "Round-trip conversion mismatch")

	// JSONL -> YAML -> JSONL
	yamlRoundTrip, err := JSONLToCPD(strings.NewReader(jsonlRoundTrip))
	assert.NoError(t, err, "Failed in round-trip conversion (JSONL->YAML->JSONL)")
	assert.YAMLEq(t, yamlResult, yamlRoundTrip, "Round-trip conversion mismatch")
}
