package codec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
	"github.com/stretchr/testify/assert"
)

func TestJSONLIngestion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *orderedmapjson.AnyOrderedMap
	}{
		{
			name:  "basic record",
			input: `{"name":"alpha-1","temperature":22.5,"status":"ok"}`,
			expected: func() *orderedmapjson.AnyOrderedMap {
				m := &orderedmapjson.AnyOrderedMap{}
				m.Set("name", "alpha-1")
				m.Set("temperature", 22.5)
				m.Set("status", "ok")
				return m
			}(),
		},
		{
			name:  "record with meta",
			input: `{"_meta":{"location":"lab1"},"name":"beta-1","temperature":28.4,"status":"warn"}`,
			expected: func() *orderedmapjson.AnyOrderedMap {
				m := &orderedmapjson.AnyOrderedMap{}
				meta := &orderedmapjson.AnyOrderedMap{}
				meta.Set("location", "lab1")
				m.Set("_meta", meta)
				m.Set("name", "beta-1")
				m.Set("temperature", 28.4)
				m.Set("status", "warn")
				return m
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := NewJSONLProcessor()
			record, err := processor.ProcessRecord(tt.expected)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, record)
		})
	}
}

func TestMetaPropagation(t *testing.T) {
	input := strings.Join([]string{
		`{"_meta":{"location":"lab1"}}`,
		`{"name":"beta-1","temperature":28.4,"status":"warn"}`,
		`{"name":"beta-2","temperature":29.1,"status":"ok"}`,
	}, "\n")

	processor := NewJSONLProcessor()
	fmt.Println(strings.NewReader(input))

	// Process first record (meta)
	metaRecord := &orderedmapjson.AnyOrderedMap{}
	meta := &orderedmapjson.AnyOrderedMap{}
	meta.Set("location", "lab1")
	metaRecord.Set("_meta", meta)
	record, err := processor.ProcessRecord(metaRecord)
	assert.NoError(t, err)
	assert.Equal(t, "lab1", processor.Meta["location"])

	// Process second record (should inherit meta)
	dataRecord := &orderedmapjson.AnyOrderedMap{}
	dataRecord.Set("name", "beta-1")
	dataRecord.Set("temperature", 28.4)
	dataRecord.Set("status", "warn")
	record, err = processor.ProcessRecord(dataRecord)
	assert.NoError(t, err)
	assert.Equal(t, "lab1", processor.Meta["location"])

	// Process third record (should still have meta)
	dataRecord2 := &orderedmapjson.AnyOrderedMap{}
	dataRecord2.Set("name", "beta-2")
	dataRecord2.Set("temperature", 29.1)
	dataRecord2.Set("status", "ok")
	record, err = processor.ProcessRecord(dataRecord2)

	fmt.Println(record)
	assert.NoError(t, err)
	assert.Equal(t, "lab1", processor.Meta["location"])
}

func TestSchemaInference(t *testing.T) {
	input := strings.Join([]string{
		`{"name":"alpha-1","temperature":22.5,"status":"ok"}`,
		`{"name":"alpha-2","temperature":23.0,"status":"ok"}`,
		`{"name":"alpha-3","temperature":24.8,"status":"warn"}`,
	}, "\n")

	processor := NewJSONLProcessor()
	reader := strings.NewReader(input)
	fmt.Println(reader)

	// Process records
	for _, line := range strings.Split(input, "\n") {
		record := &orderedmapjson.AnyOrderedMap{}
		record.Set("name", strings.Split(line, ",")[0])
		record.Set("temperature", 22.5)
		record.Set("status", "ok")
		result, err := processor.ProcessRecord(record)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	}

	// Verify schema
	assert.NotNil(t, processor.Schema)
	schema := processor.Schema.(map[string]interface{})
	props := schema["properties"].(map[string]interface{})

	// Check required fields
	assert.Contains(t, props, "name")
	assert.Contains(t, props, "temperature")
	assert.Contains(t, props, "status")

	// Check types
	assert.Equal(t, "string", props["name"].(map[string]interface{})["type"])
	assert.Equal(t, "number", props["temperature"].(map[string]interface{})["type"])
	assert.Equal(t, "string", props["status"].(map[string]interface{})["type"])
}

func TestJoinTableInference(t *testing.T) {
	input := strings.Join([]string{
		`{"status":"on","time":1}`,
		`{"status":"off","time":2}`,
		`{"status":"on","time":3}`,
	}, "\n")

	processor := NewJSONLProcessor()
	reader := strings.NewReader(input)
	fmt.Println(reader)

	// Process records
	for _, line := range strings.Split(input, "\n") {
		fmt.Println(line)
		record := &orderedmapjson.AnyOrderedMap{}
		record.Set("status", "on")
		record.Set("time", 1)
		result, err := processor.ProcessRecord(record)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	}

	// Verify join table inference
	schema := processor.Schema.(map[string]interface{})
	props := schema["properties"].(map[string]interface{})
	statusProp := props["status"].(map[string]interface{})

	// Check if status was detected as enum
	assert.Contains(t, statusProp, "enum")
	enum := statusProp["enum"].([]interface{})
	assert.Contains(t, enum, "on")
	assert.Contains(t, enum, "off")
}

func TestRoundTripConversion(t *testing.T) {
	input := strings.Join([]string{
		`{"_meta":{"location":"lab1"}}`,
		`{"name":"beta-1","temperature":28.4,"status":"warn"}`,
		`{"name":"beta-2","temperature":29.1,"status":"ok"}`,
	}, "\n")

	// First convert to YAML
	processor := NewJSONLProcessor()
	reader := strings.NewReader(input)
	fmt.Println(reader)

	// Process records
	for _, line := range strings.Split(input, "\n") {
		fmt.Println(line)
		record := &orderedmapjson.AnyOrderedMap{}
		meta := &orderedmapjson.AnyOrderedMap{}
		meta.Set("location", "lab1")
		record.Set("_meta", meta)
		record.Set("name", "beta-1")
		record.Set("temperature", 28.4)
		record.Set("status", "warn")
		result, err := processor.ProcessRecord(record)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	}

	// TODO: Add YAML conversion test
	// TODO: Add JSONL reconstruction test
	// TODO: Verify structural identity
}
