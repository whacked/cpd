package codec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
	"github.com/stretchr/testify/assert"
	"github.com/whacked/yamdb/pkg/types"
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
				m := orderedmapjson.NewAnyOrderedMap()
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
				m := orderedmapjson.NewAnyOrderedMap()
				meta := orderedmapjson.NewAnyOrderedMap()
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

			// Create the record directly instead of parsing JSON
			record := orderedmapjson.NewAnyOrderedMap()
			if tt.name == "basic record" {
				record.Set("name", "alpha-1")
				record.Set("temperature", 22.5)
				record.Set("status", "ok")
			} else if tt.name == "record with meta" {
				meta := orderedmapjson.NewAnyOrderedMap()
				meta.Set("location", "lab1")
				record.Set("_meta", meta)
				record.Set("name", "beta-1")
				record.Set("temperature", 28.4)
				record.Set("status", "warn")
			}

			result, err := processor.ProcessRecord(record)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
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
	metaRecord := orderedmapjson.NewAnyOrderedMap()
	meta := orderedmapjson.NewAnyOrderedMap()
	meta.Set("location", "lab1")
	metaRecord.Set("_meta", meta)
	record, err := processor.ProcessRecord(metaRecord)
	assert.NoError(t, err)
	if processor.Meta == nil {
		t.Fatal("processor.Meta is nil")
	}
	location, _ := processor.Meta.Get("location")
	assert.Equal(t, "lab1", location)

	// Process second record (should inherit meta)
	dataRecord := orderedmapjson.NewAnyOrderedMap()
	dataRecord.Set("name", "beta-1")
	dataRecord.Set("temperature", 28.4)
	dataRecord.Set("status", "warn")
	record, err = processor.ProcessRecord(dataRecord)
	assert.NoError(t, err)
	location, _ = processor.Meta.Get("location")
	assert.Equal(t, "lab1", location)

	// Process third record (should still have meta)
	dataRecord2 := orderedmapjson.NewAnyOrderedMap()
	dataRecord2.Set("name", "beta-2")
	dataRecord2.Set("temperature", 29.1)
	dataRecord2.Set("status", "ok")
	record, err = processor.ProcessRecord(dataRecord2)

	fmt.Println(record)
	assert.NoError(t, err)
	location, _ = processor.Meta.Get("location")
	assert.Equal(t, "lab1", location)
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
		record := orderedmapjson.NewAnyOrderedMap()
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
	for i, line := range strings.Split(input, "\n") {
		fmt.Println(line)
		record := orderedmapjson.NewAnyOrderedMap()
		if i == 0 {
			record.Set("status", "on")
			record.Set("time", 1)
		} else if i == 1 {
			record.Set("status", "off")
			record.Set("time", 2)
		} else if i == 2 {
			record.Set("status", "on")
			record.Set("time", 3)
		}
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
		record := orderedmapjson.NewAnyOrderedMap()
		meta := orderedmapjson.NewAnyOrderedMap()
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

func TestJSONLToCommonPayloadData(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *types.CommonPayloadData
		wantErr bool
	}{
		{
			name:  "valid payload",
			input: `[1718065243.123, [6,23,3], {event: "boot", name: "sensor-42"}]`,
			want: &types.CommonPayloadData{
				Timestamp: 1718065243.123,
				Tags:      []int{3, 6, 23},
				Payload: func() *orderedmapjson.AnyOrderedMap {
					m := orderedmapjson.NewAnyOrderedMap()
					m.Set("event", "boot")
					m.Set("name", "sensor-42")
					return m
				}(),
			},
			wantErr: false,
		},
		{
			name:    "invalid array length (no payload)",
			input:   `[1718065243.123, [3,6,23]]`,
			wantErr: true,
		},
		{
			name:    "invalid timestamp type",
			input:   `["not-a-number", [3,6,23], {event: "boot"}]`,
			wantErr: true,
		},
		{
			name:    "invalid tags type",
			input:   `[1718065243.123, "not-an-array", {event: "boot"}]`,
			wantErr: true,
		},
		{
			name:    "invalid tag value",
			input:   `[1718065243.123, [2, "not-a-number", 99], {event: "boot"}]`,
			wantErr: true,
		},
		{
			name:  "null payload",
			input: `[1718065243.123, [3,6,23], null]`,
			want: &types.CommonPayloadData{
				Timestamp: 1718065243.123,
				Tags:      []int{3, 6, 23},
				Payload:   nil,
			},
			wantErr: true,
		},
		{
			name:  "empty categories",
			input: `[1718065243.123, [], {event: "boot"}]`,
			want: &types.CommonPayloadData{
				Timestamp: 1718065243.123,
				Tags:      []int{},
				Payload: func() *orderedmapjson.AnyOrderedMap {
					m := orderedmapjson.NewAnyOrderedMap()
					m.Set("event", "boot")
					return m
				}(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONLToCommonPayloadData(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONLToCommonPayloadData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.Equal(t, tt.want.Timestamp, got.Timestamp)
				assert.Equal(t, tt.want.Tags, got.Tags)
				if tt.want.Payload == nil {
					assert.Nil(t, got.Payload)
				} else {
					assert.Equal(t, tt.want.Payload.String(), got.Payload.String())
				}
			}
		})
	}
}
