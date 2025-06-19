package codec

import (
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
	"github.com/stretchr/testify/assert"
)

func TestParse_BasicJoinExpansion(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *CPDDocument
		wantErr bool
	}{
		{
			name: "basic document with scalar and array joins",
			input: `_columns: [time, authors, topic, payload]
authors:
  alice: 1
  bob: 2
topic:
  food: 1
data:
  - ["2024-06-12T12:00:00Z", [1, 2], 1, {note: "ate natto"}]
  - ["2024-06-13T13:30:00Z", 1, null, {note: "light snack"}]`,
			want: &CPDDocument{
				Columns: []string{"time", "authors", "topic", "payload"},
				JoinTables: map[string]*JoinTable{
					"authors": {
						NameToID: map[string]int{"alice": 1, "bob": 2},
						IDToName: map[int]string{1: "alice", 2: "bob"},
					},
					"topic": {
						NameToID: map[string]int{"food": 1},
						IDToName: map[int]string{1: "food"},
					},
				},
				Data: []*CPDRow{
					{
						Values: func() *orderedmapjson.AnyOrderedMap {
							m := orderedmapjson.NewAnyOrderedMap()
							m.Set("time", "2024-06-12T12:00:00Z")
							m.Set("authors", []string{"alice", "bob"})
							m.Set("topic", "food")
							m.Set("note", "ate natto")
							return m
						}(),
					},
					{
						Values: func() *orderedmapjson.AnyOrderedMap {
							m := orderedmapjson.NewAnyOrderedMap()
							m.Set("time", "2024-06-13T13:30:00Z")
							m.Set("authors", "alice")
							m.Set("note", "light snack")
							return m
						}(),
					},
				},
				Meta:    orderedmapjson.NewAnyOrderedMap(),
				Version: "",
			},
			wantErr: false,
		},
		{
			name: "row with trailing omitted column",
			input: `_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-06-12T12:00:00Z", 1]`,
			want: &CPDDocument{
				Columns: []string{"time", "authors", "topic", "payload"},
				JoinTables: map[string]*JoinTable{
					"authors": {
						NameToID: map[string]int{"alice": 1},
						IDToName: map[int]string{1: "alice"},
					},
					"topic": {
						NameToID: map[string]int{"food": 1},
						IDToName: map[int]string{1: "food"},
					},
				},
				Data: []*CPDRow{
					{
						Values: func() *orderedmapjson.AnyOrderedMap {
							m := orderedmapjson.NewAnyOrderedMap()
							m.Set("time", "2024-06-12T12:00:00Z")
							m.Set("authors", "alice")
							return m
						}(),
					},
				},
				Meta:    orderedmapjson.NewAnyOrderedMap(),
				Version: "",
			},
			wantErr: false,
		},
		{
			name: "row with too many fields",
			input: `_columns: [time, authors]
authors:
  alice: 1
data:
  - ["2024-06-12T12:00:00Z", 1, 999]`,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCPD(strings.NewReader(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCPD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidation_JoinTableConflict(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "duplicate join IDs",
			input: `
_columns: [time, tags, payload]
tags:
  a: 1
  b: 1
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "duplicate ID in join table",
		},
		{
			name: "conflicting ID reuse across documents",
			input: `
---
tags:
  a: 1
_columns: [time, tags, payload]
data:
  - ["2024-01-01", 1, {x: 1}]
---
tags:
  b: 1
data:
  - ["2024-01-02", 1, {x: 2}]
`,
			wantErr: true,
			errMsg:  "duplicate ID in join table",
		},
		{
			name: "conflicting key reuse with different ID",
			input: `
---
tags:
  a: 1
_columns: [time, tags, payload]
data:
  - ["2024-01-01", 1, {x: 1}]
---
tags:
  a: 2
data:
  - ["2024-01-02", 2, {x: 2}]
`,
			wantErr: true,
			errMsg:  "duplicate key in join table",
		},
		{
			name: "valid join table extension across documents",
			input: `
---
tags:
  a: 1
_columns: [time, tags, payload]
data:
  - ["2024-01-01", 1, {x: 1}]
---
tags:
  b: 2
data:
  - ["2024-01-02", 2, {x: 2}]
`,
			wantErr: false,
		},
		{
			name: "multiple join tables with conflicts",
			input: `
---
authors:
  alice: 1
  bob: 2
topics:
  food: 1
  drink: 2
_columns: [time, authors, topics, payload]
data:
  - ["2024-01-01", 1, 1, {x: 1}]
---
authors:
  charlie: 1
topics:
  food: 3
data:
  - ["2024-01-02", 1, 3, {x: 2}]
`,
			wantErr: true,
			errMsg:  "duplicate ID in join table",
		},
		{
			name: "join table with non-integer ID",
			input: `
_columns: [time, tags, payload]
tags:
  a: "1"
  b: 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "invalid join table ID",
		},
		{
			name: "join table with negative ID",
			input: `
_columns: [time, tags, payload]
tags:
  a: -1
  b: 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "invalid join table ID",
		},
		{
			name: "join table with zero ID",
			input: `
_columns: [time, tags, payload]
tags:
  a: 0
  b: 2
data:
  - ["2024-01-01", 0, {x: 1}]
`,
			wantErr: false,
		},
		{
			name: "join table with empty key",
			input: `
_columns: [time, tags, payload]
tags:
  "": 1
  b: 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "empty join table key",
		},
		{
			name: "join table with whitespace key",
			input: `
_columns: [time, tags, payload]
tags:
  " ": 1
  b: 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "empty join table key",
		},
		{
			name: "join table with special characters in key",
			input: `
_columns: [time, tags, payload]
tags:
  "a/b": 1
  "c.d": 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: false,
		},
		{
			name: "join table with unicode characters",
			input: `
_columns: [time, tags, payload]
tags:
  "café": 1
  "résumé": 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: false,
		},
		{
			name: "join table with very large ID",
			input: `
_columns: [time, tags, payload]
tags:
  a: 999999999999999999
  b: 2
data:
  - ["2024-01-01", 1, {x: 1}]
`,
			wantErr: true,
			errMsg:  "invalid join table ID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CPDToJSONL(strings.NewReader(tt.input))
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParse_MetadataMergeAcrossDocs(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "recursive merge",
			input: `
---
_version: 1
_meta:
  location: lab1
  device:
    id: sensorA
authors:
  alice: 1
_columns: [time, authors, payload]
data:
  - ["2024-01-01", 1, {foo: 1}]
---
_meta:
  device:
    type: temp
data:
  - ["2024-01-02", 1, {bar: 2}]
`,
			want: `{"_version":1,"_meta.location":"lab1","_meta.device.id":"sensorA","time":"2024-01-01","authors":"alice","foo":1}
{"_version":1,"_meta.location":"lab1","_meta.device.id":"sensorA","_meta.device.type":"temp","time":"2024-01-02","authors":"alice","bar":2}
`,
		},
		{
			name: "_meta merge with no overlap",
			input: `
---
_meta:
  env: prod
_columns: [time, payload]
data:
  - ["2024-01-01", {a: 1}]
---
_meta:
  region: us-west
_columns: [time, payload]
data:
  - ["2024-01-02", {b: 2}]
`,
			want: `{"_meta.env":"prod","time":"2024-01-01","a":1}
{"_meta.env":"prod","_meta.region":"us-west","time":"2024-01-02","b":2}
`,
		},
		{
			name: "_meta merge with deep nesting",
			input: `
---
_meta:
  a:
    b:
      c: 1
_columns: [time, payload]
data:
  - ["2024-01-01", {foo: true}]
---
_meta:
  a:
    b:
      d: 2
_columns: [time, payload]
data:
  - ["2024-01-02", {bar: false}]
`,
			want: `{"_meta.a.b.c":1,"time":"2024-01-01","foo":true}
{"_meta.a.b.c":1,"_meta.a.b.d":2,"time":"2024-01-02","bar":false}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonl, err := CPDToJSONL(strings.NewReader(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.want, jsonl)
		})
	}
}

func TestRoundTrip_JSONL_Stable(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		transformedOutput string
	}{
		{
			name:              "basic JSONL round-trip",
			input:             `{"time":"2024-01-01","authors":["alice"],"foo":"bar"}`,
			transformedOutput: "", // no change
		},
		{
			name:              "round-trip with null join",
			input:             `{"time":"2024-01-01","authors":null,"note":"no author"}`,
			transformedOutput: `{"time":"2024-01-01","note":"no author"}`,
		},
		{
			name:              "round-trip with multiple join entries",
			input:             `{"time":"2024-01-01","authors":["alice","bob"],"note":"collab"}`,
			transformedOutput: "",
		},
		{
			name:              "round-trip with payload only",
			input:             `{"time":"2024-01-01","foo":123,"bar":false}`,
			transformedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml, err := JSONLToCPD(strings.NewReader(tt.input))
			assert.NoError(t, err)

			jsonl, err := CPDToJSONL(strings.NewReader(yaml))
			assert.NoError(t, err)

			expected := tt.transformedOutput
			if len(expected) == 0 {
				expected = tt.input
			}

			assert.Equal(t, expected+"\n", jsonl)
		})
	}
}

func TestCPDToJSONL_ExpansionCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "minimal round trip",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice","foo":"bar"}
`,
		},
		{
			name: "row with omitted trailing fields",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice"}
`,
		},
		{
			name: "row with null payload",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, null]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice"}
`,
		},
		{
			name: "row with full payload and multiple join",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1,2], {x: 1, y: 2}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":["alice","bob"],"x":1,"y":2}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonl, err := CPDToJSONL(strings.NewReader(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.want, jsonl)
		})
	}
}
func TestParse_JoinTable_ManyToMany(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "multiple authors",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, 2], {note: "test"}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":["alice","bob"],"note":"test"}
`,
		},
		{
			name: "empty many-to-many join",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-02T00:00:00Z", [], {note: "none"}]
`,
			want: `{"time":"2024-01-02T00:00:00Z","authors":[],"note":"none"}
`,
		},
		{
			name: "single item many-to-many",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-03T00:00:00Z", [1], {note: "solo"}]
`,
			want: `{"time":"2024-01-03T00:00:00Z","authors":["alice"],"note":"solo"}
`,
		},
		{
			name: "invalid join ID",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-04T00:00:00Z", [1, 999], {note: "bad id"}]
`,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonl, err := CPDToJSONL(strings.NewReader(tt.input))
			if tt.name == "invalid join ID" {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, jsonl)
			}
		})
	}
}

func TestParse_JoinTable_Null(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "null join value",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", null, {note: "no author"}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","note":"no author"}` + "\n",
		},
		{
			name: "null in many-to-many join",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, null, 2], {note: "mixed authors"}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":["alice","bob"],"note":"mixed authors"}` + "\n",
		},
		{
			name: "null payload with join",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, null]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "multiple null joins",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", null, null, {note: "no joins"}]
`,
			want: `{"time":"2024-01-01T00:00:00Z","note":"no joins"}` + "\n",
		},
		{
			name: "null join with metadata",
			input: `
_version: 1
_meta:
  location: lab1
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", null, {note: "no author"}]
`,
			want: `{"_version":1,"_meta.location":"lab1","time":"2024-01-01T00:00:00Z","note":"no author"}` + "\n",
		},
		{
			name: "null join with trailing omitted fields",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", null]
`,
			want: `{"time":"2024-01-01T00:00:00Z"}` + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonl, err := CPDToJSONL(strings.NewReader(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.want, jsonl)
		})
	}
}

func TestValidation_RowLengthTooLong(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "row length too long",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}, "extra"]
`,
			wantErr: true,
			errMsg:  "data row 0 has 4 values but only 3 columns defined",
		},
		{
			name: "row length too long with join table",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1, 1, {foo: "bar"}, "extra", "extra2"]
`,
			wantErr: true,
			errMsg:  "data row 0 has 6 values but only 4 columns defined",
		},
		{
			name: "multiple rows with different lengths",
			input: `
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
  - ["2024-01-02T00:00:00Z", 1, {foo: "bar"}, "extra"]
`,
			wantErr: true,
			errMsg:  "data row 1 has 4 values but only 3 columns defined",
		},
		{
			name: "row length too long with metadata",
			input: `
_version: 1
_meta:
  location: lab1
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}, "extra"]
`,
			wantErr: true,
			errMsg:  "data row 0 has 4 values but only 3 columns defined",
		},
		{
			name: "row length too long in second document",
			input: `
---
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
---
data:
  - ["2024-01-02T00:00:00Z", 1, {foo: "bar"}, "extra"]
`,
			wantErr: true,
			errMsg:  "data row 0 has 4 values but only 3 columns defined",
		},
		{
			name: "valid row with trailing omitted fields",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			wantErr: false,
		},
		{
			name: "valid row with null and omitted fields",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", null]
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CPDToJSONL(strings.NewReader(tt.input))
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParse_RowWithTrailingOmission(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "row with trailing omission",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
  bob: 2
topic:
  food: 1
  recovery: 2
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "row with trailing omission and metadata",
			input: `
_version: 1
_meta:
  location: lab1
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			want: `{"_version":1,"_meta.location":"lab1","time":"2024-01-01T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "row with trailing omission in second document",
			input: `
---
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1, 1]
---
data:
  - ["2024-01-02T00:00:00Z", 1]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice","topic":"food"}
{"time":"2024-01-02T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "row with trailing omission and null join",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", null]
`,
			want: `{"time":"2024-01-01T00:00:00Z"}` + "\n",
		},
		{
			name: "row with trailing omission and array join",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
  bob: 2
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", [1, 2]]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":["alice","bob"]}` + "\n",
		},
		{
			name: "row with trailing omission and payload",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1, 1, {note: "test"}]
  - ["2024-01-02T00:00:00Z", 1]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice","topic":"food","note":"test"}
{"time":"2024-01-02T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "row with trailing omission and scalar payload",
			input: `
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1, 1, "{temp: 23.4}"]
  - ["2024-01-02T00:00:00Z", 1]
`,
			want: `{"time":"2024-01-01T00:00:00Z","authors":"alice","topic":"food","temp":23.4}
{"time":"2024-01-02T00:00:00Z","authors":"alice"}` + "\n",
		},
		{
			name: "row with trailing omission and nested metadata",
			input: `
_version: 1
_meta:
  device:
    id: sensor1
    type: temp
_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			want: `{"_version":1,"_meta.device.id":"sensor1","_meta.device.type":"temp","time":"2024-01-01T00:00:00Z","authors":"alice"}` + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonl, err := CPDToJSONL(strings.NewReader(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.want, jsonl)
		})
	}
}

func TestValidation_MissingJoinTable(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "scalar join ID with no join table is allowed",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
`,
			wantErr: false,
		},
		{
			name: "missing join table in second document",
			input: `
---
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
---
data:
  - ["2024-01-02T00:00:00Z", 2, {foo: "baz"}]
`,
			wantErr: true,
			errMsg:  "unknown join ID", // still correct: 2 isn't mapped
		},
		{
			name: "null join ID without join table is allowed",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", null, {foo: "bar"}]
`,
			wantErr: false,
		},
		{
			name: "array join ID with no join table is NOT allowed",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", [1, 2], {foo: "bar"}]
`,
			wantErr: true,
			errMsg:  "join table not found for column",
		},
		{
			name: "scalar join ID with metadata but no join table is allowed",
			input: `
_version: 1
_meta:
  location: lab1
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
`,
			wantErr: false,
		},
		{
			name: "missing join table with trailing omitted fields",
			input: `
_columns: [time, authors, topic, payload]
data:
  - ["2024-01-01T00:00:00Z", 1]
`,
			wantErr: true,
			errMsg:  "row length does not match _columns", // structural error, not join-specific
		},
		{
			name: "scalar join ID with scalar payload as string",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", 1, "{temp: 23.4}"]
`,
			wantErr: false, // valid as long as payload is not required to be a structured object
		},
		{
			name: "empty array join ID is not allowed without join table",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", [], {foo: "bar"}]
`,
			wantErr: true,
			errMsg:  "join table not found for column",
		},
		{
			name: "scalar join ID as string is allowed",
			input: `
_columns: [time, authors, payload]
data:
  - ["2024-01-01T00:00:00Z", "1", {foo: "bar"}]
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CPDToJSONL(strings.NewReader(tt.input))
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidation_InvalidJoinFieldType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "string instead of int",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", "alice", {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "float instead of int",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1.5, {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "boolean instead of int",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", true, {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "object instead of int",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", {id: 1}, {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "array with invalid types",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, "bob", 2.5], {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "nested array",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [[1], [2]], {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "array with null",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, null, 2], {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "array with object",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, {id: 2}], {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "invalid type in second document",
			input: `---
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", 1, {foo: "bar"}]
---
data:
  - ["2024-01-02T00:00:00Z", "bob", {foo: "baz"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "invalid type with metadata",
			input: `_version: 1
_meta:
  location: lab1
_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", "alice", {foo: "bar"}]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "invalid type with trailing omitted fields",
			input: `_columns: [time, authors, topic, payload]
authors:
  alice: 1
topic:
  food: 1
data:
  - ["2024-01-01T00:00:00Z", "alice"]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "invalid type with scalar payload",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", "alice", "{temp: 23.4}"]`,
			wantErr: true,
			errMsg:  "invalid join ID",
		},
		{
			name: "string value allowed if not specified as join table",
			input: `_columns: [time, author, payload]
data:
  - ["2024-01-01T00:00:00Z", "alice", "{temp: 23.4}"]`,
			wantErr: false,
		},
		{
			name: "valid null value",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", null, {foo: "bar"}]`,
			wantErr: false,
		},
		{
			name: "valid empty array",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
data:
  - ["2024-01-01T00:00:00Z", [], {foo: "bar"}]`,
			wantErr: false,
		},
		{
			name: "valid array of integers",
			input: `_columns: [time, authors, payload]
authors:
  alice: 1
  bob: 2
data:
  - ["2024-01-01T00:00:00Z", [1, 2], {foo: "bar"}]`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CPDToJSONL(strings.NewReader(tt.input))
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
