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
  - ["2024-06-13T13:30:00Z", 1, null, {note: "light snack"}]
  - ["2024-06-14T14:40:04Z", 2, ~, {note: "big drink"}]`,
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
					{
						Values: func() *orderedmapjson.AnyOrderedMap {
							m := orderedmapjson.NewAnyOrderedMap()
							m.Set("time", "2024-06-14T14:40:04Z")
							m.Set("authors", "bob")
							m.Set("note", "big drink")
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
		/* NOTE: this implies we omit keys with null values. not sure this is a good idea.
		{
			name:              "round-trip with null join",
			input:             `{"time":"2024-01-01","authors":null,"note":"no author"}`,
			transformedOutput: `{"time":"2024-01-01","note":"no author"}`,
		},
		// */
		{
			name:              "round-trip with multiple join entries",
			input:             `{"time":"2024-01-01","authors":["alice","bob"],"note":"collab"}`,
			transformedOutput: "",
		},
		{
			name:              "round-trip with payload only",
			input:             `{"time":"2024-01-01","foo":123,"bar":false,"baz":true,"fop":456,"alpha":"x","zeta":null}`,
			transformedOutput: "",
		},
		{
			name:              "round-trip key order with nested objects",
			input:             `{"time":"2024-01-01","nested":{"z":3,"y":2,"x":1},"b":false,"a":true,"meta":{"last":"z","first":"a"}}`,
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
			wantErr: false,
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
			wantErr: false,
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

func TestValidation_SpecialCharacterKeys(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "keys starting with @ in payload",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"@merge": true, "@start": "2024-01-01", "@end": "2024-01-31"}]`,
			wantErr: false, // Should work if parser handles quoted keys
		},
		{
			name: "keys starting with @ in metadata",
			input: `_meta:
  "@version": "1.0"
  "@schema": "http://example.com/schema"
_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {foo: "bar"}]`,
			wantErr: false, // Should work if parser handles quoted keys
		},
		{
			name: "keys starting with @ in join table",
			input: `_columns: [time, tags, payload]
tags:
  "@system": 1
  "@user": 2
data:
  - ["2024-01-01T00:00:00Z", 1, {note: "system tag"}]`,
			wantErr: false, // Should work if parser handles quoted keys
		},
		{
			name: "mixed special character keys",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"@merge": true, "normal": "value", "@start": "2024-01-01"}]`,
			wantErr: false,
		},
		{
			name: "keys with other special characters",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"$ref": "#/definitions/User", "&copy": "2024", "?query": "value"}]`,
			wantErr: false,
		},
		{
			name: "nested special character keys",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"nested": {"@inner": "value", "normal": "other"}}]`,
			wantErr: false,
		},
		{
			name: "special character keys in array",
			input: `_columns: [time, items, payload]
items:
  "@item1": 1
  "@item2": 2
data:
  - ["2024-01-01T00:00:00Z", [1, 2], {note: "array with special keys"}]`,
			wantErr: false,
		},
		{
			name: "special character keys across documents",
			input: `---
_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"@merge": true}]
---
data:
  - ["2024-01-02T00:00:00Z", {"@start": "2024-01-02"}]`,
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

func TestValidation_EmptyStringKeys(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty string key in join table",
			input: `_columns: [time, tags, payload]
tags:
  "": 1
  "normal": 2
data:
  - ["2024-01-01T00:00:00Z", 1, {note: "empty tag"}]`,
			wantErr: true, // Current parser rejects empty string keys in join tables
			errMsg:  "empty join table key",
		},
		{
			name: "empty string key in payload",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"": "empty key value", "normal": "value"}]`,
			wantErr: false, // Should work in payload
		},
		{
			name: "empty string key in metadata",
			input: `_meta:
  "": "empty meta key"
_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {foo: "bar"}]`,
			wantErr: false, // Should work in metadata
		},
		{
			name: "empty string key in array join",
			input: `_columns: [time, tags, payload]
tags:
  "": 1
  "normal": 2
data:
  - ["2024-01-01T00:00:00Z", [1, 2], {note: "mixed tags"}]`,
			wantErr: true, // Current parser rejects empty string keys in join tables
			errMsg:  "empty join table key",
		},
		{
			name: "empty string key with whitespace",
			input: `_columns: [time, tags, payload]
tags:
  " ": 1
  "normal": 2
data:
  - ["2024-01-01T00:00:00Z", 1, {note: "whitespace tag"}]`,
			wantErr: true, // Current parser rejects whitespace-only keys in join tables
			errMsg:  "empty join table key",
		},
		{
			name: "empty string key in nested object",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"nested": {"": "empty nested key"}}]`,
			wantErr: false, // Should work in nested payload
		},
		{
			name: "empty string key across documents",
			input: `---
_columns: [time, tags, payload]
tags:
  "": 1
data:
  - ["2024-01-01T00:00:00Z", 1, {note: "first doc"}]
---
tags:
  "": 2
data:
  - ["2024-01-02T00:00:00Z", 2, {note: "second doc"}]`,
			wantErr: true, // Should fail due to empty join table key
			errMsg:  "empty join table key",
		},
		{
			name: "empty string key with special characters",
			input: `_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {"": "empty", "@special": "value", "normal": "other"}]`,
			wantErr: false, // Should work in payload
		},
		{
			name: "empty string key in version",
			input: `_version: ""
_columns: [time, payload]
data:
  - ["2024-01-01T00:00:00Z", {foo: "bar"}]`,
			wantErr: false, // Should work for version field
		},
		{
			name: "empty string key in columns",
			input: `_columns: ["", time, payload]
data:
  - ["", "2024-01-01T00:00:00Z", {foo: "bar"}]`,
			wantErr: false, // Should work for column names
		},
		{
			name: "empty string value in join table (not key)",
			input: `_columns: [time, tags, payload]
tags:
  "normal": ""
data:
  - ["2024-01-01T00:00:00Z", "", {note: "empty value"}]`,
			wantErr: true, // Empty string values in join tables are treated as invalid join IDs
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

func TestRoundTrip_SpecialKeys(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "round-trip with @ keys",
			input: `{"time":"2024-01-01","@merge":true,"@start":"2024-01-01","@end":"2024-01-31","normal":"value"}`,
			want:  `{"time":"2024-01-01","@merge":true,"@start":"2024-01-01","@end":"2024-01-31","normal":"value"}`,
		},
		{
			name:  "round-trip with empty string key",
			input: `{"time":"2024-01-01","":"empty value","normal":"value"}`,
			want:  `{"time":"2024-01-01","":"empty value","normal":"value"}`,
		},
		{
			name:  "round-trip with empty string value",
			input: `{"time":"2024-01-01","emptyvalue":"","normal":"value"}`,
			want:  `{"time":"2024-01-01","emptyvalue":"","normal":"value"}`,
		},
		{
			name:  "round-trip with mixed special keys",
			input: `{"time":"2024-01-01","@merge":true,"":"empty","$ref":"#/definitions/User","&copy":"2024"}`,
			want:  `{"time":"2024-01-01","@merge":true,"":"empty","$ref":"#/definitions/User","\u0026copy":2024}`,
		},
		{
			name:  "round-trip with nested special keys",
			input: `{"time":"2024-01-01","nested":{"@inner":"value","":"empty nested","normal":"other"}}`,
			want:  `{"time":"2024-01-01","nested":{"@inner":"value","":"empty nested","normal":"other"}}`,
		},
		{
			name:  "round-trip with special keys in arrays",
			input: `{"time":"2024-01-01","tags":["@system","@user",""],"@meta":"value"}`,
			want:  `{"time":"2024-01-01","tags":["@system","@user",""],"@meta":"value"}`,
		},
		{
			name:  "round-trip with question mark key",
			input: `{"time":"2024-01-01","?query":"value","normal":"other"}`,
			want:  `{"time":"2024-01-01","?query":"value","normal":"other"}`,
		},
		{
			name:  "round-trip with hash key",
			input: `{"time":"2024-01-01","#comment":"value","normal":"other"}`,
			want:  `{"time":"2024-01-01","#comment":"value","normal":"other"}`,
		},
		{
			name:  "round-trip with percent key",
			input: `{"time":"2024-01-01","%percent":"value","normal":"other"}`,
			want:  `{"time":"2024-01-01","%percent":"value","normal":"other"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml, err := JSONLToCPD(strings.NewReader(tt.input))
			if err != nil {
				t.Errorf("JSONLToCPD failed: %v", err)
			}

			jsonl, err := CPDToJSONL(strings.NewReader(yaml))
			if err != nil {
				t.Errorf("CPDToJSONL failed: %v", err)
			}

			assert.Equal(t, tt.want+"\n", jsonl)
		})
	}
}

func TestJSONLToCPD_EmptyStringHandling(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string value should not create join table entry",
			input: `{"time":"2024-01-01","mass":""}`,
			want:  `{"time":"2024-01-01","mass":""}`,
		},
		{
			name:  "empty string value with other values",
			input: `{"time":"2024-01-01","mass":"","weight":"100kg","status":"active"}`,
			want:  `{"time":"2024-01-01","mass":"","weight":"100kg","status":"active"}`,
		},
		{
			name:  "empty string value in array",
			input: `{"time":"2024-01-01","tags":["tag1","","tag3"]}`,
			want:  `{"time":"2024-01-01","tags":["tag1","","tag3"]}`,
		},
		{
			name:  "empty string value in nested object",
			input: `{"time":"2024-01-01","data":{"name":"test","description":"","value":123}}`,
			want:  `{"time":"2024-01-01","data":{"name":"test","description":"","value":123}}`,
		},
		{
			name:  "multiple empty string values",
			input: `{"time":"2024-01-01","field1":"","field2":"","field3":"value"}`,
			want:  `{"time":"2024-01-01","field1":"","field2":"","field3":"value"}`,
		},
		{
			name:  "empty string value with special characters",
			input: `{"time":"2024-01-01","@merge":"","normal":"value"}`,
			want:  `{"time":"2024-01-01","@merge":"","normal":"value"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml, err := JSONLToCPD(strings.NewReader(tt.input))
			if err != nil {
				// Skip tests that fail due to YAML parsing limitations
				t.Errorf("JSONLToCPD failed: %v", err)
			}

			jsonl, err := CPDToJSONL(strings.NewReader(yaml))
			if err != nil {
				// Skip tests that fail due to YAML parsing limitations
				t.Errorf("CPDToJSONL failed: %v", err)
			}

			assert.Equal(t, tt.want+"\n", jsonl)
		})
	}
}

func TestJSONLToCPD_RealWorldExample(t *testing.T) {
	input := `{"time": "2019-03-13 08:04:30+0800", "entry": "\u80e1\u6912\u9905 half", "photo": ["20190313_080346.jpg"], "category": "ingest", "device": "SM-N912"}
{"time": "2019-03-13 08:19:36+0800", "entry": "\u62b9\u8336\u7d05\u8c46 ijysheng", "photo": ["20190313_081745.jpg", "20190313_081929.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 08:32:00+0800", "entry": "\u852c\u679c\u540c\u5802 krunchee-veg", "photo": ["20190313_083235.jpg", "20190313_083253.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 08:56:06+0800", "entry": "\u5929\u3077\u3089\u308c\u3093\u3053\u3093\u305b\u3093\u3079\u3044", "mass": "12g", "barcode": ["EAN_13:4990855064882"], "photo": ["20190313_085454.jpg", "20190313_085505.jpg"], "device": "SM-N910C"}
{"time": "2019-03-13 10:53:35+0800", "entry": "rivon chocolate pineapple cake", "photo": ["20190313_105301.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 14:28:58+0800", "entry": "\u91d1\u828b\u96d9\u559c", "photo": ["20190313_142727.jpg", "20190313_142828.jpg"], "category": "ingest", "device": "decoy123"}
{"time": "2019-03-13 19:04:08+0800", "entry": "dinner food start", "photo": ["20190313_190341.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 21:51:48+0800", "entry": "\u5fa1\u54c1\u5713 \u51b0\u706b\u6e6f\u5713", "photo": ["20190313_215022.jpg"], "device": "SM-N910C"}
{"time": "2019-03-13 22:45:16+0800", "entry": "\u9999\u74dc\uff0ccanteloupe", "photo": ["20190313_224448.jpg"], "category": "ingest", "device": "fakefake"}
{"time": "2019-03-14 07:05:00+0800", "entry": "\u5149\u6cc9\u51b7\u6ce1\u8336\u51b0\u91c0\u70cf\u9f8d clear the dt", "mass": "585g", "barcode": ["EAN_13:4710105062884"], "photo": ["20190314_071813.jpg", "20190314_071828.jpg"], "dt": "21s", "device": "SM-N910C"}
{"time": "2019-03-14 08:35:15+0800", "entry": "banana", "photo": ["20190314_083356.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-14 08:57:09+0800", "photo": ["20190314_085653.jpg"], "category": "blahblah", "device": "SM-N910C"}
{"time": "2019-03-14 10:51:05+0800", "photo": ["20190314_105058.jpg"], "category": "foobar", "device": "SM-N910C"}
{"time": "2019-03-14 10:53:28+0800", "photo": ["20190314_105317.jpg"], "category": "ingest", "device": "SM-N910C"}`

	want := `_columns: [time, category, device, payload]
category:
  ingest: 1
  blahblah: 2
  foobar: 3
device:
  SM-N912: 1
  SM-N910C: 2
  decoy123: 3
  fakefake: 4
data:
  - ["2019-03-13 08:04:30+0800", 1, 1, {entry: "胡椒餅 half", photo: ["20190313_080346.jpg"]}]
  - ["2019-03-13 08:19:36+0800", 1, 2, {entry: "抹茶紅豆 ijysheng", photo: ["20190313_081745.jpg","20190313_081929.jpg"]}]
  - ["2019-03-13 08:32:00+0800", 1, 2, {entry: "蔬果同堂 krunchee-veg", photo: ["20190313_083235.jpg","20190313_083253.jpg"]}]
  - ["2019-03-13 08:56:06+0800", ~, 2, {entry: "天ぷられんこんせんべい", mass: "12g", barcode: ["EAN_13:4990855064882"], photo: ["20190313_085454.jpg","20190313_085505.jpg"]}]
  - ["2019-03-13 10:53:35+0800", 1, 2, {entry: "rivon chocolate pineapple cake", photo: ["20190313_105301.jpg"]}]
  - ["2019-03-13 14:28:58+0800", 1, 3, {entry: "金芋雙喜", photo: ["20190313_142727.jpg","20190313_142828.jpg"]}]
  - ["2019-03-13 19:04:08+0800", 1, 2, {entry: "dinner food start", photo: ["20190313_190341.jpg"]}]
  - ["2019-03-13 21:51:48+0800", ~, 2, {entry: "御品圓 冰火湯圓", photo: ["20190313_215022.jpg"]}]
  - ["2019-03-13 22:45:16+0800", 1, 4, {entry: "香瓜，canteloupe", photo: ["20190313_224448.jpg"]}]
  - ["2019-03-14 07:05:00+0800", ~, 2, {entry: "光泉冷泡茶冰釀烏龍 clear the dt", mass: "585g", barcode: ["EAN_13:4710105062884"], photo: ["20190314_071813.jpg","20190314_071828.jpg"], dt: "21s"}]
  - ["2019-03-14 08:35:15+0800", 1, 2, {entry: "banana", photo: ["20190314_083356.jpg"]}]
  - ["2019-03-14 08:57:09+0800", 2, 2, {photo: ["20190314_085653.jpg"]}]
  - ["2019-03-14 10:51:05+0800", 3, 2, {photo: ["20190314_105058.jpg"]}]
  - ["2019-03-14 10:53:28+0800", 1, 2, {photo: ["20190314_105317.jpg"]}]
`

	yaml, err := JSONLToCPD(strings.NewReader(input))
	if err != nil {
		t.Errorf("JSONLToCPD failed: %v", err)
	}

	assert.Equal(t, want, yaml)
}

func TestJSONLToCPD_PhotoFieldNotJoinTable(t *testing.T) {
	input := `{"time": "2019-03-13 08:04:30+0800", "entry": "胡椒餅 half", "photo": ["20190313_080346.jpg"], "category": "ingest", "device": "SM-N912"}
{"time": "2019-03-13 08:19:36+0800", "entry": "抹茶紅豆 ijysheng", "photo": ["20190313_081745.jpg", "20190313_081929.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 08:32:00+0800", "entry": "蔬果同堂 krunchee-veg", "photo": ["20190313_083235.jpg", "20190313_083253.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 08:56:06+0800", "entry": "天ぷられんこんせんべい", "mass": "12g", "barcode": ["EAN_13:4990855064882"], "photo": ["20190313_085454.jpg", "20190313_085505.jpg"], "device": "SM-N910C"}
{"time": "2019-03-13 10:53:35+0800", "entry": "rivon chocolate pineapple cake", "photo": ["20190313_105301.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 14:28:58+0800", "entry": "金芋雙喜", "photo": ["20190313_142727.jpg", "20190313_142828.jpg"], "category": "ingest", "device": "decoy123"}
{"time": "2019-03-13 19:04:08+0800", "entry": "dinner food start", "photo": ["20190313_190341.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-13 21:51:48+0800", "entry": "御品圓 冰火湯圓", "photo": ["20190313_215022.jpg"], "device": "SM-N910C"}
{"time": "2019-03-13 22:45:16+0800", "entry": "香瓜，canteloupe", "photo": ["20190313_224448.jpg"], "category": "ingest", "device": "fakefake"}
{"time": "2019-03-14 07:05:00+0800", "entry": "光泉冷泡茶冰釀烏龍 clear the dt", "mass": "585g", "barcode": ["EAN_13:4710105062884"], "photo": ["20190314_071813.jpg", "20190314_071828.jpg"], "dt": "21s", "device": "SM-N910C"}
{"time": "2019-03-14 08:35:15+0800", "entry": "banana", "photo": ["20190314_083356.jpg"], "category": "ingest", "device": "SM-N910C"}
{"time": "2019-03-14 08:57:09+0800", "photo": ["20190314_085653.jpg"], "category": "blahblah", "device": "SM-N910C"}
{"time": "2019-03-14 10:51:05+0800", "photo": ["20190314_105058.jpg"], "category": "foobar", "device": "SM-N910C"}
{"time": "2019-03-14 10:53:28+0800", "photo": ["20190314_105317.jpg"], "category": "ingest", "device": "SM-N910C"}`

	result, err := JSONLToCPD(strings.NewReader(input))
	if err != nil {
		t.Fatalf("JSONLToCPD failed: %v", err)
	}

	// Verify that photo is NOT in the columns (should be in payload)
	if strings.Contains(result, "_columns: [time, photo") {
		t.Errorf("photo should not be a join table column, but found in _columns")
	}

	// Verify that category and device ARE in the columns
	if !strings.Contains(result, "_columns: [time, category, device, payload]") {
		t.Errorf("expected category and device to be join table columns")
	}

	// Verify that photo appears in the payload section of the data
	if !strings.Contains(result, "photo: [") {
		t.Errorf("photo should appear in payload, but not found in data")
	}

	// Verify that category and device have join table definitions
	if !strings.Contains(result, "category:\n") {
		t.Errorf("category should have a join table definition")
	}
	if !strings.Contains(result, "device:\n") {
		t.Errorf("device should have a join table definition")
	}

	// Verify that photo does NOT have a join table definition
	if strings.Contains(result, "photo:\n") {
		t.Errorf("photo should not have a join table definition")
	}
}
