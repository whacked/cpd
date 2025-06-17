package codec

import (
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
	"github.com/stretchr/testify/assert"
)

func TestCPDParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *CPDDocument
		wantErr bool
	}{
		{
			name: "basic document",
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
						Values: []interface{}{
							"2024-06-12T12:00:00Z",
							[]interface{}{1, 2},
							1,
							map[string]interface{}{"note": "ate natto"},
						},
					},
					{
						Values: []interface{}{
							"2024-06-13T13:30:00Z",
							1,
							nil,
							map[string]interface{}{"note": "light snack"},
						},
					},
				},
				Meta:    orderedmapjson.NewAnyOrderedMap(),
				Version: "",
			},
			wantErr: false,
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

func TestJoinTableValidation(t *testing.T) {
	// TODO: Add tests for join table validation
}

func TestMetadataMerging(t *testing.T) {
	// TODO: Add tests for metadata merging
}

func TestJSONLConversion(t *testing.T) {
	// TODO: Add tests for JSONL conversion
}
