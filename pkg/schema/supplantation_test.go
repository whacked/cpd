package schema

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/whacked/yamdb/pkg/codec"
	"github.com/whacked/yamdb/pkg/testdata"
	"github.com/whacked/yamdb/pkg/types"
)

func TestSupplantRecord(t *testing.T) {
	for i, tc := range testdata.TestSupplantationCases {
		t.Run(fmt.Sprintf("Case %d: %s", i, tc.Name), func(t *testing.T) {
			// Parse the input record first
			parsed, err := codec.ParseRecordToValuesWithColumns(tc.InputRecord)
			if err != nil {
				t.Fatalf("Failed to parse input record: %v", err)
			}

			// Create current schema from start schema
			current := types.ValuesWithColumns{
				Values:  make([]interface{}, len(tc.StartSchema)),
				Columns: tc.StartSchema,
			}

			// Call SupplantRecord with parsed values
			got, gotSchema, changed, err := SupplantRecord(current, parsed)
			if err != nil {
				t.Fatalf("SupplantRecord() error = %v", err)
			}

			// Check if schema changed
			if len(tc.StartSchema) == 0 && !changed {
				t.Error("SupplantRecord() should report schema changed for empty start schema")
			}

			// Check schema
			if !reflect.DeepEqual(gotSchema, tc.CombinedRecord.Columns) {
				t.Errorf("\nExpected schema: %+v\nReceived schema: %+v", tc.CombinedRecord.Columns, gotSchema)
			}

			// Check values and columns
			if !reflect.DeepEqual(got, tc.CombinedRecord) {
				t.Errorf("\nInput: %s\nExpected: %+v\nReceived: %+v", tc.InputRecord, tc.CombinedRecord, got)
				// Debug print the types
				t.Logf("Expected types:")
				for j, v := range tc.CombinedRecord.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
				t.Logf("Received types:")
				for j, v := range got.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
			}
		})
	}
}
