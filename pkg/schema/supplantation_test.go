package schema

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/whacked/yamdb/pkg/testdata"
)

func TestSupplantRecord(t *testing.T) {
	for i, tc := range testdata.TestSupplantationCases {
		t.Run(fmt.Sprintf("Case %d: %s", i, tc.name), func(t *testing.T) {
			got, gotSchema, changed, err := SupplantRecord(tc.StartSchema, tc.InputRecord)
			if err != nil {
				t.Fatalf("SupplantRecord() error = %v", err)
			}

			// Check if schema changed
			if len(tc.StartSchema) == 0 && !changed {
				t.Error("SupplantRecord() should report schema changed for empty start schema")
			}

			// Check schema
			if !reflect.DeepEqual(gotSchema, tc.EndSchema) {
				t.Errorf("\nExpected schema: %+v\nReceived schema: %+v", tc.EndSchema, gotSchema)
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
