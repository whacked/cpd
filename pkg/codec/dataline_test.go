package codec

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/whacked/yamdb/pkg/testdata"
)

func TestParseRecordToValuesWithColumns(t *testing.T) {
	for i, tc := range testdata.TestRecords {
		t.Run(fmt.Sprintf("Record %d", i), func(t *testing.T) {
			received, err := ParseRecordToValuesWithColumns(tc.Input)
			if err != nil {
				t.Fatalf("Failed to parse record: %v", err)
			}

			if !reflect.DeepEqual(received, tc.Expected) {
				t.Errorf("\nInput: %s\nExpected: %+v\nReceived: %+v", tc.Input, tc.Expected, received)
				// Debug print the types
				t.Logf("Expected types:")
				for j, v := range tc.Expected.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
				t.Logf("Received types:")
				for j, v := range received.Values {
					t.Logf("  [%d] %T: %v", j, v, v)
				}
			}
		})
	}
}
