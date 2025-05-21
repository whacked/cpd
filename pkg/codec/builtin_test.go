package codec

import (
	"testing"

	"github.com/whacked/yamdb/pkg/types"
)

func TestBuiltinCodecs(t *testing.T) {
	codecs := BuiltinCodecs()
	if _, exists := codecs["jsonl"]; !exists {
		t.Error("jsonl codec not found in builtin codecs")
	}

	// Test JSONL codec
	record := types.RecordWithMetadata{
		Record: map[string]interface{}{
			"field1": "test",
			"field2": 123,
		},
		Columns: []types.ColumnInfo{
			{Name: "field1", Type: types.TypeString},
			{Name: "field2", Type: types.TypeInt},
		},
	}

	result, err := codecs["jsonl"](record)
	if err != nil {
		t.Errorf("jsonl codec failed: %v", err)
	}

	jsonl, ok := result.(string)
	if !ok {
		t.Errorf("expected string result, got %T", result)
	}

	if jsonl == "" {
		t.Error("got empty JSONL string")
	}
}
