package codec

import (
	"fmt"

	"github.com/whacked/yamdb/pkg/types"
)

// BuiltinCodecs returns a map of built-in codec functions
func BuiltinCodecs() map[string]CodecFunc {
	return map[string]CodecFunc{
		"jsonl": func(v interface{}) (interface{}, error) {
			if record, ok := v.(types.RecordWithMetadata); ok {
				return RecordToJSONL(record)
			}
			return nil, fmt.Errorf("expected RecordWithMetadata, got %T", v)
		},
	}
}
