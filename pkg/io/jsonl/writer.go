package jsonl

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
)

// OrderedMapToJSONL converts an ordered map to a JSONL string
func OrderedMapToJSONL(orderedMap *orderedmapjson.AnyOrderedMap) string {
	if orderedMap == nil {
		return "{}"
	}

	var buf strings.Builder
	buf.WriteString("{")
	first := true
	for el := orderedMap.Front(); el != nil; el = el.Next() {
		if !first {
			buf.WriteString(", ")
		}
		first = false

		// Key
		buf.WriteString(formatJSONLKey(el.Key))
		buf.WriteString(": ")

		// Value
		buf.WriteString(formatJSONLValue(el.Value))
	}
	buf.WriteString("}")
	return buf.String()
}

// formatJSONLKey formats a key for JSONL output
func formatJSONLKey(s string) string {
	// Always quote keys in JSON
	return strconv.Quote(s)
}

// formatJSONLValue formats a value for JSONL output
func formatJSONLValue(v interface{}) string {
	switch val := v.(type) {
	case *orderedmapjson.AnyOrderedMap:
		return OrderedMapToJSONL(val)
	case []interface{}:
		var buf strings.Builder
		buf.WriteString("[")
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(formatJSONLValue(item))
		}
		buf.WriteString("]")
		return buf.String()
	case []string:
		var buf strings.Builder
		buf.WriteString("[")
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(strconv.Quote(item))
		}
		buf.WriteString("]")
		return buf.String()
	case string:
		return strconv.Quote(val)
	case float64:
		return strconv.FormatFloat(val, 'f', 1, 64)
	case int:
		return strconv.Itoa(val)
	case nil:
		return "null"
	default:
		// For any other type, use json.Marshal to ensure proper JSON formatting
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", val)
	}
}
