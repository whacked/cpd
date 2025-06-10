package yaml

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/jsonl"
)

func CleanString(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	s = strings.Join(lines, "\n")
	return strings.TrimSpace(s)
}

// needsQuoting determines if a string needs to be quoted in YAML output
// based on YAML 1.2 spec rules
func needsQuoting(s string) bool {
	// Empty strings need quoting
	if s == "" {
		return true
	}

	// Check if string contains any special characters that require quoting
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r', // whitespace
			'[', ']', '{', '}', // flow collection indicators
			',', ':', // flow indicators
			'#', '&', '*', '!', '|', '>', '\'', '"', '%', '@', '`', // special characters
			'\\': // escape character
			return true
		}
	}

	// Check if string starts with special characters
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "?") || strings.HasPrefix(s, ":") ||
		strings.HasPrefix(s, "{") || strings.HasPrefix(s, "}") ||
		strings.HasPrefix(s, "[") || strings.HasPrefix(s, "]") ||
		strings.HasPrefix(s, ",") || strings.HasPrefix(s, "&") ||
		strings.HasPrefix(s, "*") || strings.HasPrefix(s, "#") ||
		strings.HasPrefix(s, "!") || strings.HasPrefix(s, "|") ||
		strings.HasPrefix(s, ">") || strings.HasPrefix(s, "'") ||
		strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "%") ||
		strings.HasPrefix(s, "@") || strings.HasPrefix(s, "`") {
		return true
	}

	// Check if string looks like a number, boolean, or null
	if s == "true" || s == "false" || s == "null" || s == "~" ||
		s == "yes" || s == "no" || s == "on" || s == "off" {
		return true
	}

	// Check if string looks like a number
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}

	return false
}

// quoteString properly quotes a string for YAML output
func quoteString(s string, forceQuote bool) string {
	if forceQuote || needsQuoting(s) {
		// Use strconv.Quote which properly escapes special characters
		return strconv.Quote(s)
	}
	return s
}

// WriterOptions configures the YAML writer behavior
type WriterOptions struct {
	// QuoteStrings controls whether string values are quoted in the output
	QuoteStrings bool
}

// Writer writes YAML documents with special handling for data arrays
type Writer struct {
	w       io.Writer
	options WriterOptions
}

// NewWriter creates a new YAML writer
func NewWriter(w io.Writer, opts ...WriterOptions) *Writer {
	options := WriterOptions{}
	if len(opts) > 0 {
		options = opts[0]
	}
	return &Writer{
		w:       w,
		options: options,
	}
}

// Write writes a single YAML document
func (w *Writer) Write(doc *Document) error {
	if doc == nil || doc.Data == nil {
		return nil
	}

	// First write all keys except 'data'
	for el := doc.Data.Front(); el != nil; el = el.Next() {
		if el.Key == "data" {
			continue
		}

		// Write the key-value pair
		if err := w.writeKeyValue(el.Key, el.Value, 0); err != nil {
			return err
		}
	}

	// Then handle the data array specially
	if data, _ := doc.Data.Get("data"); data != nil {
		if dataArray, ok := data.([]interface{}); ok {
			fmt.Fprintf(w.w, "data:\n")
			for _, item := range dataArray {
				fmt.Fprintf(w.w, "  - ")
				if err := w.writeDataItem(item); err != nil {
					return err
				}
				fmt.Fprintf(w.w, "\n")
			}
		}
	}

	return nil
}

// writeKeyValue writes a key-value pair with proper indentation
func (w *Writer) writeKeyValue(key string, value interface{}, indent int) error {
	indentStr := strings.Repeat("  ", indent)
	fmt.Fprintf(w.w, "%s%s: ", indentStr, key)

	switch v := value.(type) {
	case *orderedmapjson.AnyOrderedMap:
		fmt.Fprintf(w.w, "\n")
		for el := v.Front(); el != nil; el = el.Next() {
			if err := w.writeKeyValue(el.Key, el.Value, indent+1); err != nil {
				return err
			}
		}
	case []interface{}:
		fmt.Fprintf(w.w, "\n")
		for _, item := range v {
			fmt.Fprintf(w.w, "%s  - ", indentStr)
			if err := w.writeDataItem(item); err != nil {
				return err
			}
			fmt.Fprintf(w.w, "\n")
		}
	case int, float64:
		fmt.Fprintf(w.w, "%v\n", v)
	default:
		stringifiedValue := fmt.Sprintf("%v", v)
		if w.options.QuoteStrings || strings.Contains(stringifiedValue, " ") {
			fmt.Fprintf(w.w, "%q\n", stringifiedValue)
		} else {
			fmt.Fprintf(w.w, "%v\n", stringifiedValue)
		}
	}
	return nil
}

func (w *Writer) writeDataItem(item interface{}) error {
	var buf strings.Builder
	if err := w.formatValue(&buf, item); err != nil {
		return err
	}
	fmt.Fprintf(w.w, "%s", buf.String())
	return nil
}

// formatValue recursively formats a value into buf
func (w *Writer) formatValue(buf *strings.Builder, val interface{}) error {
	switch v := val.(type) {
	case *orderedmapjson.AnyOrderedMap:
		// Use OrderedMapToJSONL for map formatting
		buf.WriteString(jsonl.OrderedMapToJSONL(v))

	case []interface{}:
		buf.WriteString("[")
		for i, item := range v {
			if i > 0 {
				buf.WriteString(", ")
			}
			if err := w.formatValue(buf, item); err != nil {
				return err
			}
		}
		buf.WriteString("]")

	case []string:
		buf.WriteString("[")
		for i, item := range v {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(w.formatScalar(item))
		}
		buf.WriteString("]")

	case string:
		buf.WriteString(w.formatScalar(v))
	case float64:
		buf.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	case int:
		buf.WriteString(strconv.Itoa(v))
	default:
		buf.WriteString(fmt.Sprintf("%v", v))
	}
	return nil
}

// formatKey handles quoting for map keys
func (w *Writer) formatKey(s string) string {
	if w.options.QuoteStrings || needsQuoting(s) {
		return strconv.Quote(s)
	}
	return s
}

// formatScalar handles quoting for scalar values
func (w *Writer) formatScalar(s string) string {
	if w.options.QuoteStrings || needsQuoting(s) {
		return strconv.Quote(s)
	}
	return s
}
