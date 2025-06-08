package yaml

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
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

// writeDataItem writes a single data item in JSONL format
func (w *Writer) writeDataItem(item interface{}) error {
	switch v := item.(type) {
	case *orderedmapjson.AnyOrderedMap:
		// Build JSON manually to preserve order and control formatting
		var buf strings.Builder
		buf.WriteString("{")

		first := true
		for el := v.Front(); el != nil; el = el.Next() {
			if !first {
				buf.WriteString(", ")
			}
			first = false

			// Write key
			if w.options.QuoteStrings || strings.Contains(el.Key, " ") {
				buf.WriteString(fmt.Sprintf("%q", el.Key))
			} else {
				buf.WriteString(el.Key)
			}
			buf.WriteString(": ")

			// Write value
			switch val := el.Value.(type) {
			case string:
				if w.options.QuoteStrings || strings.Contains(val, " ") {
					buf.WriteString(fmt.Sprintf("%q", val))
				} else {
					buf.WriteString(val)
				}
			case float64:
				buf.WriteString(fmt.Sprintf("%g", val))
			case []interface{}:
				buf.WriteString("[")
				for i, item := range val {
					if i > 0 {
						buf.WriteString(", ")
					}
					if str, ok := item.(string); ok {
						if w.options.QuoteStrings {
							buf.WriteString(fmt.Sprintf("%q", str))
						} else {
							buf.WriteString(str)
						}
					} else {
						buf.WriteString(fmt.Sprintf("%v", item))
					}
				}
				buf.WriteString("]")
			case *orderedmapjson.AnyOrderedMap:
				// Recursively handle nested maps
				buf.WriteString("{")
				first := true
				for el := val.Front(); el != nil; el = el.Next() {
					if !first {
						buf.WriteString(", ")
					}
					first = false

					if w.options.QuoteStrings || strings.Contains(el.Key, " ") {
						buf.WriteString(fmt.Sprintf("%q", el.Key))
					} else {
						buf.WriteString(el.Key)
					}
					buf.WriteString(": ")

					switch val := el.Value.(type) {
					case string:
						if w.options.QuoteStrings || strings.Contains(val, " ") {
							buf.WriteString(fmt.Sprintf("%q", val))
						} else {
							buf.WriteString(val)
						}
					case []interface{}:
						buf.WriteString("[")
						for i, item := range val {
							if i > 0 {
								buf.WriteString(", ")
							}
							if str, ok := item.(string); ok {
								if w.options.QuoteStrings || strings.Contains(str, " ") {
									buf.WriteString(fmt.Sprintf("%q", str))
								} else {
									buf.WriteString(str)
								}
							} else {
								buf.WriteString(fmt.Sprintf("%v", item))
							}
						}
						buf.WriteString("]")
					case *orderedmapjson.AnyOrderedMap:
						// Recursively handle deeper nested maps
						if err := w.writeDataItem(val); err != nil {
							return err
						}
					case []string:
						buf.WriteString("[")
						for i, item := range val {
							if i > 0 {
								buf.WriteString(", ")
							}
							if w.options.QuoteStrings || strings.Contains(item, " ") {
								buf.WriteString(fmt.Sprintf("%q", item))
							} else {
								buf.WriteString(item)
							}
						}
						buf.WriteString("]")
					default:
						buf.WriteString(fmt.Sprintf("%v", val))
					}
				}
				buf.WriteString("}")
			default:
				buf.WriteString(fmt.Sprintf("%v", val))
			}
		}
		buf.WriteString("}")

		fmt.Fprintf(w.w, "%s", buf.String())

	case string:
		if w.options.QuoteStrings || strings.Contains(v, " ") {
			fmt.Fprintf(w.w, "%q", v)
		} else {
			fmt.Fprintf(w.w, "%s", v)
		}
	default:
		if w.options.QuoteStrings || strings.Contains(fmt.Sprintf("%v", v), " ") {
			fmt.Fprintf(w.w, "%q", v)
		} else {
			fmt.Fprintf(w.w, "%v", v)
		}
	}
	return nil
}
