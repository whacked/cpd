package yaml

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"gopkg.in/yaml.v3"
)

// DebugPrint enables/disables debug printing
var DebugPrint = false

// Document represents a single YAML document with ordered keys
type Document struct {
	Data *orderedmapjson.AnyOrderedMap
}

// PrintDebug prints debug information about the document if DebugPrint is enabled
func (d *Document) PrintDebug() {
	if !DebugPrint {
		return
	}

	// Print version if present
	if version, ok := d.GetVersion(); ok {
		fmt.Printf("Version: %v\n", version)
	}

	// Print metadata if present
	if meta, ok := d.GetMeta(); ok {
		fmt.Printf("Metadata:\n")
		for el := meta.Front(); el != nil; el = el.Next() {
			fmt.Printf("  %s: %v\n", el.Key, el.Value)
		}
	}

	// Print schemas if present
	if schemas, ok := d.GetSchemas(); ok {
		fmt.Printf("Schemas:\n")
		for el := schemas.Front(); el != nil; el = el.Next() {
			fmt.Printf("  %s: %v\n", el.Key, el.Value)
		}
	}
	// Print data if present
	if records, ok := d.GetData(); ok {
		fmt.Printf("Data:\n")
		for _, record := range records {
			fmt.Printf("  - ")
			switch {
			case record.Object != nil:
				for el := record.Object.Front(); el != nil; el = el.Next() {
					fmt.Printf("%s: %v ", el.Key, el.Value)
				}
			case record.Array != nil:
				for i, v := range record.Array {
					fmt.Printf("%d: %v ", i, v)
				}
			case record.String != "":
				fmt.Printf("%v", record.String)
			}
			fmt.Printf("\n")
		}
	}

	// Print columns if present
	if columns, ok := d.GetColumns(); ok {
		fmt.Printf("Columns: %v\n", columns)
	}
}

// GetVersion returns the document version if present
func (d *Document) GetVersion() (int, bool) {
	if version, ok := d.Data.Get("_version"); ok {
		if v, ok := version.(int); ok {
			return v, true
		}
	}
	return 0, false
}

// GetMeta returns the document metadata if present
func (d *Document) GetMeta() (*orderedmapjson.AnyOrderedMap, bool) {
	if meta, ok := d.Data.Get("_meta"); ok {
		if m, ok := meta.(*orderedmapjson.AnyOrderedMap); ok {
			return m, true
		}
	}
	return nil, false
}

// GetSchemas returns the document schemas if present
func (d *Document) GetSchemas() (*orderedmapjson.AnyOrderedMap, bool) {
	if schemas, ok := d.Data.Get("_schemas"); ok {
		if s, ok := schemas.(*orderedmapjson.AnyOrderedMap); ok {
			return s, true
		}
	}
	return nil, false
}

type WrappedRecord struct {
	Object *orderedmapjson.AnyOrderedMap
	Array  []interface{}
	String string
}

// GetData returns the document data records if present
func (d *Document) GetData() ([]WrappedRecord, bool) {
	if data, ok := d.Data.Get("data"); ok {
		if dataArray, ok := data.([]interface{}); ok {
			var records []WrappedRecord
			for _, record := range dataArray {
				var wrappedRecord WrappedRecord

				switch v := record.(type) {
				case *orderedmapjson.AnyOrderedMap:
					wrappedRecord.Object = v
				case []interface{}:
					wrappedRecord.Array = v
				case string:
					wrappedRecord.String = v
				default:
					// For unhandled types, convert to string
					wrappedRecord.String = fmt.Sprintf("%v", v)
				}

				records = append(records, wrappedRecord)
			}
			return records, true
		}
	}
	return nil, false
}

// GetColumns returns the document columns if present
func (d *Document) GetColumns() ([]string, bool) {
	if columns, ok := d.Data.Get("_columns"); ok {
		// First try direct string array
		if c, ok := columns.([]string); ok {
			return c, true
		}

		// Try interface array and convert to strings
		if c, ok := columns.([]interface{}); ok {
			result := make([]string, len(c))
			for i, v := range c {
				if s, ok := v.(string); ok {
					result[i] = s
				} else {
					// If any element isn't a string, return false
					return nil, false
				}
			}
			return result, true
		}

		// Debug print for unexpected types
		fmt.Printf("Unexpected columns type: %T, value: %+v\n", columns, columns)
	}
	return nil, false
}

func (d *Document) GetTableMappings() map[string]map[string]int {
	result := make(map[string]map[string]int)

	// Iterate through all top-level keys in the document
	for key := range d.Data.Keys() {

		// Skip special keys that start with _ and the data key
		if strings.HasPrefix(key, "_") || key == "data" {
			continue
		}
		// Get the mapping table
		if tableData, ok := d.Data.Get(key); ok {
			if mappings, ok := tableData.(*orderedmapjson.AnyOrderedMap); ok {
				// Initialize the inner map for this table
				result[key] = make(map[string]int)

				// Convert each key-value pair to string->int mapping
				for mapKey := range mappings.Keys() {
					if val, ok := mappings.Get(mapKey); ok {
						if intVal, ok := val.(int); ok {
							result[key][mapKey] = intVal
						}
					}
				}
			}
		}
	}

	return result
}

// Reader reads YAML documents while preserving key order
type Reader struct {
	decoder *yaml.Decoder
}

// NewReader creates a new YAML reader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		decoder: yaml.NewDecoder(r),
	}
}

// Read reads a single YAML document
func (r *Reader) Read() (*Document, error) {
	var node yaml.Node
	if err := r.decoder.Decode(&node); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to decode YAML: %w", err)
	}

	// Convert the YAML node to an ordered map
	data := orderedmapjson.NewAnyOrderedMap()
	if err := convertNodeToOrderedMap(&node, data); err != nil {
		return nil, fmt.Errorf("failed to convert YAML node: %w", err)
	}

	return &Document{Data: data}, nil
}

// ReadAll reads all YAML documents from the reader
func (r *Reader) ReadAll() ([]*Document, error) {
	var docs []*Document
	for {
		doc, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// convertNodeToOrderedMap converts a YAML node to an ordered map
func convertNodeToOrderedMap(node *yaml.Node, result *orderedmapjson.AnyOrderedMap) error {
	if node == nil {
		return nil
	}

	switch node.Kind {
	case yaml.DocumentNode:
		// Document node contains a single mapping node
		if len(node.Content) > 0 {
			return convertNodeToOrderedMap(node.Content[0], result)
		}
		return nil

	case yaml.MappingNode:
		// Process key-value pairs in order
		for i := 0; i < len(node.Content); i += 2 {
			if i+1 >= len(node.Content) {
				break
			}
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]

			// Convert key to string
			key := keyNode.Value

			// Convert value based on its type
			switch valueNode.Kind {
			case yaml.ScalarNode:
				// Handle scalar values based on tag type
				switch valueNode.Tag {
				case "!!int":
					if val, err := strconv.Atoi(valueNode.Value); err == nil {
						result.Set(key, val)
					} else {
						result.Set(key, valueNode.Value)
					}
				case "!!float":
					if val, err := strconv.ParseFloat(valueNode.Value, 64); err == nil {
						result.Set(key, val)
					} else {
						result.Set(key, valueNode.Value)
					}
				case "!!bool":
					if val, err := strconv.ParseBool(valueNode.Value); err == nil {
						result.Set(key, val)
					} else {
						result.Set(key, valueNode.Value)
					}
				case "!!null":
					result.Set(key, nil)
				default:
					result.Set(key, valueNode.Value)
				}

			case yaml.MappingNode:
				// Handle nested maps
				nestedMap := orderedmapjson.NewAnyOrderedMap()
				if err := convertNodeToOrderedMap(valueNode, nestedMap); err != nil {
					return err
				}
				result.Set(key, nestedMap)

			case yaml.SequenceNode:
				// Handle arrays
				var arr []interface{}
				for _, item := range valueNode.Content {
					switch item.Kind {
					case yaml.ScalarNode:
						arr = append(arr, item.Value)
					case yaml.MappingNode:
						nestedMap := orderedmapjson.NewAnyOrderedMap()
						if err := convertNodeToOrderedMap(item, nestedMap); err != nil {
							return err
						}
						arr = append(arr, nestedMap)
					case yaml.SequenceNode:
						var nestedArr []interface{}
						for _, nestedItem := range item.Content {
							if nestedItem.Kind == yaml.ScalarNode {
								nestedArr = append(nestedArr, nestedItem.Value)
							}
						}
						arr = append(arr, nestedArr)
					}
				}
				result.Set(key, arr)
			}
		}
		return nil

	default:
		return nil
	}
}

// String returns the YAML document as a string
func (d *Document) String() string {
	if d.Data == nil {
		return ""
	}

	var builder strings.Builder
	writeOrderedMapToYAML(d.Data, &builder, 0)
	return builder.String()
}

// writeOrderedMapToYAML writes an ordered map to YAML format
func writeOrderedMapToYAML(m *orderedmapjson.AnyOrderedMap, builder *strings.Builder, indent int) {
	indentStr := strings.Repeat("  ", indent)

	for el := m.Front(); el != nil; el = el.Next() {
		key := el.Key
		value := el.Value

		builder.WriteString(indentStr)
		builder.WriteString(key)
		builder.WriteString(": ")

		switch v := value.(type) {
		case *orderedmapjson.AnyOrderedMap:
			builder.WriteString("\n")
			writeOrderedMapToYAML(v, builder, indent+1)
		case []interface{}:
			builder.WriteString("\n")
			for _, item := range v {
				builder.WriteString(indentStr)
				builder.WriteString("  - ")
				switch item := item.(type) {
				case *orderedmapjson.AnyOrderedMap:
					builder.WriteString("\n")
					writeOrderedMapToYAML(item, builder, indent+2)
				default:
					builder.WriteString(fmt.Sprintf("%v\n", item))
				}
			}
		default:
			builder.WriteString(fmt.Sprintf("%v\n", value))
		}
	}
}
