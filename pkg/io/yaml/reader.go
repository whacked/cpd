package yaml

import (
	"fmt"
	"io"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"gopkg.in/yaml.v3"
)

// Document represents a single YAML document with ordered keys
type Document struct {
	Data *orderedmapjson.AnyOrderedMap
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
				// Handle scalar values
				result.Set(key, valueNode.Value)

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
