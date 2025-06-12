package yamlutil

import (
	"strconv"

	"github.com/GitRowin/orderedmapjson"
	"gopkg.in/yaml.v3"
)

// ConvertNodeToOrderedMap converts a YAML node to an ordered map
func ConvertNodeToOrderedMap(node *yaml.Node, result *orderedmapjson.AnyOrderedMap) error {
	if node == nil {
		return nil
	}

	switch node.Kind {
	case yaml.DocumentNode:
		// Document node contains a single mapping node
		if len(node.Content) > 0 {
			return ConvertNodeToOrderedMap(node.Content[0], result)
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
				if err := ConvertNodeToOrderedMap(valueNode, nestedMap); err != nil {
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
						if err := ConvertNodeToOrderedMap(item, nestedMap); err != nil {
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
