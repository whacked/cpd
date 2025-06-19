package codec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"gopkg.in/yaml.v3"
)

// CommonPayloadDataSchema is the fixed schema for CommonPayloadData format
const CommonPayloadDataSchema = `_schemas:
  data:
    type: array
    items:
      type: array
      minItems: 3
      maxItems: 3
      items:
        - type: string  # timestamp
          description: "ISO8601 / RFC3339 string"
          examples:
          - "2024-06-12T12:00:00Z"
          - "2022-11-06T23:12:47+08:00"
        - type: array   # tags array
          items:
            type: integer
          uniqueItems: true
          examples:
          - [1, 5, 3299]
          - []
        - type: object  # payload object
`

// CPDToJSONL converts a CPD YAML file to JSONL format
func CPDToJSONL_Legacy(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(splitYAMLDocuments)

	var jsonl strings.Builder
	currentVersion := 0
	currentMeta := orderedmapjson.NewAnyOrderedMap()
	currentSchemas := orderedmapjson.NewAnyOrderedMap()
	currentTags := make(map[int]string)

	for scanner.Scan() {
		doc := scanner.Text()
		if strings.TrimSpace(doc) == "" {
			continue
		}

		var node yaml.Node
		if err := yaml.Unmarshal([]byte(doc), &node); err != nil {
			return "", fmt.Errorf("failed to parse YAML document: %w", err)
		}

		if versionNode := findNodeByKey(&node, "_version"); versionNode != nil {
			currentVersion = parseInt(versionNode.Value)
		}

		if metaNode := findNodeByKey(&node, "_meta"); metaNode != nil {
			metaMap := orderedmapjson.NewAnyOrderedMap()
			if err := yamlutil.ConvertNodeToOrderedMap(metaNode, metaMap); err != nil {
				return "", fmt.Errorf("failed to convert _meta: %w", err)
			}
			RecursiveMergeOrderedMaps(currentMeta, metaMap)
		}

		if schemasNode := findNodeByKey(&node, "_schemas"); schemasNode != nil {
			schemasMap := orderedmapjson.NewAnyOrderedMap()
			if err := yamlutil.ConvertNodeToOrderedMap(schemasNode, schemasMap); err != nil {
				return "", fmt.Errorf("failed to convert _schemas: %w", err)
			}
			RecursiveMergeOrderedMaps(currentSchemas, schemasMap)
		}

		if tagsNode := findNodeByKey(&node, "tags"); tagsNode != nil {
			for i := 0; i < len(tagsNode.Content); i += 2 {
				if i+1 >= len(tagsNode.Content) {
					break
				}
				name := tagsNode.Content[i].Value
				id := parseInt(tagsNode.Content[i+1].Value)
				currentTags[id] = name
			}
		}

		dataNode := findNodeByKey(&node, "data")

		if dataNode == nil {
			continue
		}
		for _, row := range dataNode.Content {
			if row.Kind != yaml.SequenceNode || len(row.Content) != 3 {
				return "", fmt.Errorf("invalid data row: expected 3-element sequence")
			}

			timeStr := row.Content[0].Value
			tagsNode := row.Content[1]
			payloadNode := row.Content[2]

			// Decode tags
			var tagStrings []string
			for _, tagNode := range tagsNode.Content {
				tagID := parseInt(tagNode.Value)
				tagName, ok := currentTags[tagID]
				if !ok {
					return "", fmt.Errorf("unknown tag ID: %d", tagID)
				}
				tagStrings = append(tagStrings, tagName)
			}

			// Decode payload, including scalar-form {key: val, ...}
			var payloadMap *orderedmapjson.AnyOrderedMap
			switch payloadNode.Kind {
			case yaml.MappingNode:
				payloadMap = orderedmapjson.NewAnyOrderedMap()
				if err := yamlutil.ConvertNodeToOrderedMap(payloadNode, payloadMap); err != nil {
					return "", fmt.Errorf("failed to decode payload: %w", err)
				}
			case yaml.ScalarNode:
				// Handle scalar inline map: e.g., "{temp_c:23.4,humidity:45.2}"
				var subNode yaml.Node
				if err := yaml.Unmarshal([]byte(payloadNode.Value), &subNode); err != nil {
					return "", fmt.Errorf("invalid scalar payload: %w", err)
				}
				if subNode.Kind != yaml.MappingNode {
					return "", fmt.Errorf("scalar payload not mapping: %q", payloadNode.Value)
				}
				payloadMap = orderedmapjson.NewAnyOrderedMap()
				if err := yamlutil.ConvertNodeToOrderedMap(&subNode, payloadMap); err != nil {
					return "", fmt.Errorf("failed to decode scalar payload: %w", err)
				}
			default:
				return "", fmt.Errorf("unsupported payload node kind: %v", payloadNode.Kind)
			}

			record := orderedmapjson.NewAnyOrderedMap()
			if currentVersion > 0 {
				record.Set("_version", currentVersion)
			}
			if currentMeta.Len() > 0 {
				flat := ExpandMetaDataFields(currentMeta, ".")
				for el := flat.Front(); el != nil; el = el.Next() {
					record.Set(el.Key, el.Value)
				}
			}

			record.Set("time", timeStr)
			if len(tagStrings) == 0 {
				record.Set("tags", []string{})
			} else {
				record.Set("tags", tagStrings)
			}

			for el := payloadMap.Front(); el != nil; el = el.Next() {
				record.Set(el.Key, el.Value)
			}

			// Manual JSON construction
			var recordBuilder strings.Builder
			recordBuilder.WriteByte('{')
			idx := 0
			for el := record.Front(); el != nil; el = el.Next() {
				if idx > 0 {
					recordBuilder.WriteByte(',')
				}
				idx++
				keyJSON, _ := json.Marshal(el.Key)
				valJSON, err := customMarshalJSON(el.Value)
				if err != nil {
					return "", fmt.Errorf("marshal value error: %w", err)
				}
				recordBuilder.Write(keyJSON)
				recordBuilder.WriteByte(':')
				recordBuilder.Write(valJSON)
			}
			recordBuilder.WriteString("}\n")
			jsonl.WriteString(recordBuilder.String())
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan error: %w", err)
	}
	return jsonl.String(), nil
}
