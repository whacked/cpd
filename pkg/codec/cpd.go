package codec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"

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

// customMarshalJSON marshals a value to JSON, preserving decimal points for float values
func customMarshalJSON(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case float64:
		// If it's a float that looks like an integer (e.g. 46.0), force decimal point
		if float64(int64(val)) == val {
			return []byte(fmt.Sprintf("%.1f", val)), nil
		}
		return json.Marshal(val)
	default:
		return json.Marshal(v)
	}
}

// CPDToJSONL converts a CPD YAML file to JSONL format
func CPDToJSONL(r io.Reader) (string, error) {
	// Create a scanner to read multiple YAML documents
	scanner := bufio.NewScanner(r)
	scanner.Split(splitYAMLDocuments)

	var jsonl strings.Builder
	var currentVersion int
	currentMeta := orderedmapjson.NewAnyOrderedMap()
	currentSchemas := orderedmapjson.NewAnyOrderedMap()
	currentTags := make(map[int]string)

	for scanner.Scan() {
		doc := scanner.Text()
		if strings.TrimSpace(doc) == "" {
			continue
		}

		// Parse YAML document
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(doc), &node); err != nil {
			return "", fmt.Errorf("failed to parse YAML document: %w", err)
		}

		// Extract version if present
		if versionNode := findNodeByKey(&node, "_version"); versionNode != nil {
			currentVersion = parseInt(versionNode.Value)
		}

		// Extract and merge metadata if present
		if metaNode := findNodeByKey(&node, "_meta"); metaNode != nil {
			metaMap := orderedmapjson.NewAnyOrderedMap()
			if err := yamlutil.ConvertNodeToOrderedMap(metaNode, metaMap); err != nil {
				return "", fmt.Errorf("failed to convert metadata to ordered map: %w", err)
			}
			RecursiveMergeOrderedMaps(currentMeta, metaMap)
		}

		// Extract schemas if present
		if schemasNode := findNodeByKey(&node, "_schemas"); schemasNode != nil {
			schemasMap := orderedmapjson.NewAnyOrderedMap()
			if err := yamlutil.ConvertNodeToOrderedMap(schemasNode, schemasMap); err != nil {
				return "", fmt.Errorf("failed to convert schemas to ordered map: %w", err)
			}
			RecursiveMergeOrderedMaps(currentSchemas, schemasMap)
		}

		// Extract tags lookup table
		if tagsNode := findNodeByKey(&node, "tags"); tagsNode != nil {
			for i := 0; i < len(tagsNode.Content); i += 2 {
				if i+1 >= len(tagsNode.Content) {
					break
				}
				key := tagsNode.Content[i].Value
				val := tagsNode.Content[i+1].Value
				currentTags[parseInt(val)] = key
			}
		}

		// Extract data array
		dataNode := findNodeByKey(&node, "data")
		if dataNode == nil {
			continue // Skip documents without data
		}

		// Process each row in the data array
		for _, row := range dataNode.Content {
			if len(row.Content) != 3 {
				return "", fmt.Errorf("invalid row format: expected 3 elements")
			}

			// Extract components
			time := row.Content[0].Value
			tags := row.Content[1].Content
			payload := row.Content[2]

			// Convert tags from ints to strings
			var tagStrings []string
			for _, tag := range tags {
				tagID := parseInt(tag.Value)
				if tagName, ok := currentTags[tagID]; ok {
					tagStrings = append(tagStrings, tagName)
				} else {
					return "", fmt.Errorf("unknown tag ID: %d", tagID)
				}
			}

			// Create JSONL record with ordered map
			record := orderedmapjson.NewAnyOrderedMap()

			// Add version if present
			if currentVersion > 0 {
				record.Set("_version", currentVersion)
			}

			// Add flattened metadata
			if currentMeta.Len() > 0 {
				flattenedMeta := ExpandMetaDataFields(currentMeta, ".")
				for el := flattenedMeta.Front(); el != nil; el = el.Next() {
					record.Set(el.Key, el.Value)
				}
			}

			record.Set("time", time)
			// Ensure empty slice is marshaled as [] instead of null
			if len(tagStrings) == 0 {
				record.Set("tags", make([]string, 0))
			} else {
				record.Set("tags", tagStrings)
			}

			// Convert payload to ordered map preserving order
			if payload.Kind == yaml.MappingNode {
				payloadMap := orderedmapjson.NewAnyOrderedMap()
				if err := yamlutil.ConvertNodeToOrderedMap(payload, payloadMap); err != nil {
					return "", fmt.Errorf("failed to convert payload to ordered map: %w", err)
				}
				// Add payload fields in order
				for el := payloadMap.Front(); el != nil; el = el.Next() {
					record.Set(el.Key, el.Value)
				}
			}

			// Convert to JSONL
			var recordBuilder strings.Builder
			recordBuilder.WriteString("{")
			keyIndex := 0
			for el := record.Front(); el != nil; el = el.Next() {
				if keyIndex > 0 {
					recordBuilder.WriteString(",")
				}
				keyIndex++
				// Marshal the key
				keyBytes, _ := json.Marshal(el.Key)
				recordBuilder.Write(keyBytes)
				recordBuilder.WriteString(":")
				// Marshal the value with custom handling
				valBytes, err := customMarshalJSON(el.Value)
				if err != nil {
					return "", fmt.Errorf("failed to marshal value: %w", err)
				}
				recordBuilder.Write(valBytes)
			}
			recordBuilder.WriteString("}\n")
			jsonl.WriteString(recordBuilder.String())
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error scanning YAML documents: %w", err)
	}

	return jsonl.String(), nil
}

// splitYAMLDocuments is a bufio.SplitFunc that splits on YAML document boundaries
func splitYAMLDocuments(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Look for document separator
	if i := bytes.Index(data, []byte("\n---")); i >= 0 {
		// Found a document separator
		return i + 1, data[:i], nil
	}

	// If we're at EOF, return the rest as the last document
	if atEOF {
		return len(data), data, nil
	}

	// Request more data
	return 0, nil, nil
}

// JSONLToCPD converts a JSONL file to CPD YAML format
func JSONLToCPD(r io.Reader) (string, error) {
	// First pass: collect all unique tags and track version
	tagSet := make(map[string]struct{})
	var currentVersion int
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// Parse into YAML node to preserve order
		var node yaml.Node
		if err := yaml.Unmarshal(scanner.Bytes(), &node); err != nil {
			return "", fmt.Errorf("failed to parse JSONL: %w", err)
		}

		// Convert to ordered map
		record := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(&node, record); err != nil {
			return "", fmt.Errorf("failed to convert record to ordered map: %w", err)
		}

		// Track version if present
		if version, ok := record.Get("_version"); ok {
			if versionNum, ok := version.(float64); ok {
				currentVersion = int(versionNum)
			}
		}

		// Extract tags
		if tags, ok := record.Get("tags"); ok {
			if tagArray, ok := tags.([]interface{}); ok {
				for _, tag := range tagArray {
					if tagStr, ok := tag.(string); ok {
						tagSet[tagStr] = struct{}{}
					}
				}
			}
		}
	}

	// Create tag lookup table
	tagMap := make(map[string]int)
	tagList := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		tagList = append(tagList, tag)
	}
	sort.Strings(tagList)
	for i, tag := range tagList {
		tagMap[tag] = i + 1 // 1-based IDs
	}

	// Reset reader for second pass
	if seeker, ok := r.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return "", fmt.Errorf("failed to reset reader: %w", err)
		}
	} else {
		return "", fmt.Errorf("reader must be seekable")
	}

	// Second pass: convert records
	var data []interface{}
	scanner = bufio.NewScanner(r)
	for scanner.Scan() {
		// Parse into YAML node to preserve order
		var node yaml.Node
		if err := yaml.Unmarshal(scanner.Bytes(), &node); err != nil {
			return "", fmt.Errorf("failed to parse JSONL: %w", err)
		}

		// Convert to ordered map
		record := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(&node, record); err != nil {
			return "", fmt.Errorf("failed to convert record to ordered map: %w", err)
		}

		// Extract time
		time, ok := record.Get("time")
		if !ok {
			return "", fmt.Errorf("missing time field")
		}
		timeStr, ok := time.(string)
		if !ok {
			return "", fmt.Errorf("invalid time field type")
		}

		// Extract and convert tags
		var tagIDs []int
		if tags, ok := record.Get("tags"); ok {
			if tagArray, ok := tags.([]interface{}); ok {
				for _, tag := range tagArray {
					if tagStr, ok := tag.(string); ok {
						if id, ok := tagMap[tagStr]; ok {
							tagIDs = append(tagIDs, id)
						}
					}
				}
			}
		}
		sort.Ints(tagIDs)

		// Extract payload (all fields except time and tags)
		payload := orderedmapjson.NewAnyOrderedMap()
		for el := record.Front(); el != nil; el = el.Next() {
			if el.Key != "time" && el.Key != "tags" {
				payload.Set(el.Key, el.Value)
			}
		}

		// Add to data array
		data = append(data, []interface{}{timeStr, tagIDs, payload})
	}

	// Create a buffer for the YAML output
	var buf bytes.Buffer

	// Write each section in explicit order
	if currentVersion > 0 {
		buf.WriteString("_version: ")
		buf.WriteString(fmt.Sprintf("%d\n", currentVersion))
	}

	// Add schema section (already has correct indentation)
	buf.WriteString(CommonPayloadDataSchema)

	// Add columns section
	buf.WriteString("_columns:\n")
	buf.WriteString("  - time\n")
	buf.WriteString("  - tags\n")
	buf.WriteString("  - payload\n")

	// Add tags section
	buf.WriteString("tags:\n")
	for tag, id := range tagMap {
		buf.WriteString(fmt.Sprintf("  %s: %d\n", tag, id))
	}

	// Add data section in compact format
	buf.WriteString("data:")
	for _, row := range data {
		// Convert row to compact format
		rowArray := row.([]interface{})
		timeStr := rowArray[0].(string)
		tags := rowArray[1].([]int)
		payload := rowArray[2].(*orderedmapjson.AnyOrderedMap)

		// Format the row as a single line with no extra spaces
		buf.WriteString("\n  - [")
		buf.WriteString(fmt.Sprintf("%q,", timeStr))
		buf.WriteString(fmt.Sprintf("%v,", tags))

		// Convert payload to YAML string with unquoted keys/values where possible
		var payloadBuilder strings.Builder
		payloadBuilder.WriteString("{")
		keyIndex := 0
		for el := payload.Front(); el != nil; el = el.Next() {
			if keyIndex > 0 {
				payloadBuilder.WriteString(",")
			}
			keyIndex++
			// Format key (unquoted if possible)
			key := el.Key
			if needsQuoting(key) {
				keyBytes, _ := json.Marshal(key)
				payloadBuilder.Write(keyBytes)
			} else {
				payloadBuilder.WriteString(key)
			}
			payloadBuilder.WriteString(":")
			// Format value (unquoted if possible)
			payloadBuilder.WriteString(formatYAMLValue(el.Value))
		}
		payloadBuilder.WriteString("}")
		buf.WriteString(payloadBuilder.String())
		buf.WriteString("]")
	}

	return buf.String(), nil
}

// Helper functions

func findNodeByKey(node *yaml.Node, key string) *yaml.Node {
	// If this is a document node, look in its content
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return findNodeByKey(node.Content[0], key)
	}

	// If this is a mapping node, look for the key
	if node.Kind == yaml.MappingNode {
		for i := 0; i < len(node.Content); i += 2 {
			if i+1 >= len(node.Content) {
				break
			}
			if node.Content[i].Value == key {
				return node.Content[i+1]
			}
		}
	}
	return nil
}

func parseInt(s string) int {
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}

// formatYAMLValue formats a value for YAML output, avoiding quotes when possible
func formatYAMLValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		// Only quote strings that need it
		if needsQuoting(val) {
			return fmt.Sprintf("%q", val)
		}
		return val
	case float64:
		// Format floats with decimal point
		if float64(int64(val)) == val {
			return fmt.Sprintf("%.1f", val)
		}
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%v", val)
	case nil:
		return "null"
	default:
		// For any other type, use json.Marshal to ensure proper formatting
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", val)
	}
}

// needsQuoting determines if a string needs to be quoted in YAML
func needsQuoting(s string) bool {
	// Empty string needs quotes
	if s == "" {
		return true
	}

	// Check for special YAML values
	switch s {
	case "null", "true", "false", "yes", "no", "on", "off", "y", "n":
		return true
	}

	// Check for special characters or spaces
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' {
			return true
		}
	}

	return false
}
