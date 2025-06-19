package codec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"gopkg.in/yaml.v3"
)

// CPDRow represents a single row in the CPD format
type CPDRow struct {
	Values *orderedmapjson.AnyOrderedMap // Structured values matching _columns
}

// JoinTable represents a bijective mapping of strings to integers
type JoinTable struct {
	NameToID map[string]int
	IDToName map[int]string
}

// CPDDocument represents a complete CPD document
type CPDDocument struct {
	Columns    []string
	JoinTables map[string]*JoinTable
	Data       []*CPDRow
	Meta       *orderedmapjson.AnyOrderedMap
	Version    string
}

// ParseCPD parses a CPD YAML document into a CPDDocument
func ParseCPD(r io.Reader) (*CPDDocument, error) {
	var node yaml.Node
	if err := yaml.NewDecoder(r).Decode(&node); err != nil {
		return nil, err
	}

	doc := &CPDDocument{
		JoinTables: make(map[string]*JoinTable),
		Meta:       orderedmapjson.NewAnyOrderedMap(),
	}

	// Find root mapping node
	if node.Kind != yaml.DocumentNode || len(node.Content) == 0 {
		return nil, fmt.Errorf("invalid YAML document")
	}
	root := node.Content[0]

	// Parse columns
	if columnsNode := findNodeByKey(root, "_columns"); columnsNode != nil {
		if columnsNode.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("_columns must be a sequence")
		}
		doc.Columns = make([]string, len(columnsNode.Content))
		for i, col := range columnsNode.Content {
			doc.Columns[i] = col.Value
		}
	} else {
		return nil, fmt.Errorf("missing required _columns")
	}

	// Parse version
	if versionNode := findNodeByKey(root, "_version"); versionNode != nil {
		doc.Version = versionNode.Value
	}

	// Parse join tables
	for i := 0; i < len(root.Content); i += 2 {
		if i+1 >= len(root.Content) {
			break
		}
		key := root.Content[i].Value
		value := root.Content[i+1]

		// Skip special fields
		if strings.HasPrefix(key, "_") || key == "data" {
			continue
		}

		// Check if this is a join table (must be in _columns)
		isJoinTable := false
		for _, col := range doc.Columns {
			if col == key {
				isJoinTable = true
				break
			}
		}

		if isJoinTable {
			if value.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("join table %s must be a mapping", key)
			}

			joinTable := &JoinTable{
				NameToID: make(map[string]int),
				IDToName: make(map[int]string),
			}

			for j := 0; j < len(value.Content); j += 2 {
				if j+1 >= len(value.Content) {
					break
				}
				name := value.Content[j].Value
				idStr := value.Content[j+1].Value

				// Check for empty or whitespace-only keys
				if strings.TrimSpace(name) == "" {
					return nil, fmt.Errorf("empty join table key in %s", key)
				}

				// Check if the value is a quoted string (should be rejected)
				if value.Content[j+1].Tag == "!!str" {
					return nil, fmt.Errorf("invalid join table ID in %s: %s (must be integer, not string)", key, idStr)
				}
				id, err := strconv.Atoi(idStr)
				if err != nil {
					return nil, fmt.Errorf("invalid join table ID in %s: %s", key, idStr)
				}

				// Check for negative IDs
				if id < 0 {
					return nil, fmt.Errorf("invalid join table ID in %s: %d (must be non-negative)", key, id)
				}

				// Check for too-large IDs
				if id > math.MaxInt32 {
					return nil, fmt.Errorf("invalid join table ID in %s: %d (too large)", key, id)
				}

				// Check bijection
				if _, exists := joinTable.NameToID[name]; exists {
					return nil, fmt.Errorf("duplicate name in join table %s: %s", key, name)
				}
				if _, exists := joinTable.IDToName[id]; exists {
					return nil, fmt.Errorf("duplicate ID in join table %s: %d", key, id)
				}

				joinTable.NameToID[name] = id
				joinTable.IDToName[id] = name
			}

			doc.JoinTables[key] = joinTable
		}
	}

	// Parse data
	if dataNode := findNodeByKey(root, "data"); dataNode != nil {
		if dataNode.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("data must be a sequence")
		}

		doc.Data = make([]*CPDRow, len(dataNode.Content))
		for i, rowNode := range dataNode.Content {
			if rowNode.Kind != yaml.SequenceNode {
				return nil, fmt.Errorf("data row %d must be a sequence", i)
			}
			if len(rowNode.Content) > len(doc.Columns) {
				return nil, fmt.Errorf("data row %d length %d does not match columns length %d",
					i, len(rowNode.Content), len(doc.Columns))
			}

			cpdRow := &CPDRow{
				Values: orderedmapjson.NewAnyOrderedMap(),
			}

			// Only process up to len(rowNode.Content) columns
			for j := 0; j < len(rowNode.Content); j++ {
				colName := doc.Columns[j]
				val := rowNode.Content[j]
				joinTable, isJoin := doc.JoinTables[colName]
				if isJoin && joinTable == nil {
					return nil, fmt.Errorf("join table not found for column %s", colName)
				}

				// Check if this is an array value - if so, we need a join table
				if val.Kind == yaml.SequenceNode {
					if !isJoin || joinTable == nil {
						return nil, fmt.Errorf("join table not found for column %s", colName)
					}
					// Join column with array: must be array of int, or null
					names := make([]string, 0, len(val.Content))
					for _, idNode := range val.Content {
						if idNode.Tag == "!!null" || idNode.Value == "null" {
							// Skip null values in array instead of erroring
							continue
						}
						if idNode.Kind != yaml.ScalarNode {
							return nil, fmt.Errorf("invalid join ID in row %d column %s: non-scalar in array", j, colName)
						}
						id, err := strconv.Atoi(idNode.Value)
						if err != nil {
							return nil, fmt.Errorf("invalid join ID in row %d column %s: %s", j, colName, idNode.Value)
						}
						name, ok := joinTable.IDToName[id]
						if !ok {
							return nil, fmt.Errorf("unknown join ID in row %d column %s: %d", j, colName, id)
						}
						names = append(names, name)
					}
					cpdRow.Values.Set(colName, names)
				} else if isJoin && joinTable != nil {
					// Join column with scalar: must be int or null
					switch val.Kind {
					case yaml.ScalarNode:
						if val.Tag == "!!null" || val.Value == "null" {
							// Null join: skip
							continue
						}
						id, err := strconv.Atoi(val.Value)
						if err != nil {
							return nil, fmt.Errorf("invalid join ID in row %d column %s: %s", j, colName, val.Value)
						}
						name, ok := joinTable.IDToName[id]
						if !ok {
							return nil, fmt.Errorf("unknown join ID in row %d column %s: %d", j, colName, id)
						}
						cpdRow.Values.Set(colName, name)
					default:
						return nil, fmt.Errorf("invalid join ID in row %d column %s: invalid type", j, colName)
					}
				} else if colName == "payload" {
					// Handle payload specially
					switch val.Kind {
					case yaml.MappingNode:
						payloadMap := orderedmapjson.NewAnyOrderedMap()
						if err := yamlutil.ConvertNodeToOrderedMap(val, payloadMap); err != nil {
							return nil, fmt.Errorf("failed to decode payload in row %d: %w", j, err)
						}
						// Flatten payload fields into row
						for el := payloadMap.Front(); el != nil; el = el.Next() {
							cpdRow.Values.Set(el.Key, el.Value)
						}
					case yaml.ScalarNode:
						if val.Tag == "!!null" || val.Value == "null" {
							continue
						}

						tryVals := []string{val.Value}
						if len(val.Value) >= 2 && ((val.Value[0] == '"' && val.Value[len(val.Value)-1] == '"') || (val.Value[0] == '\'' && val.Value[len(val.Value)-1] == '\'')) {
							tryVals = append(tryVals, val.Value[1:len(val.Value)-1])
						}
						flattened := false
						for _, tryVal := range tryVals {
							trimmed := strings.TrimSpace(tryVal)
							if len(trimmed) > 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' {
								// Try parsing as a YAML document
								var m map[string]interface{}
								if err := yaml.Unmarshal([]byte(trimmed), &m); err == nil {
									for k, v := range m {
										cpdRow.Values.Set(k, v)
									}
									flattened = true
									break
								}
							}
							if flattened {
								break
							}
						}
						if !flattened {
							cpdRow.Values.Set(colName, val.Value)
						}
					default:
						return nil, fmt.Errorf("unsupported payload node kind in row %d: %v", j, val.Kind)
					}
				} else {
					// Regular scalar column
					cpdRow.Values.Set(colName, val.Value)
				}
			}

			doc.Data[i] = cpdRow
		}
	} else {
		return nil, fmt.Errorf("missing required data section")
	}

	return doc, nil
}

// ToJSONL converts a CPDDocument to JSONL format
func (d *CPDDocument) ToJSONL() (string, error) {
	var jsonl strings.Builder

	for _, row := range d.Data {
		// Manual JSON construction
		var recordBuilder strings.Builder
		recordBuilder.WriteByte('{')
		idx := 0

		// Add version if present
		if d.Version != "" {
			keyJSON, _ := json.Marshal("_version")
			// Marshal as number if possible
			if intVal, err := strconv.Atoi(d.Version); err == nil {
				valJSON, _ := json.Marshal(intVal)
				recordBuilder.Write(keyJSON)
				recordBuilder.WriteByte(':')
				recordBuilder.Write(valJSON)
				idx++
			} else {
				valJSON, _ := json.Marshal(d.Version)
				recordBuilder.Write(keyJSON)
				recordBuilder.WriteByte(':')
				recordBuilder.Write(valJSON)
				idx++
			}
		}

		// Add metadata if present
		if d.Meta.Len() > 0 {
			for el := d.Meta.Front(); el != nil; el = el.Next() {
				if idx > 0 {
					recordBuilder.WriteByte(',')
				}
				keyJSON, _ := json.Marshal(el.Key)
				valJSON, err := customMarshalJSON(el.Value)
				if err != nil {
					return "", fmt.Errorf("marshal metadata value error: %w", err)
				}
				recordBuilder.Write(keyJSON)
				recordBuilder.WriteByte(':')
				recordBuilder.Write(valJSON)
				idx++
			}
		}

		// Add row values
		for el := row.Values.Front(); el != nil; el = el.Next() {
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

	return jsonl.String(), nil
}

// Validate checks if the CPDDocument is valid according to spec6
func (d *CPDDocument) Validate() error {
	// TODO: Implement
	return nil
}

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

// CPDToJSONL converts a CPD YAML file to JSONL format
func CPDToJSONL(r io.Reader) (string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(splitYAMLDocuments)

	var documents []*CPDDocument
	currentVersion := 0
	hasVersion := false
	currentMeta := orderedmapjson.NewAnyOrderedMap()
	currentColumns := []string{}
	joinTables := make(map[string]map[int]string) // table name -> id -> name

	for scanner.Scan() {
		doc := scanner.Text()
		if strings.TrimSpace(doc) == "" {
			continue
		}

		var node yaml.Node
		if err := yaml.Unmarshal([]byte(doc), &node); err != nil {
			return "", fmt.Errorf("failed to parse YAML document: %w", err)
		}

		// Parse version
		if versionNode := findNodeByKey(&node, "_version"); versionNode != nil {
			hasVersion = true
			currentVersion = parseInt(versionNode.Value)
		}

		// Parse meta
		if metaNode := findNodeByKey(&node, "_meta"); metaNode != nil {
			metaMap := orderedmapjson.NewAnyOrderedMap()
			if err := yamlutil.ConvertNodeToOrderedMap(metaNode, metaMap); err != nil {
				return "", fmt.Errorf("failed to convert _meta: %w", err)
			}
			RecursiveMergeOrderedMaps(currentMeta, metaMap)
		}

		// Parse columns (persist if not present)
		if columnsNode := findNodeByKey(&node, "_columns"); columnsNode != nil {
			if columnsNode.Kind != yaml.SequenceNode {
				return "", fmt.Errorf("_columns must be a sequence")
			}
			currentColumns = make([]string, len(columnsNode.Content))
			for i, col := range columnsNode.Content {
				currentColumns[i] = col.Value
			}
		} else if len(currentColumns) == 0 {
			return "", fmt.Errorf("missing required _columns")
		}

		// Parse join tables (persist and extend if not present)
		for i := 0; i < len(node.Content[0].Content); i += 2 {
			if i+1 >= len(node.Content[0].Content) {
				break
			}
			key := node.Content[0].Content[i].Value
			value := node.Content[0].Content[i+1]

			// Skip special fields and data
			if strings.HasPrefix(key, "_") || key == "data" {
				continue
			}

			// Check if this is a join table (must be in _columns)
			isJoinTable := false
			for _, col := range currentColumns {
				if col == key {
					isJoinTable = true
					break
				}
			}

			if isJoinTable {
				if value.Kind != yaml.MappingNode {
					return "", fmt.Errorf("join table %s must be a mapping", key)
				}

				// Initialize join table if not exists
				if _, exists := joinTables[key]; !exists {
					joinTables[key] = make(map[int]string)
				}

				// Add entries to join table
				for j := 0; j < len(value.Content); j += 2 {
					if j+1 >= len(value.Content) {
						break
					}
					name := value.Content[j].Value
					idStr := value.Content[j+1].Value

					// Check for empty or whitespace-only keys
					if strings.TrimSpace(name) == "" {
						return "", fmt.Errorf("empty join table key in %s", key)
					}

					// Check if the value is a quoted string (should be rejected)
					if value.Content[j+1].Tag == "!!str" {
						return "", fmt.Errorf("invalid join table ID in %s: %s (must be integer, not string)", key, idStr)
					}
					id, err := strconv.Atoi(idStr)
					if err != nil {
						return "", fmt.Errorf("invalid join table ID in %s: %s", key, idStr)
					}

					// Check for negative IDs
					if id < 0 {
						return "", fmt.Errorf("invalid join table ID in %s: %d (must be non-negative)", key, id)
					}

					// Check for too-large IDs
					if id > math.MaxInt32 {
						return "", fmt.Errorf("invalid join table ID in %s: %d (too large)", key, id)
					}

					// Check bijection - both ID and name must be unique
					if existingName, exists := joinTables[key][id]; exists {
						return "", fmt.Errorf("duplicate ID in join table %s: %d (already maps to %s)", key, id, existingName)
					}
					// Check for duplicate names across all IDs
					for existingID, existingName := range joinTables[key] {
						if existingName == name {
							return "", fmt.Errorf("duplicate key in join table %s: %s (already maps to %d)", key, name, existingID)
						}
					}
					joinTables[key][id] = name
				}
			}
		}

		// Parse data
		dataNode := findNodeByKey(&node, "data")
		if dataNode == nil {
			continue
		}

		if dataNode.Kind != yaml.SequenceNode {
			return "", fmt.Errorf("data must be a sequence")
		}

		// Create CPDDocument for this YAML document
		cpdDoc := &CPDDocument{
			Columns:    make([]string, len(currentColumns)),
			JoinTables: make(map[string]*JoinTable),
			Data:       make([]*CPDRow, 0, len(dataNode.Content)),
			Meta:       orderedmapjson.NewAnyOrderedMap(),
			Version:    "",
		}
		if hasVersion {
			cpdDoc.Version = fmt.Sprintf("%d", currentVersion)
		}
		copy(cpdDoc.Columns, currentColumns)

		// Convert join tables to CPDDocument format
		for tableName, idToName := range joinTables {
			joinTable := &JoinTable{
				NameToID: make(map[string]int),
				IDToName: make(map[int]string),
			}
			for id, name := range idToName {
				joinTable.NameToID[name] = id
				joinTable.IDToName[id] = name
			}
			cpdDoc.JoinTables[tableName] = joinTable
		}

		// Copy metadata
		if currentMeta.Len() > 0 {
			flattenMeta("_meta", currentMeta, cpdDoc.Meta)
		}

		// Parse rows
		for rowIdx, row := range dataNode.Content {
			if row.Kind != yaml.SequenceNode {
				return "", fmt.Errorf("data row %d must be a sequence", rowIdx)
			}

			// Validate row length
			if len(row.Content) > len(currentColumns) {
				return "", fmt.Errorf("data row %d has %d values but only %d columns defined",
					rowIdx, len(row.Content), len(currentColumns))
			}

			cpdRow := &CPDRow{
				Values: orderedmapjson.NewAnyOrderedMap(),
			}

			// Only process up to len(row.Content) columns
			for colIdx := 0; colIdx < len(row.Content); colIdx++ {
				colName := currentColumns[colIdx]
				val := row.Content[colIdx]
				joinTable, isJoin := joinTables[colName]
				if isJoin && joinTable == nil {
					return "", fmt.Errorf("join table not found for column %s", colName)
				}

				// Check if this is an array value - if so, we need a join table
				if val.Kind == yaml.SequenceNode {
					if !isJoin || joinTable == nil {
						return "", fmt.Errorf("join table not found for column %s", colName)
					}
					// Join column with array: must be array of int, or null
					names := make([]string, 0, len(val.Content))
					for _, idNode := range val.Content {
						if idNode.Tag == "!!null" || idNode.Value == "null" {
							// Skip null values in array instead of erroring
							continue
						}
						if idNode.Kind != yaml.ScalarNode {
							return "", fmt.Errorf("invalid join ID in row %d column %s: non-scalar in array", rowIdx, colName)
						}
						id, err := strconv.Atoi(idNode.Value)
						if err != nil {
							return "", fmt.Errorf("invalid join ID in row %d column %s: %s", rowIdx, colName, idNode.Value)
						}
						name, ok := joinTable[id]
						if !ok {
							return "", fmt.Errorf("unknown join ID in row %d column %s: %d", rowIdx, colName, id)
						}
						names = append(names, name)
					}
					cpdRow.Values.Set(colName, names)
				} else if isJoin && joinTable != nil {
					// Join column with scalar: must be int or null
					switch val.Kind {
					case yaml.ScalarNode:
						if val.Tag == "!!null" || val.Value == "null" {
							// Null join: skip
							continue
						}
						id, err := strconv.Atoi(val.Value)
						if err != nil {
							return "", fmt.Errorf("invalid join ID in row %d column %s: %s", rowIdx, colName, val.Value)
						}
						name, ok := joinTable[id]
						if !ok {
							return "", fmt.Errorf("unknown join ID in row %d column %s: %d", rowIdx, colName, id)
						}
						cpdRow.Values.Set(colName, name)
					default:
						return "", fmt.Errorf("invalid join ID in row %d column %s: invalid type", rowIdx, colName)
					}
				} else if colName == "payload" {
					// Handle payload specially
					switch val.Kind {
					case yaml.MappingNode:
						payloadMap := orderedmapjson.NewAnyOrderedMap()
						if err := yamlutil.ConvertNodeToOrderedMap(val, payloadMap); err != nil {
							return "", fmt.Errorf("failed to decode payload in row %d: %w", rowIdx, err)
						}
						// Flatten payload fields into row
						for el := payloadMap.Front(); el != nil; el = el.Next() {
							cpdRow.Values.Set(el.Key, el.Value)
						}
					case yaml.ScalarNode:
						if val.Tag == "!!null" || val.Value == "null" {
							continue
						}

						tryVals := []string{val.Value}
						if len(val.Value) >= 2 && ((val.Value[0] == '"' && val.Value[len(val.Value)-1] == '"') || (val.Value[0] == '\'' && val.Value[len(val.Value)-1] == '\'')) {
							tryVals = append(tryVals, val.Value[1:len(val.Value)-1])
						}
						flattened := false
						for _, tryVal := range tryVals {
							trimmed := strings.TrimSpace(tryVal)
							if len(trimmed) > 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' {
								// Try parsing as a YAML document
								var m map[string]interface{}
								if err := yaml.Unmarshal([]byte(trimmed), &m); err == nil {
									for k, v := range m {
										cpdRow.Values.Set(k, v)
									}
									flattened = true
									break
								}
							}
							if flattened {
								break
							}
						}
						if !flattened {
							cpdRow.Values.Set(colName, val.Value)
						}
					default:
						return "", fmt.Errorf("unsupported payload node kind in row %d: %v", rowIdx, val.Kind)
					}
				} else {
					// Regular scalar column
					cpdRow.Values.Set(colName, val.Value)
				}
			}

			cpdDoc.Data = append(cpdDoc.Data, cpdRow)
		}

		documents = append(documents, cpdDoc)
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan error: %w", err)
	}

	// Emit JSONL from all parsed documents
	var jsonl strings.Builder
	for _, doc := range documents {
		jsonlStr, err := doc.ToJSONL()
		if err != nil {
			return "", fmt.Errorf("failed to convert document to JSONL: %w", err)
		}
		jsonl.WriteString(jsonlStr)
	}

	return jsonl.String(), nil
}

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

// Helper functions
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

// JSONLToCPD converts a JSONL file to CPD YAML format
func JSONLToCPD(r io.Reader) (string, error) {
	// First pass: collect all unique tags and track version
	orderedTagCount := orderedmapjson.NewAnyOrderedMap()
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
						// Initialize count to 0 if not present
						if _, exists := orderedTagCount.Get(tagStr); !exists {
							orderedTagCount.Set(tagStr, 0)
						}
						// Increment count
						count, _ := orderedTagCount.Get(tagStr)
						orderedTagCount.Set(tagStr, count.(int)+1)
					}
				}
			}
		}
	}

	// Create tag lookup table
	tagMap := make(map[string]int)
	for el := orderedTagCount.Front(); el != nil; el = el.Next() {
		tagMap[el.Key] = len(tagMap) + 1 // 1-based IDs
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
	for el := orderedTagCount.Front(); el != nil; el = el.Next() {
		buf.WriteString(fmt.Sprintf("  %s: %d\n", el.Key, tagMap[el.Key]))
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
		// Format tags array with proper commas
		buf.WriteString("[")
		for i, tag := range tags {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("%d", tag))
		}
		buf.WriteString("],")

		// Convert payload to YAML string with unquoted keys/values where possible
		var payloadBuilder strings.Builder
		payloadBuilder.WriteString("{")
		keyIndex := 0
		for el := payload.Front(); el != nil; el = el.Next() {
			if keyIndex > 0 {
				payloadBuilder.WriteString(",")
			}
			keyIndex++
			// Always quote keys
			keyBytes, _ := json.Marshal(el.Key)
			payloadBuilder.Write(keyBytes)
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

// Add this helper at the end of the file:
func flattenMeta(prefix string, v interface{}, out *orderedmapjson.AnyOrderedMap) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, subv := range val {
			flattenMeta(prefix+"."+k, subv, out)
		}
	case *orderedmapjson.AnyOrderedMap:
		for el := val.Front(); el != nil; el = el.Next() {
			flattenMeta(prefix+"."+el.Key, el.Value, out)
		}
	default:
		out.Set(prefix, val)
	}
}
