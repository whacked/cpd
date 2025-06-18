package codec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"gopkg.in/yaml.v3"
)

// CPDRow represents a single row in the CPD format
type CPDRow struct {
	Values []interface{} // Positional values matching _columns
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
				id, err := strconv.Atoi(idStr)
				if err != nil {
					return nil, fmt.Errorf("invalid join table ID in %s: %s", key, idStr)
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
			if len(rowNode.Content) != len(doc.Columns) {
				return nil, fmt.Errorf("data row %d length %d does not match columns length %d",
					i, len(rowNode.Content), len(doc.Columns))
			}

			row := &CPDRow{
				Values: make([]interface{}, len(rowNode.Content)),
			}

			for j, val := range rowNode.Content {
				// TODO: Convert value based on column type
				row.Values[j] = val.Value
			}

			doc.Data[i] = row
		}
	} else {
		return nil, fmt.Errorf("missing required data section")
	}

	return doc, nil
}

// ToJSONL converts a CPDDocument to JSONL format
func (d *CPDDocument) ToJSONL() (string, error) {
	// TODO: Implement
	return "", nil
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

	var jsonl strings.Builder
	currentVersion := 0
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
					id, err := strconv.Atoi(idStr)
					if err != nil {
						return "", fmt.Errorf("invalid join table ID in %s: %s", key, idStr)
					}

					// Check bijection
					if existingName, exists := joinTables[key][id]; exists {
						return "", fmt.Errorf("duplicate ID in join table %s: %d (already maps to %s)", key, id, existingName)
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

		for rowIdx, row := range dataNode.Content {
			if row.Kind != yaml.SequenceNode {
				return "", fmt.Errorf("data row %d must be a sequence", rowIdx)
			}

			// Validate row length
			if len(row.Content) > len(currentColumns) {
				return "", fmt.Errorf("data row %d has %d values but only %d columns defined",
					rowIdx, len(row.Content), len(currentColumns))
			}

			record := orderedmapjson.NewAnyOrderedMap()

			// Add version if present
			if currentVersion > 0 {
				record.Set("_version", currentVersion)
			}

			// Add flattened metadata
			if currentMeta.Len() > 0 {
				flat := ExpandMetaDataFields(currentMeta, ".")
				for el := flat.Front(); el != nil; el = el.Next() {
					record.Set(el.Key, el.Value)
				}
			}

			// Process each column value
			for colIdx, val := range row.Content {
				colName := currentColumns[colIdx]
				joinTable, isJoin := joinTables[colName]

				if isJoin {
					// Join column: must be int, array of int, or null
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
						record.Set(colName, name)
					case yaml.SequenceNode:
						names := make([]string, 0, len(val.Content))
						for _, idNode := range val.Content {
							if idNode.Tag == "!!null" || idNode.Value == "null" {
								return "", fmt.Errorf("invalid join ID in row %d column %s: null in array", rowIdx, colName)
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
						record.Set(colName, names)
					default:
						return "", fmt.Errorf("invalid join ID in row %d column %s: invalid type", rowIdx, colName)
					}
				} else {
					// Not a join column: accept any value
					var v interface{}
					yaml.Unmarshal([]byte(val.Value), &v)
					record.Set(colName, v)
				}
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
					return "", fmt.Errorf("marshal value error in row %d: %w", rowIdx, err)
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
