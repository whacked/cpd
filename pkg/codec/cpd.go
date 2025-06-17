package codec

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/GitRowin/orderedmapjson"
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

// Helper function to find a node by key in a mapping
func findNodeByKey(node *yaml.Node, key string) *yaml.Node {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(node.Content); i += 2 {
		if i+1 >= len(node.Content) {
			break
		}
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}
