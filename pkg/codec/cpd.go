package codec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"github.com/whacked/yamdb/pkg/relational"
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
								var node yaml.Node
								if err := yaml.Unmarshal([]byte(trimmed), &node); err == nil {
									orderedObj := orderedmapjson.NewAnyOrderedMap()
									if err := yamlutil.ConvertNodeToOrderedMap(&node, orderedObj); err == nil {
										for el := orderedObj.Front(); el != nil; el = el.Next() {
											cpdRow.Values.Set(el.Key, el.Value)
										}
										flattened = true
										break
									}
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

		// Add flattened metadata if present
		if d.Meta != nil && d.Meta.Len() > 0 {
			flatMeta := orderedmapjson.NewAnyOrderedMap()
			flattenMeta("_meta", d.Meta, flatMeta)
			for el := flatMeta.Front(); el != nil; el = el.Next() {
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

			/* UH OH... what to do?
			// Skip fields with nil or "null" value
			if el.Value == nil {
				continue
			}
			if s, ok := el.Value.(string); ok && s == "null" {
				continue
			}
			*/
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

func (d *CPDDocument) PrettyPrint() {
	fmt.Println("<CPDDocument>")
	fmt.Printf("  Version: %s\n", d.Version)
	fmt.Printf("  Meta: %+v\n", d.Meta)
	fmt.Printf("  Columns: %+v\n", d.Columns)
	fmt.Printf("  JoinTables: %+v\n", d.JoinTables)
	fmt.Println("</CPDDocument>")
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
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	var documents []*CPDDocument
	currentVersion := ""
	currentMeta := orderedmapjson.NewAnyOrderedMap()
	var currentColumns []string
	currentJoinTables := make(map[string]*JoinTable)

	for {
		doc, err := parseNextDocument(scanner, currentColumns, currentJoinTables, currentMeta, currentVersion)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("failed to parse document: %w", err)
		}

		// Update state for next document
		if len(doc.Columns) > 0 {
			currentColumns = make([]string, len(doc.Columns))
			copy(currentColumns, doc.Columns)
		}
		// Update metadata state for the next document
		if doc.Version != "" {
			currentVersion = doc.Version
		}
		for k, v := range doc.JoinTables {
			if v == nil {
				continue // Skip nil join tables
			}
			if _, ok := currentJoinTables[k]; !ok {
				currentJoinTables[k] = &JoinTable{
					NameToID: make(map[string]int),
					IDToName: make(map[int]string),
				}
			}
			// Ensure the join table is properly initialized
			if currentJoinTables[k] == nil {
				currentJoinTables[k] = &JoinTable{
					NameToID: make(map[string]int),
					IDToName: make(map[int]string),
				}
			}
			for name, id := range v.NameToID {
				currentJoinTables[k].NameToID[name] = id
				currentJoinTables[k].IDToName[id] = name
			}
		}

		documents = append(documents, doc)

		// Update currentMeta for the next document after processing this document
		if doc.Meta.Len() > 0 {
			// Create a new map for the next document with merged metadata
			currentMeta = orderedmapjson.NewAnyOrderedMap()
			RecursiveMergeOrderedMaps(currentMeta, doc.Meta)
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan error: %w", err)
	}

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

// parseNextDocument parses a single CPD document from the scanner, propagating state
func parseNextDocument(scanner *bufio.Scanner, prevColumns []string, prevJoinTables map[string]*JoinTable, prevMeta *orderedmapjson.AnyOrderedMap, prevVersion string) (*CPDDocument, error) {
	var headSection strings.Builder
	var inDataSection bool
	var dataLines []string
	var foundDocument bool

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "---" {
			if foundDocument {
				break
			}
			foundDocument = true
			continue
		}
		if !foundDocument && strings.TrimSpace(line) != "" {
			foundDocument = true
		}
		if !foundDocument {
			continue
		}
		if !inDataSection {
			if strings.TrimSpace(line) == "data:" {
				inDataSection = true
				continue
			}
			headSection.WriteString(line)
			headSection.WriteString("\n")
		} else {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if strings.HasPrefix(strings.TrimSpace(line), "---") {
				break
			}
			dataLines = append(dataLines, line)
		}
	}
	if !foundDocument {
		return nil, io.EOF
	}
	// Parse head section
	headYAML := headSection.String()
	var node yaml.Node
	if strings.TrimSpace(headYAML) != "" {
		if err := yaml.Unmarshal([]byte(headYAML), &node); err != nil {
			return nil, fmt.Errorf("failed to parse head section YAML: %w", err)
		}
	}
	doc := &CPDDocument{
		Columns:    nil,
		JoinTables: make(map[string]*JoinTable),
		Data:       []*CPDRow{},
		Meta:       orderedmapjson.NewAnyOrderedMap(),
		Version:    "",
	}
	// Version
	if versionNode := findNodeByKey(&node, "_version"); versionNode != nil {
		doc.Version = versionNode.Value
	} else {
		doc.Version = prevVersion
	}
	// Meta
	if metaNode := findNodeByKey(&node, "_meta"); metaNode != nil {
		metaMap := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(metaNode, metaMap); err != nil {
			return nil, fmt.Errorf("failed to convert _meta: %w", err)
		}
		// For the current document, use merged metadata (previous + new)
		mergedMeta := orderedmapjson.NewAnyOrderedMap()
		if prevMeta != nil && prevMeta.Len() > 0 {
			// Create a deep copy of prevMeta to avoid modifying the original
			prevMetaCopy := deepCopyOrderedMap(prevMeta)
			RecursiveMergeOrderedMaps(mergedMeta, prevMetaCopy)
		}
		RecursiveMergeOrderedMaps(mergedMeta, metaMap)
		doc.Meta = mergedMeta // Use merged metadata for current document
	} else if prevMeta != nil && prevMeta.Len() > 0 {
		// Create a deep copy of prevMeta to avoid modifying the original
		prevMetaCopy := deepCopyOrderedMap(prevMeta)
		doc.Meta = prevMetaCopy
	}
	// Columns
	if columnsNode := findNodeByKey(&node, "_columns"); columnsNode != nil {
		if columnsNode.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("_columns must be a sequence")
		}
		doc.Columns = make([]string, len(columnsNode.Content))
		for i, col := range columnsNode.Content {
			doc.Columns[i] = col.Value
		}
	} else if len(prevColumns) > 0 {
		doc.Columns = make([]string, len(prevColumns))
		copy(doc.Columns, prevColumns)
	} else {
		return nil, fmt.Errorf("missing required _columns")
	}
	// Join tables
	for i := 0; node.Content != nil && i < len(node.Content[0].Content); i += 2 {
		if i+1 >= len(node.Content[0].Content) {
			break
		}
		key := node.Content[0].Content[i].Value
		value := node.Content[0].Content[i+1]
		if strings.HasPrefix(key, "_") {
			continue
		}
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
				if strings.TrimSpace(name) == "" {
					return nil, fmt.Errorf("empty join table key in %s", key)
				}
				if value.Content[j+1].Tag == "!!str" {
					return nil, fmt.Errorf("invalid join table ID in %s: %s (must be integer, not string)", key, idStr)
				}
				id, err := strconv.Atoi(idStr)
				if err != nil {
					return nil, fmt.Errorf("invalid join table ID in %s: %s", key, idStr)
				}
				if id < 0 {
					return nil, fmt.Errorf("invalid join table ID in %s: %d (must be non-negative)", key, id)
				}
				if id > math.MaxInt32 {
					return nil, fmt.Errorf("invalid join table ID in %s: %d (too large)", key, id)
				}
				if existingName, exists := joinTable.IDToName[id]; exists {
					return nil, fmt.Errorf("duplicate ID in join table %s: %d (already maps to %s)", key, id, existingName)
				}
				for existingID, existingName := range joinTable.IDToName {
					if existingName == name {
						return nil, fmt.Errorf("duplicate key in join table %s: %s (already maps to %d)", key, name, existingID)
					}
				}
				joinTable.NameToID[name] = id
				joinTable.IDToName[id] = name
			}
			doc.JoinTables[key] = joinTable
		}
	}
	// Propagate previous join tables if not present
	for k, v := range prevJoinTables {
		if v == nil {
			continue // Skip nil join tables
		}
		if _, ok := doc.JoinTables[k]; !ok {
			// Create a new join table and copy the data
			doc.JoinTables[k] = &JoinTable{
				NameToID: make(map[string]int),
				IDToName: make(map[int]string),
			}
			for name, id := range v.NameToID {
				doc.JoinTables[k].NameToID[name] = id
				doc.JoinTables[k].IDToName[id] = name
			}
		} else {
			// Check for conflicts when merging with existing join table
			for name, id := range v.NameToID {
				// Check for duplicate ID
				if existingName, exists := doc.JoinTables[k].IDToName[id]; exists {
					return nil, fmt.Errorf("duplicate ID in join table %s: %d (already maps to %s)", k, id, existingName)
				}
				// Check for duplicate key
				if existingID, exists := doc.JoinTables[k].NameToID[name]; exists {
					return nil, fmt.Errorf("duplicate key in join table %s: %s (already maps to %d)", k, name, existingID)
				}
				// Add the entry
				doc.JoinTables[k].NameToID[name] = id
				doc.JoinTables[k].IDToName[id] = name
			}
		}
	}
	// Parse data section
	for lineIdx, line := range dataLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		row, err := parseDataRow(line, doc.Columns, doc.JoinTables, lineIdx)
		if err != nil {
			return nil, fmt.Errorf("error parsing data row %d: %w", lineIdx, err)
		}
		doc.Data = append(doc.Data, row)
	}
	return doc, nil
}

// parseDataRow parses a single data row line, flattening payload fields
func parseDataRow(line string, columns []string, joinTables map[string]*JoinTable, rowIndex int) (*CPDRow, error) {
	line = strings.TrimPrefix(line, "- ")
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
		return nil, fmt.Errorf("data row must be an array")
	}
	inner := strings.TrimPrefix(strings.TrimSuffix(line, "]"), "[")
	inner = strings.TrimSpace(inner)
	var values []string
	var current strings.Builder
	var braceCount int
	var bracketCount int
	var inString bool
	var escapeNext bool
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if escapeNext {
			current.WriteByte(ch)
			escapeNext = false
			continue
		}
		if ch == '\\' {
			escapeNext = true
			current.WriteByte(ch)
			continue
		}
		if ch == '"' && !escapeNext {
			inString = !inString
			current.WriteByte(ch)
			continue
		}
		if !inString {
			if ch == '{' {
				braceCount++
			} else if ch == '}' {
				braceCount--
			} else if ch == '[' {
				bracketCount++
			} else if ch == ']' {
				bracketCount--
			} else if ch == ',' && braceCount == 0 && bracketCount == 0 {
				values = append(values, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
		}
		current.WriteByte(ch)
	}
	if current.Len() > 0 {
		values = append(values, strings.TrimSpace(current.String()))
	}
	if len(values) > len(columns) {
		return nil, fmt.Errorf("data row %d has %d values but only %d columns defined", rowIndex, len(values), len(columns))
	}
	row := &CPDRow{
		Values: orderedmapjson.NewAnyOrderedMap(),
	}
	for colIdx := 0; colIdx < len(values); colIdx++ {
		colName := columns[colIdx]
		valStr := values[colIdx]
		joinTable, isJoin := joinTables[colName]
		val, err := parseValue(valStr, joinTable, isJoin)
		if err != nil {
			return nil, fmt.Errorf("error parsing column %s: %w", colName, err)
		}
		if colName == "payload" && val != nil {
			// Flatten payload fields
			if om, ok := val.(*orderedmapjson.AnyOrderedMap); ok {
				for el := om.Front(); el != nil; el = el.Next() {
					row.Values.Set(el.Key, el.Value)
				}
			} else {
				row.Values.Set(colName, val)
			}
		} else if colName == "time" && val != nil {
			// Remove extra quotes from time
			if s, ok := val.(string); ok && len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
				row.Values.Set(colName, s[1:len(s)-1])
			} else {
				row.Values.Set(colName, val)
			}
		} else if val != nil {
			row.Values.Set(colName, val)
		}
	}
	return row, nil
}

// parseValue parses a single value from a data row
func parseValue(valStr string, joinTable *JoinTable, isJoin bool) (interface{}, error) {
	valStr = strings.TrimSpace(valStr)

	// Handle null
	if valStr == "null" {
		return nil, nil
	}

	// Handle arrays (for join tables)
	if strings.HasPrefix(valStr, "[") && strings.HasSuffix(valStr, "]") {
		if !isJoin {
			return nil, fmt.Errorf("join table not found for column")
		}
		if joinTable == nil {
			return nil, fmt.Errorf("join table not found for column")
		}

		inner := strings.TrimPrefix(strings.TrimSuffix(valStr, "]"), "[")
		inner = strings.TrimSpace(inner)

		if inner == "" {
			return []string{}, nil // Empty array
		}

		// Split by comma and parse IDs
		parts := strings.Split(inner, ",")
		var result []string
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" || part == "null" {
				continue
			}
			// Accept quoted empty string as an empty string value
			if part == "\"\"" {
				result = append(result, "")
				continue
			}
			// Reject nested arrays or objects
			if strings.HasPrefix(part, "[") || strings.HasPrefix(part, "{") {
				return nil, fmt.Errorf("invalid join ID: %s", part)
			}
			// Only allow integer IDs
			if _, err := strconv.Atoi(part); err != nil {
				return nil, fmt.Errorf("invalid join ID: %s", part)
			}
			id, _ := strconv.Atoi(part)
			if name, exists := joinTable.IDToName[id]; exists {
				result = append(result, name)
			} else {
				return nil, fmt.Errorf("unknown join ID: %d", id)
			}
		}
		return result, nil
	}

	// Handle objects (payload)
	if strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}") {
		if isJoin {
			return nil, fmt.Errorf("invalid join ID: %s", valStr)
		}
		// Parse as YAML node to preserve order
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(valStr), &node); err != nil {
			return nil, fmt.Errorf("failed to parse object: %w", err)
		}

		// Convert to ordered map
		orderedObj := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(&node, orderedObj); err != nil {
			return nil, fmt.Errorf("failed to convert object to ordered map: %w", err)
		}
		return orderedObj, nil
	}

	// Handle quoted object strings (scalar payload)
	if strings.HasPrefix(valStr, "\"{") && strings.HasSuffix(valStr, "}\"") {
		if isJoin {
			return nil, fmt.Errorf("invalid join ID: %s", valStr)
		}
		// Remove the outer quotes and parse as object
		unquoted := valStr[1 : len(valStr)-1]
		// Parse as YAML node to preserve order
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(unquoted), &node); err != nil {
			return nil, fmt.Errorf("failed to parse quoted object: %w", err)
		}

		// Convert to ordered map
		orderedObj := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(&node, orderedObj); err != nil {
			return nil, fmt.Errorf("failed to convert quoted object to ordered map: %w", err)
		}
		return orderedObj, nil
	}

	// Handle join table single values
	if isJoin {
		if joinTable == nil {
			return nil, fmt.Errorf("join table is nil for join field")
		}
		// Reject booleans
		if valStr == "true" || valStr == "false" {
			return nil, fmt.Errorf("invalid join ID: %s", valStr)
		}
		// Reject floats
		if strings.Contains(valStr, ".") {
			if _, err := strconv.ParseFloat(valStr, 64); err == nil {
				return nil, fmt.Errorf("invalid join ID: %s", valStr)
			}
		}
		// Reject anything that isn't a valid integer
		id, err := strconv.Atoi(valStr)
		if err != nil {
			return nil, fmt.Errorf("invalid join ID: %s", valStr)
		}

		// Look up name
		if name, exists := joinTable.IDToName[id]; exists {
			return name, nil
		} else {
			return nil, fmt.Errorf("unknown join ID: %d", id)
		}
	}

	// Handle regular scalar values
	// Try to parse as number first
	if f, err := strconv.ParseFloat(valStr, 64); err == nil {
		return f, nil
	}

	// Try to parse as boolean
	switch valStr {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}

	// Default to string
	return valStr, nil
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
		return fmt.Sprintf("%q", val)
	case float64:
		if float64(int64(val)) == val {
			return fmt.Sprintf("%.1f", val)
		}
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%v", val)
	case nil:
		return "null"
	default:
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

// isNumberLike returns true if the string starts with a digit and is followed by optional letters (e.g., 12g, 585g, 21s)
func isNumberLike(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	seenDigit := false
	for _, r := range s[start:] {
		if unicode.IsDigit(r) {
			seenDigit = true
			continue
		}
		if unicode.IsLetter(r) {
			if !seenDigit {
				return false
			}
			continue
		}
		if r == '.' {
			if !seenDigit {
				return false
			}
			continue
		}
		if r == ' ' {
			return false
		}
		return false
	}
	return seenDigit
}

// isCJK returns true if the string contains only CJK (no ASCII letters or digits), spaces, or punctuation
func isCJK(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if r <= 127 && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}

// isUnquotedAllowed returns true if the string can be safely emitted unquoted in YAML
func isUnquotedAllowed(s string) bool {
	if len(s) == 0 {
		return false
	}
	// Check for special YAML values
	switch s {
	case "null", "true", "false", "yes", "no", "on", "off", "y", "n":
		return false
	}
	// Check for special characters or spaces
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

// JSONLToCPD converts a JSONL file to CPD YAML format
func JSONLToCPD(r io.Reader) (string, error) {
	// First pass: collect all records and analyze field distributions
	var allRecords []*orderedmapjson.AnyOrderedMap
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

		allRecords = append(allRecords, record)
	}

	// Use TableDeriver to analyze field distributions and identify good join candidates
	deriver := relational.NewTableDeriver()
	if err := deriver.ProcessHistory(allRecords); err != nil {
		return "", fmt.Errorf("failed to analyze field distributions: %w", err)
	}

	// Get sophisticated join table candidates with scores
	joinCandidates := deriver.GetJoinTableCandidates()
	fieldInfo := deriver.GetFieldInfo()

	// Determine which fields should be join fields based on sophisticated analysis
	joinFields := orderedmapjson.NewAnyOrderedMap()
	columns := []string{"time"} // Always include time as first column

	// Sort candidates by score (highest first) for optimal selection
	type candidateScore struct {
		field string
		score float64
	}
	var candidates []candidateScore
	for field, score := range joinCandidates {
		candidates = append(candidates, candidateScore{field, score})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Select top-scoring candidates for join tables
	// Use adaptive threshold based on data characteristics
	threshold := 0.4 // Base threshold
	if len(candidates) > 0 {
		// Adjust threshold based on the best candidate's score
		bestScore := candidates[0].score
		if bestScore > 0.7 {
			threshold = 0.5 // Higher threshold for high-quality candidates
		} else if bestScore < 0.5 {
			threshold = 0.3 // Lower threshold for lower-quality candidates
		}
	}

	for _, candidate := range candidates {
		if candidate.score >= threshold {
			info := fieldInfo[candidate.field]
			// Additional validation: ensure it's a good candidate
			if info != nil && info.ElementType == "string" && info.UniqueValues > 1 {
				joinFields.Set(candidate.field, true)
				columns = append(columns, candidate.field)
			}
		}
	}

	// Add payload as last column
	columns = append(columns, "payload")

	// Create join tables for the selected fields
	joinTables := make(map[string]map[string]int)
	for el := joinFields.Front(); el != nil; el = el.Next() {
		field := el.Key
		joinTables[field] = make(map[string]int)
		id := 1

		// Get the field stats to extract unique values
		stats := deriver.FieldStats[field]
		if stats == nil {
			continue
		}

		// Create a map to track first appearance order
		firstAppearance := make(map[string]int)
		order := 0

		// Go through all records to find first appearance order
		for _, record := range allRecords {
			if value, exists := record.Get(field); exists {
				switch v := value.(type) {
				case string:
					if v != "" && firstAppearance[v] == 0 {
						firstAppearance[v] = order
						order++
					}
				case []interface{}:
					for _, item := range v {
						if str, ok := item.(string); ok && str != "" && firstAppearance[str] == 0 {
							firstAppearance[str] = order
							order++
						}
					}
				}
			}
		}

		// Sort values by first appearance order
		var valueNames []string
		for value := range firstAppearance {
			valueNames = append(valueNames, value)
		}
		sort.Slice(valueNames, func(i, j int) bool {
			return firstAppearance[valueNames[i]] < firstAppearance[valueNames[j]]
		})

		for _, value := range valueNames {
			joinTables[field][value] = id
			id++
		}
	}

	// Reset reader for second pass
	if seeker, ok := r.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return "", fmt.Errorf("failed to reset reader: %w", err)
		}
	} else {
		return "", fmt.Errorf("reader must be seekable")
	}

	// Second pass: convert records using the smart join table decisions
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

		// Build row values
		rowValues := make([]interface{}, len(columns))
		rowValues[0] = timeStr // time is always first

		// Handle join fields
		for i, col := range columns[1 : len(columns)-1] { // Skip time and payload
			if joinTable, isJoin := joinTables[col]; isJoin {
				if value, exists := record.Get(col); exists {
					switch v := value.(type) {
					case string:
						if id, ok := joinTable[v]; ok {
							rowValues[i+1] = id
						} else {
							rowValues[i+1] = nil // Unknown value, use null
						}
					case []interface{}:
						var ids []interface{}
						for _, item := range v {
							if str, ok := item.(string); ok {
								if str == "" {
									ids = append(ids, "")
									continue
								}
								if id, ok := joinTable[str]; ok {
									ids = append(ids, id)
								}
							}
						}
						rowValues[i+1] = ids
					default:
						rowValues[i+1] = nil
					}
				} else {
					rowValues[i+1] = nil
				}
			}
		}

		// Handle payload (all remaining fields) - preserve original order
		payload := orderedmapjson.NewAnyOrderedMap()
		for el := record.Front(); el != nil; el = el.Next() {
			if el.Key != "time" && !joinFields.Has(el.Key) {
				payload.Set(el.Key, el.Value)
			}
		}
		rowValues[len(columns)-1] = payload

		data = append(data, rowValues)
	}

	// Create a buffer for the YAML output
	var buf bytes.Buffer

	// Write version if present
	if currentVersion > 0 {
		buf.WriteString("_version: ")
		buf.WriteString(fmt.Sprintf("%d\n", currentVersion))
	}

	// Add columns section
	buf.WriteString("_columns: [")
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col)
	}
	buf.WriteString("]\n")

	// Add join tables
	for el := joinFields.Front(); el != nil; el = el.Next() {
		field := el.Key
		joinTable := joinTables[field]
		buf.WriteString(fmt.Sprintf("%s:\n", field))
		// Collect (value, id) pairs and sort by id
		type pair struct {
			value string
			id    int
		}
		var pairs []pair
		for value, id := range joinTable {
			pairs = append(pairs, pair{value, id})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].id < pairs[j].id })
		for _, p := range pairs {
			buf.WriteString(fmt.Sprintf("  %s: %d\n", p.value, p.id))
		}
	}

	// Add data section
	buf.WriteString("data:\n")
	for _, row := range data {
		rowArray := row.([]interface{})
		buf.WriteString("  - [")

		for i, val := range rowArray {
			if i > 0 {
				buf.WriteString(", ")
			}

			switch v := val.(type) {
			case string:
				buf.WriteString(fmt.Sprintf("%q", v))
			case int:
				buf.WriteString(fmt.Sprintf("%d", v))
			case []interface{}:
				buf.WriteString("[")
				for j, elem := range v {
					if j > 0 {
						buf.WriteString(", ")
					}
					switch e := elem.(type) {
					case int:
						buf.WriteString(fmt.Sprintf("%d", e))
					case string:
						buf.WriteString(fmt.Sprintf("%q", e))
					default:
						buf.WriteString(fmt.Sprintf("%v", e))
					}
				}
				buf.WriteString("]")
			case *orderedmapjson.AnyOrderedMap:
				if v.Len() == 0 {
					buf.WriteString("null")
				} else {
					buf.WriteString("{")
					keyIndex := 0
					for el := v.Front(); el != nil; el = el.Next() {
						if keyIndex > 0 {
							buf.WriteString(", ")
						}
						keyIndex++
						buf.WriteString(el.Key)
						buf.WriteString(": ")
						buf.WriteString(formatYAMLValue(el.Value))
					}
					buf.WriteString("}")
				}
			case nil:
				buf.WriteString("null")
			default:
				buf.WriteString(fmt.Sprintf("%v", v))
			}
		}
		buf.WriteString("]\n")
	}

	return buf.String(), nil
}

// Add this helper at the end of the file:
func flattenMeta(prefix string, v interface{}, out *orderedmapjson.AnyOrderedMap) {
	switch val := v.(type) {
	case *orderedmapjson.AnyOrderedMap:
		for el := val.Front(); el != nil; el = el.Next() {
			flattenMeta(prefix+"."+el.Key, el.Value, out)
		}
	default:
		out.Set(prefix, val)
	}
}

// deepCopyOrderedMap creates a deep copy of an ordered map
func deepCopyOrderedMap(src *orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap {
	if src == nil {
		return nil
	}
	dst := orderedmapjson.NewAnyOrderedMap()
	for el := src.Front(); el != nil; el = el.Next() {
		// Deep copy nested maps
		if subMap, ok := el.Value.(*orderedmapjson.AnyOrderedMap); ok {
			dst.Set(el.Key, deepCopyOrderedMap(subMap))
		} else {
			dst.Set(el.Key, el.Value)
		}
	}
	return dst
}
