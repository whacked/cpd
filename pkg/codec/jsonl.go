package codec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/whacked/yamdb/pkg/internal"
	"github.com/whacked/yamdb/pkg/types"
)

// RecursiveMerge merges newMap into oldMap, recursively handling nested maps
func RecursiveMergeOrderedMaps(oldMap *orderedmapjson.AnyOrderedMap, newMap *orderedmapjson.AnyOrderedMap) {
	for el := newMap.Front(); el != nil; el = el.Next() {
		// Check if key exists in old map
		oldVal, exists := oldMap.Get(el.Key)
		if !exists {
			// Key doesn't exist, simply add it
			oldMap.Set(el.Key, el.Value)
			continue
		}

		// Key exists, check if both are maps
		oldSubMap, oldIsMap := oldVal.(*orderedmapjson.AnyOrderedMap)
		newSubMap, newIsMap := el.Value.(*orderedmapjson.AnyOrderedMap)

		if oldIsMap && newIsMap {
			// Both are maps, recursively merge
			RecursiveMergeOrderedMaps(oldSubMap, newSubMap)
		} else {
			// Different types or not both maps, overwrite with new value
			oldMap.Set(el.Key, el.Value)
		}
	}
}

// RecordToJSONL converts a RecordWithMetadata to a JSONL string, only including the record data
func RecordToJSONL(record types.RecordWithMetadata) (string, error) {
	// Marshal only the record data to JSON
	jsonBytes, err := json.Marshal(record.Record)
	if err != nil {
		return "", fmt.Errorf("error marshaling to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// JSONLToRecord converts a JSONL string back to a RecordWithMetadata by inferring column types
func JSONLToRecord(jsonl string) (types.RecordWithMetadata, error) {
	// Unmarshal the raw data
	var record map[string]interface{}
	if err := json.Unmarshal([]byte(jsonl), &record); err != nil {
		return types.RecordWithMetadata{}, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	// Infer column types from the record data
	columns := make([]types.ColumnInfo, 0, len(record))
	for name, value := range record {
		columns = append(columns, types.ColumnInfo{
			Name: name,
			Type: InferType(value),
		})
	}

	return types.RecordWithMetadata{
		Record:  record,
		Columns: columns,
	}, nil
}

// InferType determines the ColumnType of a value
func InferType(val interface{}) types.ColumnType {
	// note we simplify number parsing to all floats
	// to preserve compatibility with the built-in JSON parser
	switch v := val.(type) {
	case int, int64:
		return types.TypeFloat
	case float32, float64:
		return types.TypeFloat
	case string:
		if matched, _ := regexp.MatchString(`^\d+$`, v); matched {
			return types.TypeFloat
		}
		if matched, _ := regexp.MatchString(`^\d*\.\d+$`, v); matched {
			return types.TypeFloat
		}
		return types.TypeString
	case []interface{}:
		return types.TypeArray
	case map[string]interface{}:
		return types.TypeObject
	default:
		return types.TypeString
	}
}

func PrintRecordAsJSONL(recordMap map[string]interface{}, orderedKeys []string) string {
	if len(orderedKeys) == 1 {
		valBytes, _ := json.Marshal(recordMap)
		return string(valBytes)
	}

	// Create a custom encoder that preserves order
	buf := new(strings.Builder)

	enc := json.NewEncoder(buf)
	enc.SetIndent("", "")

	// Write the opening brace
	buf.WriteString("{")

	// Write each field in order
	for j, key := range orderedKeys {
		if j > 0 {
			buf.WriteString(",")
		}
		// Marshal the key
		keyBytes, _ := json.Marshal(key)
		buf.Write(keyBytes)
		buf.WriteString(":")
		// Marshal the value
		valBytes, _ := json.Marshal(recordMap[key])
		buf.Write(valBytes)
	}

	// Write the closing brace
	buf.WriteString("}")

	return buf.String()
}

// PrintRecordGroupAsJSONL prints a group of records in JSON Lines format
func PrintRecordGroupAsJSONL(group *types.RecordGroup) {
	fmt.Println("\n=== JSON Lines Format ===")
	for _, record := range group.Records {
		// Create an ordered map to hold the record data
		recordMap := make(map[string]interface{})
		orderedKeys := make([]string, 0, len(group.Columns))

		// Add each field to the map using column names as keys
		for j, col := range group.Columns {
			var val interface{}
			if j < len(record.Values) {
				val = record.Values[j]
			}

			// Use column name as key, or generate one if empty
			key := col.Name
			if key == "" {
				key = fmt.Sprintf("field_%d", j)
			}
			recordMap[key] = val
			orderedKeys = append(orderedKeys, key)
		}

		fmt.Println(PrintRecordAsJSONL(recordMap, orderedKeys))
	}
	fmt.Println()
}

// Result represents how a single Record line should be interpreted.
type JSONLProcessResult struct {
	// One of these is set depending on the directive type.
	Version   *int
	Schema    []types.ColumnInfo
	Meta      *orderedmapjson.AnyOrderedMap
	Data      *orderedmapjson.AnyOrderedMap // if it's a data row
	IsCommand bool                          // for "@..." commands, future extension
}

func intPtr(i int) *int {
	return &i
}

// ParseSchemaObject converts a valid JSON Schema object into []ColumnInfo.
// It expects a schema with type: "object" and a "properties" map.
func ParseSchemaObject(schemaMap *orderedmapjson.AnyOrderedMap) ([]types.ColumnInfo, error) {
	schemaType, ok := schemaMap.Get("type")
	if !ok {
		return nil, fmt.Errorf("_schema missing 'type'")
	}
	if schemaType != "object" {
		return nil, fmt.Errorf("_schema must be of type 'object'")
	}

	propsRaw, ok := schemaMap.Get("properties")
	if !ok {
		return nil, fmt.Errorf("_schema missing 'properties'")
	}

	props, ok := propsRaw.(*orderedmapjson.AnyOrderedMap)
	if !ok {
		return nil, fmt.Errorf("'properties' must be an object")
	}

	// We extract in insertion order only if ordering is preserved externally
	var cols []types.ColumnInfo
	for el := props.Front(); el != nil; el = el.Next() {
		name := el.Key
		defRaw := el.Value
		colType := types.TypeString // default
		if def, ok := defRaw.(*orderedmapjson.AnyOrderedMap); ok {
			if typeVal, ok := def.Get("type"); ok {
				colType = mapJSONSchemaTypeToColumnType(typeVal)
			}
		}

		cols = append(cols, types.ColumnInfo{
			Name: name,
			Type: colType,
		})
	}

	return cols, nil
}

func mapJSONSchemaTypeToColumnType(schemaType interface{}) types.ColumnType {
	switch t := schemaType.(type) {
	case string:
		switch t {
		case "integer":
			return types.TypeInt
		case "number":
			return types.TypeFloat
		case "string":
			return types.TypeString
		default:
			return types.TypeString
		}
	case []interface{}:
		// Accept e.g. ["string", "null"]
		for _, item := range t {
			if str, ok := item.(string); ok {
				if str != "null" {
					return mapJSONSchemaTypeToColumnType(str)
				}
			}
		}
	}
	return types.TypeString
}

// CategoryProcessor defines how to handle a special category
type CategoryProcessor interface {
	// Process handles the special category logic
	Process(record *orderedmapjson.AnyOrderedMap) error
	// Category returns the category name this processor handles
	Category() string
}

// JSONLProcessor handles processing of JSONL records with state tracking
type JSONLProcessor struct {
	Version   int
	Schema    interface{}        // Raw JSON Schema object
	validator *jsonschema.Schema // Cached validator
	Meta      map[string]interface{}
	// Category processors
	categoryProcessors []CategoryProcessor
	// Record history for lookback operations
	RecordHistory []*orderedmapjson.AnyOrderedMap
	CurrentIndex  int
	// Canonical column order tracking
	OrderedColumns []types.ColumnInfo // Maintains the order of columns as they appear in records
	// Join tables for enum-like fields
	JoinTables map[string]map[string]int // field name -> value -> id
}

// NewJSONLProcessor creates a new JSONL processor
func NewJSONLProcessor() *JSONLProcessor {
	processor := &JSONLProcessor{
		Meta:           make(map[string]interface{}),
		RecordHistory:  make([]*orderedmapjson.AnyOrderedMap, 0),
		CurrentIndex:   -1,
		OrderedColumns: make([]types.ColumnInfo, 0),
		JoinTables:     make(map[string]map[string]int),
	}

	// Create and initialize category processors
	mergeProcessor := &MergeCategoryProcessor{
		processor: processor,
	}
	processor.categoryProcessors = []CategoryProcessor{
		mergeProcessor,
		// Add more category processors here
	}

	return processor
}

// RegisterCategoryProcessor adds a new category processor
func (p *JSONLProcessor) RegisterCategoryProcessor(processor CategoryProcessor) {
	p.categoryProcessors = append(p.categoryProcessors, processor)
}

// processCategories handles any special categories in the record
func (p *JSONLProcessor) processCategories(record *orderedmapjson.AnyOrderedMap) error {
	if category, ok := record.Get("category"); ok {

		if catStr, ok := category.(string); ok {
			// Split categories by comma
			categories := strings.Split(catStr, ",")
			for _, cat := range categories {
				cat = strings.TrimSpace(cat)
				// Skip if not a special category
				if !strings.HasPrefix(cat, "@") {
					continue
				}

				// Find and use appropriate processor
				for _, processor := range p.categoryProcessors {
					if processor.Category() == cat {
						fmt.Printf("Processing category: %s\n", cat)
						if err := processor.Process(record); err != nil {
							return fmt.Errorf("failed to process category %s: %w", cat, err)
						}
						internal.DebugLog("Processed category: %s", cat)
					}
				}
			}
		}
	}
	return nil
}

// GetRecordAt returns the record at the specified index relative to current
// offset: 0 is current record, -1 is previous, -2 is two back, etc.
func (p *JSONLProcessor) GetRecordAt(offset int) (*orderedmapjson.AnyOrderedMap, error) {
	targetIndex := p.CurrentIndex + offset
	if offset < 0 {
		targetIndex += 1
	}
	if targetIndex < 0 || targetIndex >= len(p.RecordHistory) {
		return nil, fmt.Errorf("no record at offset %d (current: %d, history: %d)",
			offset, p.CurrentIndex, len(p.RecordHistory))
	}
	return p.RecordHistory[targetIndex], nil
}

// MergeCategoryProcessor handles the @merge category
type MergeCategoryProcessor struct {
	processor *JSONLProcessor // Reference to parent processor
}

func (p *MergeCategoryProcessor) Category() string {
	return "@merge"
}

func (p *MergeCategoryProcessor) Process(record *orderedmapjson.AnyOrderedMap) error {
	prevRecord, err := p.processor.GetRecordAt(-1)
	if err != nil {
		return err
	}
	if prevRecord == nil {
		return fmt.Errorf("no previous record to merge with")
	}
	fmt.Printf("Previous record: %+v\n", prevRecord)

	record.Delete("category")

	RecursiveMergeOrderedMaps(prevRecord, record)

	// empty the record
	remainingKeys := make([]string, 0)
	for key := range record.Keys() {
		remainingKeys = append(remainingKeys, key)
	}
	for _, key := range remainingKeys {
		record.Delete(key)
	}

	return nil
}

// GetOrderedColumns returns the canonical column order as seen in processed records
func (p *JSONLProcessor) GetOrderedColumns() []types.ColumnInfo {
	return p.OrderedColumns
}

// updateOrderedColumns updates the canonical column order with new columns from a record
func (p *JSONLProcessor) updateOrderedColumns(record *orderedmapjson.AnyOrderedMap) {
	// Create a map to track which columns we've already seen
	seen := make(map[string]bool)

	// First add any existing columns that are still present
	var newColumns []types.ColumnInfo
	for _, col := range p.OrderedColumns {
		if _, ok := record.Get(col.Name); ok {
			newColumns = append(newColumns, col)
			seen[col.Name] = true
		}
	}

	// Then add any new columns
	for el := record.Front(); el != nil; el = el.Next() {
		key := el.Key
		if !seen[key] {
			newColumns = append(newColumns, types.ColumnInfo{
				Name: key,
				Type: InferType(el.Value),
			})
			seen[key] = true
		}
	}

	p.OrderedColumns = newColumns
}

// ProcessRecord processes a single JSONL record
func (p *JSONLProcessor) ProcessRecord(record *orderedmapjson.AnyOrderedMap) (*orderedmapjson.AnyOrderedMap, error) {

	// Process any category-specific logic
	if err := p.processCategories(record); err != nil {
		fmt.Printf("ERROR processing categories: %v\n", err)
		return nil, err
	}

	// if category processing consumed the record
	if record.Len() == 0 {
		return nil, nil
	}

	// FIXME: this may now be redundant
	p.updateOrderedColumns(record)

	p.RecordHistory = append(p.RecordHistory, record)
	p.CurrentIndex = len(p.RecordHistory) - 1

	return record, nil
}

const defaultDelimiter = "."

func ExpandMetaDataFields(dataMap *orderedmapjson.AnyOrderedMap, delimiter string) *orderedmapjson.AnyOrderedMap {
	if delimiter == "" {
		delimiter = defaultDelimiter
	}

	result := orderedmapjson.NewAnyOrderedMap()
	flattenMapRecursive("_meta", dataMap, delimiter, result)
	return result
}

func flattenMapRecursive(prefix string, m *orderedmapjson.AnyOrderedMap, delimiter string, out *orderedmapjson.AnyOrderedMap) {
	for el := m.Front(); el != nil; el = el.Next() {
		key := el.Key
		val := el.Value

		fullKey := key
		if prefix != "" {
			fullKey = prefix + delimiter + key
		}

		switch v := val.(type) {
		case *orderedmapjson.AnyOrderedMap:
			flattenMapRecursive(fullKey, v, delimiter, out)
		default:
			// Preserve primitives and arrays as-is
			out.Set(fullKey, val)
		}
	}
}

func (p *JSONLProcessor) ToExpandedJSONL(expandAndCarrySpecialFields bool) string {

	var currentVersion int = 0
	currentSchemas := *orderedmapjson.NewAnyOrderedMap()
	currentMeta := *orderedmapjson.NewAnyOrderedMap()
	jsonl := strings.Builder{}

	for _, record := range p.RecordHistory {

		if record == nil {
			continue
		}

		dataRecord := record.Copy()
		outputRecord := orderedmapjson.NewAnyOrderedMap()

		// handle special records
		if recordVersion, ok := dataRecord.Get("_version"); ok {
			currentVersion = int(recordVersion.(float64))
			dataRecord.Delete("_version")
		}
		if recordSchema, ok := dataRecord.Get("_schema"); ok {
			RecursiveMergeOrderedMaps(&currentSchemas, recordSchema.(*orderedmapjson.AnyOrderedMap))
			dataRecord.Delete("_schema")
		}
		if recordMeta, ok := dataRecord.Get("_meta"); ok {
			RecursiveMergeOrderedMaps(&currentMeta, recordMeta.(*orderedmapjson.AnyOrderedMap))
			dataRecord.Delete("_meta")
		}

		if dataRecord.Len() == 0 {
			continue
		}

		if expandAndCarrySpecialFields {
			// Add version if set
			if currentVersion > 0 {
				outputRecord.Set("_version", currentVersion)
			}

			if currentMeta.Len() > 0 {
				expandedMeta := ExpandMetaDataFields(&currentMeta, ".")
				for key := range expandedMeta.Keys() {
					val, _ := expandedMeta.Get(key)
					outputRecord.Set(key, val)
				}
			}
		}

		// add the data record to the output record
		for key := range dataRecord.Keys() {
			val, _ := dataRecord.Get(key)
			outputRecord.Set(key, val)
		}

		// Manually build JSONL output preserving key order
		jsonl.WriteString("{")
		keyIndex := 0
		for key := range outputRecord.Keys() {
			if keyIndex > 0 {
				jsonl.WriteString(",")
			}
			keyIndex++
			// Marshal the key
			jsonKey, _ := json.Marshal(key)
			jsonl.Write(jsonKey)
			jsonl.WriteString(":")

			val, _ := outputRecord.Get(key)
			// Marshal the value (using built-in marshaller for nested structures)
			var valBytes []byte
			if f, ok := val.(float64); ok && float64(int64(f)) == f {
				// If it's a float that looks like an integer (e.g. 25.0), force decimal point
				valBytes = []byte(fmt.Sprintf("%.1f", f))
			} else {
				valBytes, _ = json.Marshal(val)
			}
			jsonl.Write(valBytes)
		}
		jsonl.WriteString("}\n")
	}

	return jsonl.String()
}
