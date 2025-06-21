package codec

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"

	"github.com/GitRowin/orderedmapjson"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/whacked/yamdb/pkg/internal"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"github.com/whacked/yamdb/pkg/types"
)

// RecursiveMerge merges newMap into oldMap, recursively handling nested maps
func RecursiveMergeOrderedMaps(oldMap *orderedmapjson.AnyOrderedMap, newMap *orderedmapjson.AnyOrderedMap) {
	for el := newMap.Front(); el != nil; el = el.Next() {
		oldVal, exists := oldMap.Get(el.Key)
		if !exists {
			// Key doesn't exist, simply add it (appended at the end)
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
			// Different types or not both maps, overwrite with new value (but keep key order)
			oldMap.Set(el.Key, el.Value)
		}
	}
}

// TODO: DELETE THIS
// RecordToJSONL converts a RecordWithMetadata to a JSONL string, only including the record data
func RecordToJSONL(record types.RecordWithMetadata) (string, error) {
	// Marshal only the record data to JSON
	jsonBytes, err := json.Marshal(record.Record)
	if err != nil {
		return "", fmt.Errorf("error marshaling to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// TODO: DELETE THIS
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
	Meta      *orderedmapjson.AnyOrderedMap
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
		Meta:           orderedmapjson.NewAnyOrderedMap(),
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
	// Add nil check to prevent panic
	if record == nil {
		return
	}

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
	// Add nil check to prevent panic
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}

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

// flattenMapRecursiveWithOrder flattens the map and records the order of first-seen flattened keys
func flattenMapRecursiveWithOrder(prefix string, m *orderedmapjson.AnyOrderedMap, delimiter string, out *orderedmapjson.AnyOrderedMap, order *[]string, seen map[string]struct{}) {
	for el := m.Front(); el != nil; el = el.Next() {
		key := el.Key
		val := el.Value

		fullKey := key
		if prefix != "" {
			fullKey = prefix + delimiter + key
		}

		if subMap, ok := val.(*orderedmapjson.AnyOrderedMap); ok {
			flattenMapRecursiveWithOrder(fullKey, subMap, delimiter, out, order, seen)
		} else {
			out.Set(fullKey, val)
			if _, exists := seen[fullKey]; !exists {
				*order = append(*order, fullKey)
				seen[fullKey] = struct{}{}
			}
		}
	}
}

// ExpandMetaDataFieldsWithOrder flattens the map and returns the flattened map and the order of keys
func ExpandMetaDataFieldsWithOrder(dataMap *orderedmapjson.AnyOrderedMap, delimiter string) (*orderedmapjson.AnyOrderedMap, []string) {
	if delimiter == "" {
		delimiter = defaultDelimiter
	}
	result := orderedmapjson.NewAnyOrderedMap()
	order := []string{}
	seen := make(map[string]struct{})
	flattenMapRecursiveWithOrder("_meta", dataMap, delimiter, result, &order, seen)
	return result, order
}

// ExpandRecord combines version, schema, and meta data with a record into a single ordered map
func ExpandRecord(record *orderedmapjson.AnyOrderedMap, version int, schemas, meta *orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap {
	if record == nil {
		return nil
	}

	dataRecord := record.Copy()
	outputRecord := orderedmapjson.NewAnyOrderedMap()

	// handle special records
	if recordVersion, ok := dataRecord.Get("_version"); ok {
		version = int(recordVersion.(float64))
		dataRecord.Delete("_version")
	}
	if recordSchema, ok := dataRecord.Get("_schema"); ok {
		RecursiveMergeOrderedMaps(schemas, recordSchema.(*orderedmapjson.AnyOrderedMap))
		dataRecord.Delete("_schema")
	}
	if recordMeta, ok := dataRecord.Get("_meta"); ok {
		RecursiveMergeOrderedMaps(meta, recordMeta.(*orderedmapjson.AnyOrderedMap))
		dataRecord.Delete("_meta")
	}

	if dataRecord.Len() == 0 {
		return nil
	}

	if version > 0 {
		outputRecord.Set("_version", version)
	} else {
		fmt.Printf("no version found in record\n")
	}

	if meta.Len() > 0 {
		expandedMeta, metaOrder := ExpandMetaDataFieldsWithOrder(meta, ".")
		for _, key := range metaOrder {
			val, _ := expandedMeta.Get(key)
			outputRecord.Set(key, val)
		}
	}

	// add the data record to the output record
	for key := range dataRecord.Keys() {
		val, _ := dataRecord.Get(key)
		outputRecord.Set(key, val)
	}

	return outputRecord
}

// OrderedMapToJSONL converts a single ordered map to a JSONL string
func OrderedMapToJSONL(record *orderedmapjson.AnyOrderedMap) string {
	if record == nil {
		return ""
	}

	var jsonl strings.Builder
	jsonl.WriteString("{")
	keyIndex := 0
	for key := range record.Keys() {
		if keyIndex > 0 {
			jsonl.WriteString(",")
		}
		keyIndex++
		// Marshal the key
		jsonKey, _ := json.Marshal(key)
		jsonl.Write(jsonKey)
		jsonl.WriteString(":")

		val, _ := record.Get(key)
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
	return jsonl.String()
}

func (p *JSONLProcessor) ToExpandedJSONL(expandAndCarrySpecialFields bool) string {
	var currentVersion int = 0
	currentSchemas := *orderedmapjson.NewAnyOrderedMap()
	currentMeta := *orderedmapjson.NewAnyOrderedMap()
	jsonl := strings.Builder{}

	for _, record := range p.RecordHistory {

		if expandAndCarrySpecialFields {
			if newVersion, ok := record.Get("_version"); ok {
				currentVersion = int(newVersion.(float64))
			}
			if newSchema, ok := record.Get("_schema"); ok {
				RecursiveMergeOrderedMaps(&currentSchemas, newSchema.(*orderedmapjson.AnyOrderedMap))
			}
			if newMeta, ok := record.Get("_meta"); ok {
				RecursiveMergeOrderedMaps(&currentMeta, newMeta.(*orderedmapjson.AnyOrderedMap))
			}
		}

		fmt.Printf("current schemas: %+v\n", currentSchemas)
		fmt.Printf("current meta: %+v\n", currentMeta)

		expandedRecord := ExpandRecord(record, currentVersion, &currentSchemas, &currentMeta)
		if expandedRecord != nil {
			jsonl.WriteString(OrderedMapToJSONL(expandedRecord))
		}
	}

	return jsonl.String()
}

func parseNextScalarAsTimestamp(r *bufio.Reader) (float64, error) {
	var token strings.Builder
	inQuotes := false

	for {
		ch, err := r.ReadByte()
		if err != nil {
			break
		}
		if ch == '"' {
			inQuotes = !inQuotes
			token.WriteByte(ch)
		} else if ch == ',' && !inQuotes {
			r.UnreadByte()
			break
		} else {
			token.WriteByte(ch)
		}
	}

	s := strings.TrimSpace(token.String())

	// Case 1: quoted string timestamp
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
		str := strings.Trim(s, "\"")

		// Try parsing with Go's default date layouts
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999Z07:00", // common ISO-like format
		}

		for _, layout := range layouts {
			if t, err := time.Parse(layout, str); err == nil {
				return float64(t.UnixNano()) / 1e9, nil
			}
		}
		return 0, fmt.Errorf("unrecognized timestamp format: %s", str)
	}

	// Case 2: numeric timestamp (e.g. 1718065243.123)
	return strconv.ParseFloat(s, 64)
}

func parseNextArrayOfIntsOrNull(r *bufio.Reader) ([]int, error) {
	// Skip any whitespace
	for {
		ch, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		if !unicode.IsSpace(rune(ch[0])) {
			break
		}
		r.ReadByte()
	}

	// Check for null
	ch, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	if ch[0] == 'n' {
		// read "null"
		buf := make([]byte, 4)
		if _, err := r.Read(buf); err != nil || string(buf) != "null" {
			return nil, fmt.Errorf("invalid null literal")
		}
		return []int{}, nil
	}

	if ch[0] != '[' {
		return nil, fmt.Errorf("expected '[' or 'null'")
	}

	// Consume opening bracket
	r.ReadByte()

	// Skip whitespace after [
	for {
		ch, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		if !unicode.IsSpace(rune(ch[0])) {
			break
		}
		r.ReadByte()
	}

	// Check for empty array
	ch, _ = r.Peek(1)
	if ch[0] == ']' {
		r.ReadByte()
		return []int{}, nil
	}

	var nums []int
	for {
		// Skip whitespace
		for {
			ch, err := r.Peek(1)
			if err != nil {
				return nil, err
			}
			if !unicode.IsSpace(rune(ch[0])) {
				break
			}
			r.ReadByte()
		}

		// Check for end of array
		ch, _ = r.Peek(1)
		if ch[0] == ']' {
			r.ReadByte()
			break
		}

		// If not first number, expect comma
		if len(nums) > 0 {
			ch, _ = r.Peek(1)
			if ch[0] != ',' {
				return nil, fmt.Errorf("expected comma between array elements")
			}
			r.ReadByte()

			// Skip whitespace after comma
			for {
				ch, err := r.Peek(1)
				if err != nil {
					return nil, err
				}
				if !unicode.IsSpace(rune(ch[0])) {
					break
				}
				r.ReadByte()
			}
		}

		// Parse number
		var numStr strings.Builder
		for {
			ch, err := r.Peek(1)
			if err != nil {
				return nil, err
			}
			if !unicode.IsDigit(rune(ch[0])) && ch[0] != '-' {
				break
			}
			numStr.WriteByte(ch[0])
			r.ReadByte()
		}

		if numStr.Len() == 0 {
			return nil, fmt.Errorf("expected integer")
		}

		num, err := strconv.Atoi(numStr.String())
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}

		nums = append(nums, num)
	}

	sort.Ints(nums)
	return nums, nil
}

func consumeComma(r *bufio.Reader) error {
	ch, err := r.ReadByte()
	if err != nil {
		return err
	}
	if ch != ',' {
		return fmt.Errorf("expected comma, got %q", ch)
	}
	return nil
}

func JSONLToCommonPayloadData(line string) (*types.CommonPayloadData, error) {
	line = strings.TrimSpace(line)

	if !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
		return nil, fmt.Errorf("line must be a JSON array")
	}

	// Strip outer brackets
	inner := strings.TrimPrefix(strings.TrimSuffix(line, "]"), "[")

	// Setup scanner
	r := bufio.NewReader(strings.NewReader(inner))

	// --- Parse Timestamp (float or string) ---
	timestamp, err := parseNextScalarAsTimestamp(r)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	// Expect comma
	if err := consumeComma(r); err != nil {
		return nil, fmt.Errorf("expected comma after timestamp: %w", err)
	}

	// --- Parse Tags (null or [int,...]) ---
	tags, err := parseNextArrayOfIntsOrNull(r)
	if err != nil {
		return nil, fmt.Errorf("invalid tags: %w", err)
	}

	// Expect comma
	if err := consumeComma(r); err != nil {
		return nil, fmt.Errorf("expected comma after tags: %w", err)
	}

	// --- Read remaining bytes as object ---
	payloadBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload object: %w", err)
	}
	payloadStr := strings.TrimSpace(string(payloadBytes))

	if !strings.HasPrefix(payloadStr, "{") || !strings.HasSuffix(payloadStr, "}") {
		return nil, fmt.Errorf("payload must be JSON object")
	}

	// Parse the payload object using YAML node parser to preserve order
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(payloadStr), &node); err != nil {
		return nil, fmt.Errorf("failed to parse payload object: %w", err)
	}

	// Convert the YAML node to an ordered map
	payload := orderedmapjson.NewAnyOrderedMap()
	if err := yamlutil.ConvertNodeToOrderedMap(&node, payload); err != nil {
		return nil, fmt.Errorf("failed to convert payload to ordered map: %w", err)
	}

	fmt.Printf(">>> tags: %+v\n", tags)
	return &types.CommonPayloadData{
		Timestamp: timestamp,
		Tags:      tags,
		Payload:   payload,
	}, nil
}

// Legacy compatibility: ExpandMetaDataFields returns only the map
func ExpandMetaDataFields(dataMap *orderedmapjson.AnyOrderedMap, delimiter string) *orderedmapjson.AnyOrderedMap {
	m, _ := ExpandMetaDataFieldsWithOrder(dataMap, delimiter)
	return m
}
