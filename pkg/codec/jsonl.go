package codec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/whacked/yamdb/pkg/types"
)

// ProcessedRecord represents the result of processing a JSONL record
type ProcessedRecord struct {
	Version *int
	Schema  interface{} // Raw JSON Schema object
	Meta    map[string]interface{}
	// Data    *types.Record // if it's a data row
	Data *map[string]interface{}
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

		fmt.Println(buf.String())
	}
	fmt.Println()
}

// Result represents how a single Record line should be interpreted.
type JSONLProcessResult struct {
	// One of these is set depending on the directive type.
	Version   *int
	Schema    []types.ColumnInfo
	Meta      map[string]interface{}
	Data      *types.Record // if it's a data row
	IsCommand bool          // for "@..." commands, future extension
}

func intPtr(i int) *int {
	return &i
}

// ParseSchemaObject converts a valid JSON Schema object into []ColumnInfo.
// It expects a schema with type: "object" and a "properties" map.
func ParseSchemaObject(raw interface{}) ([]types.ColumnInfo, error) {
	schema, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("_schema must be a JSON object")
	}

	if schema["type"] != "object" {
		return nil, fmt.Errorf("_schema must be of type 'object'")
	}

	propsRaw, ok := schema["properties"]
	if !ok {
		return nil, fmt.Errorf("_schema missing 'properties'")
	}

	props, ok := propsRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'properties' must be an object")
	}

	// We extract in insertion order only if ordering is preserved externally
	var cols []types.ColumnInfo
	for name, defRaw := range props {
		colType := types.TypeString // default
		if def, ok := defRaw.(map[string]interface{}); ok {
			if typeVal, ok := def["type"]; ok {
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

// ProcessJSONLRecord interprets one line of JSONL as a Record.
// It detects and extracts reserved keywords (_version, _schema, _meta),
// and returns a structured result. Everything else is passed through as data.
func ProcessJSONLRecord(r types.Record) (*JSONLProcessResult, error) {
	result := &JSONLProcessResult{}

	// Check for version
	if v, ok := r["_version"]; ok {
		fmt.Printf("\n[DEBUG] Detected _version field: %v\n", v)
		versionNum, ok := v.(float64) // JSON numbers decode as float64
		if !ok {
			return nil, fmt.Errorf("_version must be a number")
		}
		fmt.Printf("[DEBUG] Parsed version number: %d\n", int(versionNum))
		result.Version = intPtr(int(versionNum))
	}

	// Check for schema
	if schemaRaw, ok := r["_schema"]; ok {
		fmt.Printf("\n[DEBUG] Detected _schema field: %v\n", schemaRaw)
		cols, err := ParseSchemaObject(schemaRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid _schema: %w", err)
		}
		fmt.Printf("[DEBUG] Parsed schema with %d columns:\n", len(cols))
		for _, col := range cols {
			fmt.Printf("  - %s (%s)\n", col.Name, types.ColumnTypeToString(col.Type))
		}
		result.Schema = cols
	}

	// Check for meta
	if metaRaw, ok := r["_meta"]; ok {
		fmt.Printf("\n[DEBUG] Detected _meta field: %v\n", metaRaw)
		meta, ok := metaRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("_meta must be an object")
		}
		fmt.Printf("[DEBUG] Parsed meta with %d fields:\n", len(meta))
		for k, v := range meta {
			fmt.Printf("  - %s: %v\n", k, v)
		}
		result.Meta = meta
	}

	// Check for commands
	for _, v := range r {
		if str, ok := v.(string); ok && strings.HasPrefix(str, "@") {
			fmt.Printf("\n[DEBUG] Detected command: %s\n", str)
			result.Data = &r
			result.IsCommand = true
			return result, nil // Commands are the only case where we return early
		}
	}

	// If we have any special fields, return the result
	if result.Version != nil || result.Schema != nil || result.Meta != nil {
		return result, nil
	}

	// Default: a normal data row
	result.Data = &r
	return result, nil
}

// CategoryProcessor defines how to handle a special category
type CategoryProcessor interface {
	// Process handles the special category logic
	Process(record types.Record) error
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
	RecordHistory []types.Record
	currentIndex  int
}

// NewJSONLProcessor creates a new JSONL processor
func NewJSONLProcessor() *JSONLProcessor {
	processor := &JSONLProcessor{
		Meta:          make(map[string]interface{}),
		RecordHistory: make([]types.Record, 0),
		currentIndex:  -1,
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
func (p *JSONLProcessor) processCategories(record types.Record) error {
	if category, ok := record["category"].(string); ok {
		// Split categories by comma
		categories := strings.Split(category, ",")
		for _, cat := range categories {
			cat = strings.TrimSpace(cat)
			// Skip if not a special category
			if !strings.HasPrefix(cat, "@") {
				continue
			}

			// Find and use appropriate processor
			for _, processor := range p.categoryProcessors {
				if processor.Category() == cat {
					if err := processor.Process(record); err != nil {
						return fmt.Errorf("failed to process category %s: %w", cat, err)
					}
					fmt.Printf("[DEBUG] Processed category: %s\n", cat)
				}
			}
		}
	}
	return nil
}

// GetRecordAt returns the record at the specified index relative to current
// offset: 0 is current record, -1 is previous, -2 is two back, etc.
func (p *JSONLProcessor) GetRecordAt(offset int) (types.Record, error) {
	targetIndex := p.currentIndex + offset
	if targetIndex < 0 || targetIndex >= len(p.RecordHistory) {
		return nil, fmt.Errorf("no record at offset %d (current: %d, history: %d)",
			offset, p.currentIndex, len(p.RecordHistory))
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

func (p *MergeCategoryProcessor) Process(record types.Record) error {
	// Get previous record
	prevRecord, err := p.processor.GetRecordAt(-1)
	if err != nil {
		return fmt.Errorf("no previous record to merge with: %w", err)
	}

	// Create a new record by merging previous into current
	merged := make(types.Record)

	// First copy all fields from previous record
	for k, v := range prevRecord {
		merged[k] = v
	}

	// Then overlay with current record's fields
	for k, v := range record {
		// Skip special fields, category, and timestamp
		if strings.HasPrefix(k, "_") || k == "category" || k == "timestamp" {
			continue
		}
		merged[k] = v
	}

	// Update the previous record with merged data
	p.processor.RecordHistory[p.processor.currentIndex-1] = merged

	// Remove the current record from history (it's been swallowed)
	p.processor.RecordHistory = p.processor.RecordHistory[:p.processor.currentIndex]
	p.processor.currentIndex--

	fmt.Printf("[DEBUG] Merged record at index %d: %v\n",
		p.processor.currentIndex, merged)
	fmt.Printf("[DEBUG] Current record swallowed, history length now: %d\n",
		len(p.processor.RecordHistory))
	return nil
}

// ProcessRecord processes a single record, updating internal state
func (p *JSONLProcessor) ProcessRecord(record types.Record) (*ProcessedRecord, error) {
	result := &ProcessedRecord{
		Version: &p.Version,
		Schema:  p.Schema,
		Meta:    p.Meta,
	}

	// Process special fields
	if version, ok := record["_version"]; ok {
		if v, ok := version.(float64); ok {
			p.Version = int(v)
			result.Version = &p.Version
			fmt.Printf("[DEBUG] Updated version to: %d\n", p.Version)
		}
	}

	if schema, ok := record["_schema"]; ok {
		// Store the raw schema object
		p.Schema = schema
		result.Schema = schema

		// Create and cache the validator
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			fmt.Printf("[WARN] Failed to create JSON Schema: %v\n", err)
		} else {
			validator, err := jsonschema.CompileString("schema.json", string(schemaBytes))
			if err != nil {
				fmt.Printf("[WARN] Failed to compile JSON Schema: %v\n", err)
			} else {
				p.validator = validator
				fmt.Printf("[DEBUG] Updated schema and cached validator\n")
			}
		}
	}

	if meta, ok := record["_meta"]; ok {
		if m, ok := meta.(map[string]interface{}); ok {
			// Merge new meta with existing
			for k, v := range m {
				p.Meta[k] = v
			}
			fmt.Printf("[DEBUG] Updated meta: %v\n", p.Meta)
			result.Meta = p.Meta
		}
	}

	// Check if record has any non-special fields
	hasData := false
	for k := range record {
		if !strings.HasPrefix(k, "_") {
			hasData = true
			break
		}
	}

	// Only add to history if it has data
	if hasData {
		// Add record to history BEFORE processing categories
		p.RecordHistory = append(p.RecordHistory, record)
		p.currentIndex = len(p.RecordHistory) - 1
		fmt.Printf("[DEBUG] Added record to history (length: %d)\n", len(p.RecordHistory))
	} else {
		fmt.Printf("[DEBUG] Skipping empty record (only special fields)\n")
	}

	// Process any special categories
	if err := p.processCategories(record); err != nil {
		return nil, err
	}

	// If this is a data record (not just special fields), add it to the result
	if hasData {
		// Remove special fields from the plainData record
		plainData := make(map[string]interface{})
		for k, v := range record {
			if !strings.HasPrefix(k, "_") {
				plainData[k] = v
			}
		}
		if len(plainData) > 0 {
			result.Data = &plainData

			// Validate against schema if we have one
			if p.validator != nil {
				fmt.Printf("\n[DEBUG] Validating record [%v]\n", plainData)

				// Validate the record
				if err := p.validator.Validate(plainData); err != nil {
					fmt.Printf("[WARN] Record validation failed: %v\n", err)
				} else {
					fmt.Printf("[DEBUG] Record validated successfully\n")
				}
			}
		} else {
			fmt.Printf("[DEBUG] Skipping validation for directive-only record\n")
		}
	}

	return result, nil
}

// ProcessRecords processes multiple records while maintaining state
func (p *JSONLProcessor) ProcessRecords(records []types.Record) ([]*ProcessedRecord, error) {
	var results []*ProcessedRecord
	for _, record := range records {
		result, err := p.ProcessRecord(record)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}
