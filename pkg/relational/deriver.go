package relational

import (
	"fmt"
	"math"

	"github.com/GitRowin/orderedmapjson"
)

const (
	// MaxUniqueValues is the maximum number of unique values we'll track for a string field
	// Beyond this, we assume it's not a good candidate for a join table
	MaxUniqueValues = 1000

	// MinOccurrences is the minimum number of times a value must appear to be considered
	// for a join table
	MinOccurrences = 3
)

// FieldCategory represents the type of a field
type FieldCategory int

const (
	// FieldCategoryUnknown represents a field we can't categorize
	FieldCategoryUnknown FieldCategory = iota
	// FieldCategoryOneToMany represents a field that maps one record to many values (e.g. "event")
	FieldCategoryOneToMany
	// FieldCategoryManyToMany represents a field that maps many records to many values (e.g. "tags")
	FieldCategoryManyToMany
	// FieldCategoryUnique represents a field with unique values (e.g. "id", "timestamp")
	FieldCategoryUnique
	// FieldCategoryCommand represents a special command field (starts with @)
	FieldCategoryCommand
)

func GetHumanFriendlyFieldCategory(category FieldCategory) string {
	switch category {
	case FieldCategoryManyToMany:
		return "many-to-many"
	case FieldCategoryOneToMany:
		return "one-to-many"
	case FieldCategoryUnique:
		return "unique"
	case FieldCategoryCommand:
		return "command"
	case FieldCategoryUnknown:
		return "unknown"
	}
	return "unknown"
}

func GetCategoryString(category FieldCategory) string {
	switch category {
	case FieldCategoryManyToMany:
		return "many-to-many"
	case FieldCategoryOneToMany:
		return "one-to-many"
	case FieldCategoryUnknown:
		return "unknown"
	}
	return "unknown"
}

// FieldInfo holds metadata about a field
type FieldInfo struct {
	Name          string
	Category      FieldCategory
	IsArray       bool
	ElementType   string
	TotalRecords  int
	TotalElements int
	UniqueValues  int
	Score         float64
}

// ValueStats tracks statistics about field values
type ValueStats struct {
	TotalOccurrences int                           // Total non-nil occurrences
	TotalElements    int                           // For arrays: total number of elements across all records
	Values           *orderedmapjson.AnyOrderedMap // Frequency of each value
	IsArray          bool                          // Whether this is an array field
	ElementType      string                        // For arrays: type of elements
}

// TableDeriver analyzes JSONL records to derive relational table structures
type TableDeriver struct {
	FieldStats map[string]*ValueStats // Stats for all fields
}

// NewTableDeriver creates a new TableDeriver instance
func NewTableDeriver() *TableDeriver {
	return &TableDeriver{
		FieldStats: make(map[string]*ValueStats),
	}
}

// ProcessHistory analyzes a slice of records to identify potential join tables
func (d *TableDeriver) ProcessHistory(history []*orderedmapjson.AnyOrderedMap) error {
	// First pass: collect statistics for all fields
	for _, record := range history {
		for el := record.Front(); el != nil; el = el.Next() {
			field := el.Key
			value := el.Value

			// Skip special fields
			if field[0] == '_' {
				continue
			}

			// Initialize stats for this field if not exists
			if _, exists := d.FieldStats[field]; !exists {
				d.FieldStats[field] = &ValueStats{
					Values: orderedmapjson.NewAnyOrderedMap(),
				}
			}
			stats := d.FieldStats[field]

			// Handle arrays
			if arr, ok := value.([]interface{}); ok {
				stats.IsArray = true
				stats.TotalOccurrences++
				stats.TotalElements += len(arr)

				// Only process if we haven't exceeded the limit
				if stats.Values.Len() < MaxUniqueValues {
					// Determine element type from first non-nil value
					if stats.ElementType == "" && len(arr) > 0 {
						for _, v := range arr {
							if v != nil {
								switch v.(type) {
								case string:
									stats.ElementType = "string"
								case float64:
									stats.ElementType = "number"
								case bool:
									stats.ElementType = "boolean"
								default:
									stats.ElementType = "unknown"
								}
								break
							}
						}
					}

					// Track values
					for _, v := range arr {
						if str, ok := v.(string); ok {
							currentLength, _ := stats.Values.Get(str)
							stats.Values.Set(str, currentLength.(int)+1)
						}
					}
				}
			} else if str, ok := value.(string); ok {
				// Handle string fields
				stats.TotalOccurrences++
				stats.TotalElements++ // For string fields, this is the same as occurrences
				if stats.Values.Len() < MaxUniqueValues {
					maybeCurrentLength, _ := stats.Values.Get(str)
					currentLength := 0
					if maybeCurrentLength != nil {
						currentLength = maybeCurrentLength.(int)
					}
					stats.Values.Set(str, currentLength+1)
				}
			}
		}
	}

	return nil
}

// calculateEntropy computes the Shannon entropy of the value distribution
func (stats *ValueStats) calculateEntropy() float64 {
	if stats.TotalOccurrences == 0 {
		return 0
	}

	var entropy float64
	for key := range stats.Values.Keys() {
		count, _ := stats.Values.Get(key)
		p := float64(count.(int)) / float64(stats.TotalOccurrences)
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// calculateGini computes the Gini coefficient of the value distribution
func (stats *ValueStats) calculateGini() float64 {
	if stats.TotalOccurrences == 0 {
		return 0
	}

	var sumSquares float64
	for count := range stats.Values.Values() {
		p := float64(count.(int)) / float64(stats.TotalOccurrences)
		sumSquares += p * p
	}
	return 1 - sumSquares
}

// calculateMaxFrequency returns the ratio of the most frequent value
func (stats *ValueStats) calculateMaxFrequency() float64 {
	if stats.TotalOccurrences == 0 {
		return 0
	}

	var maxCount int
	for count := range stats.Values.Values() {
		if count.(int) > maxCount {
			maxCount = count.(int)
		}
	}
	return float64(maxCount) / float64(stats.TotalOccurrences)
}

// calculateReuseRatio returns the ratio of total occurrences to unique values
func (stats *ValueStats) calculateReuseRatio() float64 {
	if stats.Values.Len() == 0 {
		return 0
	}
	return float64(stats.TotalOccurrences) / float64(stats.Values.Len())
}

// GetJoinTableCandidates returns fields that should be join tables with their scores
func (d *TableDeriver) GetJoinTableCandidates() map[string]float64 {
	candidates := make(map[string]float64)

	for field, stats := range d.FieldStats {
		if stats.TotalOccurrences == 0 || stats.Values.Len() == 0 {
			continue
		}

		// Calculate base metrics
		entropy := stats.calculateEntropy()
		maxEntropy := math.Log2(float64(stats.Values.Len()))
		normalizedEntropy := entropy / maxEntropy
		inverseEntropy := 1 - normalizedEntropy
		gini := stats.calculateGini()
		maxFreq := stats.calculateMaxFrequency()

		// Calculate reuse ratio based on field type
		var reuseRatio float64
		if stats.IsArray {
			reuseRatio = float64(stats.TotalElements) / float64(stats.Values.Len())
		} else {
			reuseRatio = float64(stats.TotalOccurrences) / float64(stats.Values.Len())
		}

		// Calculate final score
		// For arrays (many-to-many), we weight reuse more heavily
		// For strings (one-to-many), we weight concentration more heavily
		var score float64
		if stats.IsArray {
			score = 0.3*inverseEntropy + 0.2*gini + 0.5*math.Tanh(math.Log(reuseRatio))
		} else {
			score = 0.4*inverseEntropy + 0.4*maxFreq + 0.2*math.Tanh(math.Log(reuseRatio))
		}

		// Only include if score is above threshold
		if score > 0.3 {
			candidates[field] = score
			fmt.Printf("Field: %s (array: %v)\n", field, stats.IsArray)
			fmt.Printf("  Entropy: %.3f (inverse: %.3f)\n", entropy, inverseEntropy)
			fmt.Printf("  Gini: %.3f\n", gini)
			fmt.Printf("  Max Freq: %.3f\n", maxFreq)
			fmt.Printf("  Reuse Ratio: %.3f\n", reuseRatio)
			fmt.Printf("  Final Score: %.3f\n", score)
		}
	}

	return candidates
}

// GetFieldInfo returns detailed information about all detected fields
func (d *TableDeriver) GetFieldInfo() map[string]*FieldInfo {
	info := make(map[string]*FieldInfo)

	for field, stats := range d.FieldStats {
		// Skip fields with no data
		if stats.TotalOccurrences == 0 {
			continue
		}

		// Skip command fields
		if len(field) > 0 && field[0] == '@' {
			fmt.Printf("skipping command field: %q\n", field)
			continue
		}

		fieldInfo := &FieldInfo{
			Name:          field,
			IsArray:       stats.IsArray,
			ElementType:   stats.ElementType,
			TotalRecords:  stats.TotalOccurrences,
			TotalElements: stats.TotalElements,
			UniqueValues:  stats.Values.Len(),
		}

		// Calculate metrics for categorization
		entropy := stats.calculateEntropy()
		maxEntropy := math.Log2(float64(stats.Values.Len()))
		normalizedEntropy := entropy / maxEntropy
		inverseEntropy := 1 - normalizedEntropy
		gini := stats.calculateGini()
		maxFreq := stats.calculateMaxFrequency()

		// Calculate reuse ratio based on field type
		var reuseRatio float64
		if stats.IsArray {
			reuseRatio = float64(stats.TotalElements) / float64(stats.Values.Len())
		} else {
			reuseRatio = float64(stats.TotalOccurrences) / float64(stats.Values.Len())
		}

		// Calculate score
		if stats.IsArray {
			fieldInfo.Score = 0.3*inverseEntropy + 0.2*gini + 0.5*math.Tanh(math.Log(reuseRatio))
		} else {
			fieldInfo.Score = 0.4*inverseEntropy + 0.4*maxFreq + 0.2*math.Tanh(math.Log(reuseRatio))
		}

		// Categorize the field
		if stats.IsArray && reuseRatio > 1.5 {
			fieldInfo.Category = FieldCategoryManyToMany
		} else if reuseRatio > 1.5 && maxFreq > 0.3 {
			fieldInfo.Category = FieldCategoryOneToMany
		} else if reuseRatio < 1.1 {
			fieldInfo.Category = FieldCategoryUnique
		} else {
			fieldInfo.Category = FieldCategoryUnknown
		}

		info[field] = fieldInfo
	}

	return info
}

// GetJoinTableCandidates identifies fields that should be converted to join tables
func GetJoinTableCandidates(fieldInfo map[string]*FieldInfo) map[string]bool {
	candidates := make(map[string]bool)

	for field, info := range fieldInfo {
		// Calculate unique value ratio
		uniqueRatio := float64(info.UniqueValues) / float64(info.TotalRecords)
		if uniqueRatio < 0.3 && info.ElementType == "string" {
			candidates[field] = true
		}
	}

	return candidates
}

// GenerateSchema creates a JSON Schema from field info
func GenerateSchema(fieldInfo map[string]*FieldInfo, joinCandidates map[string]bool) map[string]interface{} {
	schema := map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type":       "object",
			"properties": make(map[string]interface{}),
			"required":   make([]string, 0),
		},
	}

	props := schema["items"].(map[string]interface{})["properties"].(map[string]interface{})
	// required := schema["items"].(map[string]interface{})["required"].([]string)

	for field, info := range fieldInfo {
		// Skip join table fields in compacted mode
		if joinCandidates[field] {
			continue
		}

		// Add property definition
		propType := "string"
		switch info.ElementType {
		case "number":
			propType = "number"
		case "array":
			propType = "array"
		case "object":
			propType = "object"
		}

		props[field] = map[string]interface{}{
			"type": propType,
		}

		// TODO: Add to required fields if needed
	}

	return schema
}

// // ProcessHistory processes a record history to generate schema and join tables
// func ProcessHistory(history []*orderedmapjson.AnyOrderedMap) (map[string]interface{}, map[string]map[string]int, error) {

// 	fmt.Printf("history: %+v\n", history)
// 	// Get field info
// 	fieldInfo := GetFieldInfo(history)

// 	fmt.Printf("fieldInfo: %+v\n", fieldInfo)

// 	// Identify join table candidates
// 	joinCandidates := GetJoinTableCandidates(fieldInfo)

// 	// Generate schema
// 	schema := GenerateSchema(fieldInfo, joinCandidates)

// 	// Build join tables
// 	joinTables := make(map[string]map[string]int)
// 	for field := range joinCandidates {
// 		joinTables[field] = make(map[string]int)
// 		values := fieldInfo[field].UniqueValues
// 		id := 1
// 		for value := range values {
// 			if strValue, ok := value.(string); ok {
// 				joinTables[field][strValue] = id
// 				id++
// 			}
// 		}
// 	}

// 	return schema, joinTables, nil
// }
