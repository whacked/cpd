package relational

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/GitRowin/orderedmapjson"
)

// ValueLookup tracks the mapping between values and their IDs in value tables
type ValueLookup struct {
	ValueToID map[string]int
	NextID    int
}

// GenerateSQLiteDDL generates SQLite DDL for the main data table and join tables from field info
func GenerateSQLiteDDL(fieldInfo map[string]*FieldInfo, mainTableName string) string {
	var ddl []string
	var mainCols []string
	var valueTables []string
	var joinTables []string

	// Sort fields for stable output
	var fieldNames []string
	for name := range fieldInfo {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// First, create value tables for all categorical fields
	for _, name := range fieldNames {
		info := fieldInfo[name]
		if info.Category == FieldCategoryManyToMany || info.Category == FieldCategoryOneToMany {
			valueTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT);", name)
			valueTables = append(valueTables, valueTable)
		}
	}

	// Main table columns
	for _, name := range fieldNames {
		info := fieldInfo[name]
		if info.Category == FieldCategoryOneToMany {
			mainCols = append(mainCols, fmt.Sprintf("%s_id INTEGER REFERENCES %s(id)", name, name))
		} else if info.Category == FieldCategoryManyToMany {
			// Do not add to main table, handled by join table
			continue
		} else {
			colType := "TEXT"
			if info.ElementType == "number" {
				colType = "REAL"
			}
			mainCols = append(mainCols, fmt.Sprintf("%s %s", name, colType))
		}
	}

	ddl = append(ddl, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, %s);", mainTableName, strings.Join(mainCols, ", ")))
	ddl = append(ddl, valueTables...)

	// Join tables for many-to-many
	for _, name := range fieldNames {
		info := fieldInfo[name]
		if info.Category == FieldCategoryManyToMany {
			joinTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_%s_join (data_id INTEGER REFERENCES %s(id), %s_id INTEGER REFERENCES %s(id));", mainTableName, name, mainTableName, name, name)
			joinTables = append(joinTables, joinTable)
		}
	}
	ddl = append(ddl, joinTables...)
	return strings.Join(ddl, "\n")
}

// GenerateSQLiteInserts generates INSERT statements for all tables
func GenerateSQLiteInserts(fieldInfo map[string]*FieldInfo, history []*orderedmapjson.AnyOrderedMap) (string, error) {
	// First, build value lookups for all categorical fields
	valueLookups := make(map[string]*ValueLookup)
	for field, info := range fieldInfo {
		if info.Category == FieldCategoryManyToMany || info.Category == FieldCategoryOneToMany {
			valueLookups[field] = &ValueLookup{
				ValueToID: make(map[string]int),
				NextID:    1,
			}
		}
	}

	var inserts []string

	// First pass: collect all unique values and assign IDs
	for _, record := range history {
		for el := record.Front(); el != nil; el = el.Next() {
			field := el.Key
			value := el.Value
			// Skip special fields
			if field[0] == '_' {
				continue
			}

			_, exists := fieldInfo[field]
			if !exists {
				continue
			}

			lookup, isCategorical := valueLookups[field]
			if !isCategorical {
				continue
			}

			// Handle arrays (many-to-many)
			if arr, ok := value.([]interface{}); ok {
				for _, v := range arr {
					if str, ok := v.(string); ok {
						if _, exists := lookup.ValueToID[str]; !exists {
							lookup.ValueToID[str] = lookup.NextID
							lookup.NextID++
						}
					}
				}
			} else if str, ok := value.(string); ok {
				// Handle strings (one-to-many)
				if _, exists := lookup.ValueToID[str]; !exists {
					lookup.ValueToID[str] = lookup.NextID
					lookup.NextID++
				}
			}
		}
	}

	// Generate INSERT statements for value tables
	for field, lookup := range valueLookups {
		for value, id := range lookup.ValueToID {
			insert := fmt.Sprintf("INSERT INTO %s (id, value) VALUES (%d, '%s');", field, id, value)
			inserts = append(inserts, insert)
		}
	}

	// Second pass: generate INSERT statements for main table and join tables
	for i, record := range history {
		// Skip special records
		if _, hasSchema := record.Get("_schema"); hasSchema {
			continue
		}
		if _, hasMeta := record.Get("_meta"); hasMeta {
			continue
		}

		// Collect column names and values for main table
		var cols []string
		var vals []string
		var joinInserts []string

		// Add id column
		cols = append(cols, "id")
		vals = append(vals, fmt.Sprintf("%d", i+1))

		// Process each field
		for el := record.Front(); el != nil; el = el.Next() {
			field := el.Key
			value := el.Value
			// Skip special fields
			if field[0] == '_' {
				continue
			}

			info, exists := fieldInfo[field]
			if !exists {
				continue
			}

			lookup, isCategorical := valueLookups[field]
			if !isCategorical {
				// Non-categorical field, add as is
				cols = append(cols, field)
				switch v := value.(type) {
				case string:
					vals = append(vals, fmt.Sprintf("'%s'", v))
				case []interface{}, map[string]interface{}:
					// JSON-serialize arrays and objects
					jsonBytes, err := json.Marshal(v)
					if err != nil {
						return "", fmt.Errorf("failed to JSON-serialize field %s: %v", field, err)
					}
					vals = append(vals, fmt.Sprintf("'%s'", string(jsonBytes)))
				default:
					vals = append(vals, fmt.Sprintf("%v", v))
				}
				continue
			}

			// Handle categorical fields
			if info.Category == FieldCategoryOneToMany {
				// One-to-many: add foreign key to main table
				if str, ok := value.(string); ok {
					if id, exists := lookup.ValueToID[str]; exists {
						cols = append(cols, field+"_id")
						vals = append(vals, fmt.Sprintf("%d", id))
					}
				}
			} else if info.Category == FieldCategoryManyToMany {
				// Many-to-many: generate join table inserts
				if arr, ok := value.([]interface{}); ok {
					for _, v := range arr {
						if str, ok := v.(string); ok {
							if id, exists := lookup.ValueToID[str]; exists {
								joinInsert := fmt.Sprintf("INSERT INTO data_%s_join (data_id, %s_id) VALUES (%d, %d);",
									field, field, i+1, id)
								joinInserts = append(joinInserts, joinInsert)
							}
						}
					}
				}
			}
		}

		// Generate main table INSERT
		mainInsert := fmt.Sprintf("INSERT INTO data (%s) VALUES (%s);",
			strings.Join(cols, ", "),
			strings.Join(vals, ", "))
		inserts = append(inserts, mainInsert)

		// Add join table inserts
		inserts = append(inserts, joinInserts...)
	}

	return strings.Join(inserts, "\n"), nil
}
