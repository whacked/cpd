package types

// Record represents a single row of data as a map of field names to values
type Record map[string]interface{}

// Row represents a single row of data, which can be either a Record or a positional list
type Row interface {
	AsRecord() (Record, error)
	AsList() ([]interface{}, error)
}

// Table represents a collection of rows with an associated schema
type Table struct {
	Name    string
	Schema  *Schema
	Records []Record
}

// Schema represents the JSON Schema for a table
type Schema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Required   []string               `json:"required,omitempty"`
}

// ColumnType represents the inferred type of a column
type ColumnType int

const (
	TypeString ColumnType = iota
	TypeInt               // likely not used
	TypeFloat
	TypeArray
	TypeObject
)

// ColumnInfo holds metadata about a column
type ColumnInfo struct {
	Name string
	Type ColumnType
}

// RecordWithMetadata holds a record and its column information
type RecordWithMetadata struct {
	Record  map[string]interface{}
	Columns []ColumnInfo
}

// StringToColumnType converts a string type name to ColumnType
func StringToColumnType(typeStr string) ColumnType {
	switch typeStr {
	case "int":
		return TypeFloat
	case "float":
		return TypeFloat
	default:
		return TypeString
	}
}

// ColumnTypeToString converts a ColumnType to its string representation
func ColumnTypeToString(t ColumnType) string {
	switch t {
	case TypeInt:
		return "float"
	case TypeFloat:
		return "float"
	default:
		return "string"
	}
}
