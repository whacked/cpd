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
    Required   []string              `json:"required,omitempty"`
}
