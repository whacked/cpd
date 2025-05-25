package schema

import (
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/whacked/yamdb/pkg/types"
)

// Validator handles schema validation for tables
type Validator struct {
	schemas map[string]*jsonschema.Schema
}

// NewValidator creates a new schema validator
func NewValidator() *Validator {
	return &Validator{
		schemas: make(map[string]*jsonschema.Schema),
	}
}

// ValidateRecord checks if a record matches its schema
func (v *Validator) ValidateRecord(tableName string, record types.Record) error {
	schema, exists := v.schemas[tableName]
	if !exists {
		return fmt.Errorf("no schema found for table %s", tableName)
	}

	return schema.Validate(record)
}

// RegisterSchema adds a new schema to the validator
func (v *Validator) RegisterSchema(tableName string, schema *jsonschema.Schema) error {
	v.schemas[tableName] = schema
	return nil
}
