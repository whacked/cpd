//go:build cue

package codec

import "embed"

//go:embed testdata/cue
var CUEExamplesFS embed.FS

// CUESchemaExample describes one embedded CUE schema example.
type CUESchemaExample struct {
	Name        string
	Description string
	Filename    string // path relative to CUEExamplesFS root
}

// CUESchemaExamples is the canonical list of CUE schema demonstrations.
// Tests and the CLI examples output both derive from this list.
var CUESchemaExamples = []CUESchemaExample{
	{
		Name:        "simple-object",
		Description: "Basic object schema: all fields required, plain types.",
		Filename:    "testdata/cue/simple_object.cue",
	},
	{
		Name:        "array-of-objects",
		Description: "Array of objects using CUE's spread syntax `[...{...}]`. Use `data...:` as the table key to apply this as an array schema without wrapping.",
		Filename:    "testdata/cue/array_of_objects.cue",
	},
	{
		Name:        "optional-field",
		Description: "Optional field with `name?:` syntax. The field appears in `properties` but is absent from `required`.",
		Filename:    "testdata/cue/optional_field.cue",
	},
	{
		Name:        "nested-array",
		Description: "Nested array field: `tags: [...string]` produces an `items: {type: string}` array schema inline.",
		Filename:    "testdata/cue/nested_array.cue",
	},
}
