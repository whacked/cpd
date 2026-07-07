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
		Description: "Basic object schema: all fields required, plain types. The outermost braces are **optional** — a CUE block scalar is implicitly a top-level struct, so `id: int` on its own line is a complete schema. Prefer this braceless form.",
		Filename:    "testdata/cue/simple_object.cue",
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
	{
		Name:        "record-with-enum",
		Description: "Where CUE earns its keep as a JSON Schema shorthand: an enum via disjunction (`\"ok\" | \"fail\"`), a nullable field (`... | null`), and an optional nullable number. Braceless top-level, as usual.",
		Filename:    "testdata/cue/record_with_enum.cue",
	},
	{
		Name:        "array-of-objects",
		Description: "The one place braces are **required**: a struct nested inside an array keeps its braces (`[...{...}]`). Dropping them is a CUE parse error. In practice, prefer the `data...:` key suffix with a braceless object (see the array shorthand section below) instead of writing this top-level spread by hand.",
		Filename:    "testdata/cue/array_of_objects.cue",
	},
}
