//go:build cue

package codec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readCUEExample(t *testing.T, name string) string {
	t.Helper()
	for _, ex := range CUESchemaExamples {
		if ex.Name == name {
			b, err := CUEExamplesFS.ReadFile(ex.Filename)
			require.NoError(t, err, "reading embedded CUE file for %q", name)
			return string(b)
		}
	}
	t.Fatalf("no CUE example named %q", name)
	return ""
}

func TestCUEStringToJSONSchema(t *testing.T) {
	t.Run("simple object", func(t *testing.T) {
		schema, err := cueStringToJSONSchema(readCUEExample(t, "simple-object"))
		require.NoError(t, err)
		assert.Equal(t, "object", schema["type"])
		props := schema["properties"].(map[string]interface{})
		assert.Equal(t, map[string]interface{}{"type": "integer"}, props["id"])
		assert.Equal(t, map[string]interface{}{"type": "string"}, props["name"])
		req := schema["required"].([]interface{})
		assert.Contains(t, req, "id")
		assert.Contains(t, req, "name")
	})

	t.Run("array of objects", func(t *testing.T) {
		schema, err := cueStringToJSONSchema(readCUEExample(t, "array-of-objects"))
		require.NoError(t, err)
		assert.Equal(t, "array", schema["type"])
		items := schema["items"].(map[string]interface{})
		assert.Equal(t, "object", items["type"])
	})

	t.Run("optional field", func(t *testing.T) {
		schema, err := cueStringToJSONSchema(readCUEExample(t, "optional-field"))
		require.NoError(t, err)
		props := schema["properties"].(map[string]interface{})
		assert.Contains(t, props, "name")
		req, hasReq := schema["required"]
		if hasReq {
			assert.NotContains(t, req.([]interface{}), "name")
		}
	})

	t.Run("nested array field", func(t *testing.T) {
		schema, err := cueStringToJSONSchema(readCUEExample(t, "nested-array"))
		require.NoError(t, err)
		props := schema["properties"].(map[string]interface{})
		tags := props["tags"].(map[string]interface{})
		assert.Equal(t, "array", tags["type"])
		items := tags["items"].(map[string]interface{})
		assert.Equal(t, "string", items["type"])
	})

	t.Run("named reference in spread gives actionable error", func(t *testing.T) {
		for _, src := range []string{
			"#User: {id: int}\n[...#User]",
			"_User: {id: int}\n[..._User]",
			"let U = {id: int}\n[...U]",
		} {
			_, err := cueStringToJSONSchema(src)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "Inline the struct directly", "error should suggest inlining")
		}
	})

	t.Run("invalid CUE errors", func(t *testing.T) {
		_, err := cueStringToJSONSchema(`{id: notavalidtype!!!`)
		assert.Error(t, err)
	})
}

func TestCUESchemaKeyArrayShorthand(t *testing.T) {
	cpd := `_columns:
  - id
  - name
_schemas:
  data...: |
    {
      id:   int
      name: string
    }
data:
  - [1, "alice"]
  - [2, "bob"]
`
	doc, err := ParseCPD(strings.NewReader(cpd))
	require.NoError(t, err)
	require.NotNil(t, doc.Schemas)

	schema, ok := doc.Schemas["data"]
	require.True(t, ok, "schema stored under 'data', not 'data...'")

	schemaMap := schema.(map[string]interface{})
	assert.Equal(t, "array", schemaMap["type"])
	items := schemaMap["items"].(map[string]interface{})
	assert.Equal(t, "object", items["type"])
}

func TestCUESchemaKeyNoSuffix(t *testing.T) {
	cpd := `_columns:
  - version
_schemas:
  config: |
    {version: int}
config:
  version: 1
data:
  - [42]
`
	doc, err := ParseCPD(strings.NewReader(cpd))
	require.NoError(t, err)
	schema, ok := doc.Schemas["config"]
	require.True(t, ok)
	schemaMap := schema.(map[string]interface{})
	assert.NotEqual(t, "array", schemaMap["type"])
	assert.Equal(t, "object", schemaMap["type"])
}

func TestCUESchemaMapPassthrough(t *testing.T) {
	cpd := `_columns:
  - id
_schemas:
  data:
    type: array
    items:
      type: object
      properties:
        id:
          type: integer
      required: [id]
data:
  - [1]
  - [2]
`
	doc, err := ParseCPD(strings.NewReader(cpd))
	require.NoError(t, err)
	require.NotNil(t, doc.Schemas["data"])
}
