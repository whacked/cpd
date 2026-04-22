//go:build cue

package codec

import (
	"encoding/json"
	"fmt"
	"strings"

	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/encoding/jsonschema"
)

// ExpandCUESchema is the exported form of cueStringToJSONSchema.
func ExpandCUESchema(src string) (map[string]interface{}, error) {
	return cueStringToJSONSchema(src)
}

// cueStringToJSONSchema compiles a CUE string and generates a JSON Schema.
// Pipeline: CompileString → Generate (CUE→JSON Schema AST) → BuildExpr → json.Marshal.
func cueStringToJSONSchema(src string) (map[string]interface{}, error) {
	ctx := cuecontext.New()
	v := ctx.CompileString(src)
	if err := v.Err(); err != nil {
		return nil, fmt.Errorf("CUE compile: %w", err)
	}

	schemaAST, err := jsonschema.Generate(v, &jsonschema.GenerateConfig{})
	if err != nil {
		if strings.Contains(err.Error(), "len not supported for type _|_") {
			return nil, fmt.Errorf(
				"CUE→JSON Schema: named references in spreads (e.g. [...#Foo], [..._Foo], [...let]) "+
					"cannot be resolved by the JSON Schema generator. "+
					"Inline the struct directly: [...{field: type, ...}]",
			)
		}
		return nil, fmt.Errorf("CUE→JSON Schema: %w", err)
	}

	schemaVal := ctx.BuildExpr(schemaAST)
	if err := schemaVal.Err(); err != nil {
		return nil, fmt.Errorf("build schema value: %w", err)
	}

	jsonBytes, err := json.Marshal(schemaVal)
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &schema); err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}
	return schema, nil
}
