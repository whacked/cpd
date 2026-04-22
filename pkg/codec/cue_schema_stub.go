//go:build !cue

package codec

import "fmt"

func cueStringToJSONSchema(src string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("CUE schema support not compiled in; rebuild with -tags cue")
}
