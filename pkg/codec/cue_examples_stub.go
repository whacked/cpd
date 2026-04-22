//go:build !cue

package codec

import "io/fs"

// CUEExamplesFS is empty when built without -tags cue.
var CUEExamplesFS fs.FS

// CUESchemaExample and CUESchemaExamples are unavailable without -tags cue.
type CUESchemaExample struct {
	Name        string
	Description string
	Filename    string
}

var CUESchemaExamples []CUESchemaExample
