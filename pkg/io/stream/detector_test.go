package stream

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectFormat_JSONL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
	}{
		{
			name:     "simple jsonl",
			input:    `{"name":"test"}`,
			wantType: "jsonl",
		},
		{
			name:     "jsonl with leading whitespace",
			input:    "  \n\t  \n{\"name\":\"test\"}",
			wantType: "jsonl",
		},
		{
			name:     "jsonl with leading comments",
			input:    "// this is a comment\n// another comment\n{\"name\":\"test\"}",
			wantType: "jsonl",
		},
		{
			name:     "jsonl with mixed whitespace and comments",
			input:    "\n  // comment\n\t\n  // another\n  {\"name\":\"test\"}",
			wantType: "jsonl",
		},
		{
			name:     "multiple jsonl records",
			input:    "{\"_version\":1}\n{\"name\":\"test\"}\n",
			wantType: "jsonl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			format, data, err := DetectFormat(reader)

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, format)
			assert.Equal(t, tt.input, string(data), "data should be preserved exactly")
		})
	}
}

func TestDetectFormat_YAML(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
	}{
		{
			name:     "simple yaml",
			input:    "_columns: [name, value]",
			wantType: "yaml",
		},
		{
			name:     "yaml with leading whitespace",
			input:    "  \n\t\n_columns: [name, value]",
			wantType: "yaml",
		},
		{
			name:     "yaml with leading comments",
			input:    "// CPD file\n// version 1\n_columns: [name]",
			wantType: "yaml",
		},
		{
			name: "full cpd yaml",
			input: `_columns: [name, value]
data:
  - ["test", 1]`,
			wantType: "yaml",
		},
		{
			name:     "yaml starting with document separator",
			input:    "---\n_version: 1",
			wantType: "yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			format, data, err := DetectFormat(reader)

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, format)
			assert.Equal(t, tt.input, string(data), "data should be preserved exactly")
		})
	}
}

func TestDetectFormat_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:      "empty input",
			input:     "",
			wantError: true,
		},
		{
			name:      "only whitespace",
			input:     "   \n\t\n   ",
			wantError: true,
		},
		{
			name:      "only comments",
			input:     "// comment\n// another comment\n",
			wantError: true,
		},
		{
			name:      "whitespace and comments only",
			input:     "\n  // comment\n\t\n",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			_, _, err := DetectFormat(reader)

			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDetectFormat_PreservesData(t *testing.T) {
	// Critical test: ensure we can read the data after detection
	// This validates our buffering strategy

	input := `// header comment
{"_version":1}
{"_meta":{"location":"lab1"}}
{"name":"test","value":123}`

	reader := strings.NewReader(input)
	format, data, err := DetectFormat(reader)

	require.NoError(t, err)
	assert.Equal(t, "jsonl", format)

	// Verify ALL data is preserved including comments and whitespace
	assert.Equal(t, input, string(data))

	// Verify we can parse it as JSONL
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	nonCommentLines := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "//") {
			nonCommentLines++
		}
	}
	assert.Equal(t, 3, nonCommentLines, "should have 3 JSON records")
}

func TestDetectFormat_CommentParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantFormat  string
		description string
	}{
		{
			name:        "comment at start of line",
			input:       "// comment\n{\"test\":1}",
			wantFormat:  "jsonl",
			description: "should skip // at line start",
		},
		{
			name:        "// in middle of json string is not a comment",
			input:       "{\"url\":\"http://example.com\"}",
			wantFormat:  "jsonl",
			description: "// inside JSON string should not be treated as comment",
		},
		{
			name:        "multiple // comments",
			input:       "// comment 1\n// comment 2\n// comment 3\n_columns: [name]",
			wantFormat:  "yaml",
			description: "should skip multiple comment lines",
		},
		{
			name:        "comment with no space after //",
			input:       "//comment\n{\"test\":1}",
			wantFormat:  "jsonl",
			description: "should handle // without space",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			format, _, err := DetectFormat(reader)

			require.NoError(t, err, tt.description)
			assert.Equal(t, tt.wantFormat, format, tt.description)
		})
	}
}
