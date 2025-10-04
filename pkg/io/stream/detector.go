package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

// DetectFormat reads from an input stream, detects whether it's JSONL or YAML,
// and returns the format along with the complete buffered data.
//
// Algorithm:
// 1. Skip leading whitespace
// 2. Skip // comments (line-by-line)
// 3. First non-comment, non-whitespace character determines format:
//    - '{' → JSONL
//    - anything else → YAML
//
// The entire input is buffered and returned so it can be processed after detection.
func DetectFormat(reader io.Reader) (format string, data []byte, err error) {
	// Buffer entire input
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, reader); err != nil {
		return "", nil, fmt.Errorf("failed to read input: %w", err)
	}

	data = buf.Bytes()

	// Find first real character
	firstChar, err := findFirstRealCharacter(bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}

	// Determine format based on first character
	if firstChar == '{' {
		return "jsonl", data, nil
	}

	return "yaml", data, nil
}

// findFirstRealCharacter scans input, skipping whitespace and // comments,
// and returns the first meaningful character
func findFirstRealCharacter(reader io.Reader) (byte, error) {
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()

		// Process character by character to handle mixed whitespace and content
		for i := 0; i < len(line); i++ {
			ch := line[i]

			// Skip whitespace
			if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
				continue
			}

			// Check for comment start
			if ch == '/' && i+1 < len(line) && line[i+1] == '/' {
				// Rest of line is comment, break to next line
				break
			}

			// Found first real character
			return ch, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning input: %w", err)
	}

	return 0, fmt.Errorf("no content found in input")
}
