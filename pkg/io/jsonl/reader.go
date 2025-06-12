package jsonl

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/GitRowin/orderedmapjson"
	"github.com/whacked/yamdb/pkg/io/yamlutil"
	"gopkg.in/yaml.v3"
)

// Reader provides a way to read JSONL records from an io.Reader
type Reader interface {
	Read() (*orderedmapjson.AnyOrderedMap, error)
	ReadAll() ([]*orderedmapjson.AnyOrderedMap, error)
	Close() error
}

// jsonlReader implements the Reader interface
type jsonlReader struct {
	scanner *bufio.Scanner
	closer  io.Closer
}

// NewReader creates a new JSONL reader
func NewReader(r io.ReadCloser) Reader {
	return &jsonlReader{
		scanner: bufio.NewScanner(r),
		closer:  r,
	}
}

// Read implements our custom record reader interface
func (r *jsonlReader) Read() (*orderedmapjson.AnyOrderedMap, error) {
	// Read next data line
	for r.scanner.Scan() {
		line := r.scanner.Text()

		// Trim whitespace
		line = strings.TrimSpace(line)

		// Skip empty lines and comment-only lines
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}

		// Trim out trailing comments
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}

		// Parse into YAML node to preserve order
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(line), &node); err != nil {
			return nil, fmt.Errorf("failed to decode record: %w (line: %q)", err, line)
		}

		// Convert to ordered map
		orderedRecord := orderedmapjson.NewAnyOrderedMap()
		if err := yamlutil.ConvertNodeToOrderedMap(&node, orderedRecord); err != nil {
			return nil, fmt.Errorf("failed to convert record to ordered map: %w", err)
		}

		return orderedRecord, nil
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// ReadAll reads all records from the reader
func (r *jsonlReader) ReadAll() ([]*orderedmapjson.AnyOrderedMap, error) {
	var records []*orderedmapjson.AnyOrderedMap
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// skip bad lines, but log or collect errors if needed
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

// Close implements io.Closer
func (r *jsonlReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
