package jsonl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	yamdbio "github.com/whacked/yamdb/pkg/io"
	"github.com/whacked/yamdb/pkg/types"
)

type jsonlReader struct {
	scanner *bufio.Scanner
	closer  io.Closer // for closing underlying file if needed
}

func NewReader(r io.Reader, opts ...yamdbio.ReaderOption) (yamdbio.Reader, error) {
	scanner := bufio.NewScanner(r)
	reader := &jsonlReader{
		scanner: scanner,
	}
	// Optionally set r as closer if it implements io.Closer
	if c, ok := r.(io.Closer); ok {
		reader.closer = c
	}
	return reader, nil
}

// Read implements our custom record reader interface
func (r *jsonlReader) Read() (types.Record, error) {
	// Read next data line
	for r.scanner.Scan() {
		line := strings.TrimSpace(r.scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue // skip blank or comment lines
		}
		var record types.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("failed to decode record: %w (line: %q)", err, line)
		}
		return record, nil
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (r *jsonlReader) ReadAll() ([]types.Record, error) {
	var records []types.Record
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

func (r *jsonlReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
