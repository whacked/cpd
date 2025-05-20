package jsonl

import (
    "encoding/json"
    "github.com/yourusername/yamdb/pkg/types"
    "github.com/yourusername/yamdb/pkg/io"
)

type jsonlReader struct {
    decoder *json.Decoder
    schema  *types.Schema
}

func NewReader(r io.Reader, opts ...io.ReaderOption) (io.Reader, error) {
    decoder := json.NewDecoder(r)
    
    // First read the version
    var version struct {
        Version int `json:"_version"`
    }
    if err := decoder.Decode(&version); err != nil {
        return nil, fmt.Errorf("failed to read version: %w", err)
    }
    
    // Then read the schema
    var schema struct {
        Schema *types.Schema `json:"_schema"`
    }
    if err := decoder.Decode(&schema); err != nil {
        return nil, fmt.Errorf("failed to read schema: %w", err)
    }
    
    return &jsonlReader{
        decoder: decoder,
        schema:  schema.Schema,
    }, nil
}

func (r *jsonlReader) Read() (types.Record, error) {
    var record types.Record
    if err := r.decoder.Decode(&record); err != nil {
        if err == io.EOF {
            return nil, err
        }
        return nil, fmt.Errorf("failed to decode record: %w", err)
    }
    return record, nil
}

func (r *jsonlReader) ReadAll() ([]types.Record, error) {
    var records []types.Record
    for {
        record, err := r.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        records = append(records, record)
    }
    return records, nil
}

func (r *jsonlReader) Close() error {
    return nil // JSON decoder doesn't need explicit cleanup
}
