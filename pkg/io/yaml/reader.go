package yaml

import (
    "gopkg.in/yaml.v3"
    "github.com/yourusername/yamdb/pkg/types"
    "github.com/yourusername/yamdb/pkg/io"
)

type yamlReader struct {
    decoder *yaml.Decoder
    schema  *types.Schema
    version int
}

func NewReader(r io.Reader, opts ...io.ReaderOption) (io.Reader, error) {
    decoder := yaml.NewDecoder(r)
    
    // First read the version
    var version struct {
        Version int `yaml:"_version"`
    }
    if err := decoder.Decode(&version); err != nil {
        return nil, fmt.Errorf("failed to read version: %w", err)
    }
    
    // Then read the schema
    var schema struct {
        Schemas map[string]*types.Schema `yaml:"_schemas"`
    }
    if err := decoder.Decode(&schema); err != nil {
        return nil, fmt.Errorf("failed to read schema: %w", err)
    }
    
    return &yamlReader{
        decoder: decoder,
        schema:  schema.Schemas["data"], // Assuming "data" is the main table
        version: version.Version,
    }, nil
}

func (r *yamlReader) Read() (types.Record, error) {
    var record types.Record
    if err := r.decoder.Decode(&record); err != nil {
        if err == io.EOF {
            return nil, err
        }
        return nil, fmt.Errorf("failed to decode record: %w", err)
    }
    return record, nil
}

func (r *yamlReader) ReadAll() ([]types.Record, error) {
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

func (r *yamlReader) Close() error {
    return nil // YAML decoder doesn't need explicit cleanup
}
