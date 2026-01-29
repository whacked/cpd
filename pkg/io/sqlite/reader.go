package sqlite

import (
    "database/sql"
    "fmt"
    "io"

    _ "github.com/mattn/go-sqlite3"
    yio "github.com/whacked/yamdb/pkg/io"
    "github.com/whacked/yamdb/pkg/types"
)

type sqliteReader struct {
    db     *sql.DB
    schema *types.Schema
    table  string
    rows   *sql.Rows
}

func NewReader(dbPath string, opts ...yio.ReaderOption) (yio.Reader, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open database: %w", err)
    }
    
    return &sqliteReader{
        db: db,
    }, nil
}

func (r *sqliteReader) Read() (types.Record, error) {
    if r.rows == nil {
        rows, err := r.db.Query("SELECT * FROM " + r.table)
        if err != nil {
            return nil, fmt.Errorf("failed to query table: %w", err)
        }
        r.rows = rows
    }
    
    if !r.rows.Next() {
        return nil, io.EOF
    }
    
    columns, err := r.rows.Columns()
    if err != nil {
        return nil, fmt.Errorf("failed to get columns: %w", err)
    }
    
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }
    
    if err := r.rows.Scan(valuePtrs...); err != nil {
        return nil, fmt.Errorf("failed to scan row: %w", err)
    }
    
    record := make(types.Record)
    for i, col := range columns {
        record[col] = values[i]
    }
    
    return record, nil
}

func (r *sqliteReader) ReadAll() ([]types.Record, error) {
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

func (r *sqliteReader) Close() error {
    if r.rows != nil {
        r.rows.Close()
    }
    return r.db.Close()
}
