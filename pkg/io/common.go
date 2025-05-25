package io

import (
    "io"
    "github.com/whacked/yamdb/pkg/types"
)

// Reader defines the interface for reading data from any supported format
type Reader interface {
    Read() (types.Record, error)
    ReadAll() ([]types.Record, error)
    Close() error
}

// Writer defines the interface for writing data to any supported format
type Writer interface {
    Write(types.Record) error
    WriteAll([]types.Record) error
    Close() error
}

// FormatReader is a factory interface for creating readers
type FormatReader interface {
    NewReader(r io.Reader, opts ...ReaderOption) (Reader, error)
}

// FormatWriter is a factory interface for creating writers
type FormatWriter interface {
    NewWriter(w io.Writer, opts ...WriterOption) (Writer, error)
}

// ReaderOption configures a reader
type ReaderOption func(interface{}) error

// WriterOption configures a writer
type WriterOption func(interface{}) error
