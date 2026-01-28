package parquet

import (
	"fmt"
	"sort"

	"github.com/GitRowin/orderedmapjson"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

// FieldInfo tracks metadata about each field for schema inference
type FieldInfo struct {
	Name        string
	HasNull     bool
	HasString   bool
	HasFloat    bool
	HasInt      bool
	HasBool     bool
	HasArray    bool
	ArrayValues []interface{} // Sample array values for element type inference
}

// InferSchema analyzes records and infers Parquet schema
func InferSchema(records []*orderedmapjson.AnyOrderedMap) (*arrow.Schema, []string, error) {
	fieldMap := make(map[string]*FieldInfo)
	fieldOrder := []string{}

	// First pass: collect all fields and their types
	for _, record := range records {
		for el := record.Front(); el != nil; el = el.Next() {
			key := el.Key
			val := el.Value

			if _, exists := fieldMap[key]; !exists {
				fieldMap[key] = &FieldInfo{Name: key}
				fieldOrder = append(fieldOrder, key)
			}

			info := fieldMap[key]
			if val == nil {
				info.HasNull = true
				continue
			}

			switch v := val.(type) {
			case string:
				info.HasString = true
			case float64:
				info.HasFloat = true
			case int, int64:
				info.HasInt = true
			case bool:
				info.HasBool = true
			case []interface{}:
				info.HasArray = true
				if len(v) > 0 && len(info.ArrayValues) < 10 {
					// Collect sample array values for element type inference
					info.ArrayValues = append(info.ArrayValues, v...)
				}
			case []string:
				info.HasArray = true
				if len(v) > 0 && len(info.ArrayValues) < 10 {
					for _, s := range v {
						info.ArrayValues = append(info.ArrayValues, s)
					}
				}
			default:
				// Fallback: treat as string
				info.HasString = true
			}
		}
	}

	// Build Arrow schema
	fields := make([]arrow.Field, 0, len(fieldOrder))
	for _, fieldName := range fieldOrder {
		info := fieldMap[fieldName]
		nullable := info.HasNull

		var dataType arrow.DataType

		if info.HasArray {
			// Infer array element type
			elemType := inferArrayElementType(info.ArrayValues)
			dataType = arrow.ListOf(elemType)
		} else if info.HasBool {
			dataType = arrow.FixedWidthTypes.Boolean
		} else if info.HasInt && !info.HasFloat {
			dataType = arrow.PrimitiveTypes.Int64
		} else if info.HasFloat || info.HasInt {
			dataType = arrow.PrimitiveTypes.Float64
		} else {
			// Default to string
			dataType = arrow.BinaryTypes.String
		}

		fields = append(fields, arrow.Field{
			Name:     fieldName,
			Type:     dataType,
			Nullable: nullable,
		})
	}

	schema := arrow.NewSchema(fields, nil)
	return schema, fieldOrder, nil
}

// inferArrayElementType infers the element type from sample array values
func inferArrayElementType(samples []interface{}) arrow.DataType {
	if len(samples) == 0 {
		return arrow.BinaryTypes.String // Default to string arrays
	}

	hasString := false
	hasFloat := false
	hasInt := false
	hasBool := false

	for _, sample := range samples {
		switch sample.(type) {
		case string:
			hasString = true
		case float64:
			hasFloat = true
		case int, int64:
			hasInt = true
		case bool:
			hasBool = true
		}
	}

	if hasBool {
		return arrow.FixedWidthTypes.Boolean
	} else if hasInt && !hasFloat && !hasString {
		return arrow.PrimitiveTypes.Int64
	} else if (hasFloat || hasInt) && !hasString {
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
}

// WriteRecordsToParquet writes ordered map records to Parquet format
func WriteRecordsToParquet(records []*orderedmapjson.AnyOrderedMap) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to write")
	}

	// Infer schema
	schema, fieldOrder, err := InferSchema(records)
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	mem := memory.NewGoAllocator()

	// Create Arrow record builder
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Build each field
	for rowIdx, record := range records {
		for colIdx, fieldName := range fieldOrder {
			fieldBuilder := builder.Field(colIdx)
			val := getFieldValue(record, fieldName)

			if err := appendValue(fieldBuilder, val, schema.Field(colIdx).Type); err != nil {
				return nil, fmt.Errorf("failed to append value at row %d, col %s: %w", rowIdx, fieldName, err)
			}
		}
	}

	// Create Arrow record
	rec := builder.NewRecord()
	defer rec.Release()

	// Write to Parquet bytes using in-memory buffer
	var buf bytesBuffer
	parquetProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(
		rec.Schema(),
		&buf,
		parquetProps,
		arrowProps,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(rec); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// getFieldValue safely retrieves a field value from an ordered map
func getFieldValue(record *orderedmapjson.AnyOrderedMap, fieldName string) interface{} {
	for el := record.Front(); el != nil; el = el.Next() {
		if el.Key == fieldName {
			return el.Value
		}
	}
	return nil
}

// appendValue appends a value to an Arrow array builder
func appendValue(builder array.Builder, val interface{}, dataType arrow.DataType) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		if s, ok := val.(string); ok {
			b.Append(s)
		} else {
			b.Append(fmt.Sprintf("%v", val))
		}

	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case int:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		default:
			return fmt.Errorf("cannot convert %T to float64", val)
		}

	case *array.Int64Builder:
		switch v := val.(type) {
		case int:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		case float64:
			b.Append(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", val)
		}

	case *array.BooleanBuilder:
		if bVal, ok := val.(bool); ok {
			b.Append(bVal)
		} else {
			return fmt.Errorf("cannot convert %T to bool", val)
		}

	case *array.ListBuilder:
		return appendArrayValue(b, val, dataType)

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// appendArrayValue appends an array value to a list builder
func appendArrayValue(listBuilder *array.ListBuilder, val interface{}, dataType arrow.DataType) error {
	listBuilder.Append(true)
	valueBuilder := listBuilder.ValueBuilder()

	switch arr := val.(type) {
	case []interface{}:
		for _, item := range arr {
			if err := appendValue(valueBuilder, item, dataType.(*arrow.ListType).Elem()); err != nil {
				return err
			}
		}
	case []string:
		strBuilder, ok := valueBuilder.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("expected string builder for []string")
		}
		for _, item := range arr {
			strBuilder.Append(item)
		}
	case []float64:
		floatBuilder, ok := valueBuilder.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf("expected float64 builder for []float64")
		}
		for _, item := range arr {
			floatBuilder.Append(item)
		}
	case []int:
		intBuilder, ok := valueBuilder.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("expected int64 builder for []int")
		}
		for _, item := range arr {
			intBuilder.Append(int64(item))
		}
	case []bool:
		boolBuilder, ok := valueBuilder.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf("expected boolean builder for []bool")
		}
		for _, item := range arr {
			boolBuilder.Append(item)
		}
	default:
		return fmt.Errorf("unsupported array type: %T", val)
	}

	return nil
}

// bytesBuffer implements io.Writer for in-memory Parquet writing
type bytesBuffer struct {
	data []byte
}

func (b *bytesBuffer) Write(p []byte) (n int, err error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *bytesBuffer) Bytes() []byte {
	return b.data
}

// RecordsToParquetWithSchema converts records to Parquet with explicit column ordering
func RecordsToParquetWithSchema(records []*orderedmapjson.AnyOrderedMap, columns []string) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to write")
	}

	// If columns not provided, collect from records in stable order
	if columns == nil || len(columns) == 0 {
		columnSet := make(map[string]bool)
		for _, record := range records {
			for el := record.Front(); el != nil; el = el.Next() {
				if !columnSet[el.Key] {
					columns = append(columns, el.Key)
					columnSet[el.Key] = true
				}
			}
		}
		sort.Strings(columns)
	}

	// Build field info map with explicit ordering
	fieldMap := make(map[string]*FieldInfo)
	for _, col := range columns {
		fieldMap[col] = &FieldInfo{Name: col}
	}

	// Analyze types
	for _, record := range records {
		for el := record.Front(); el != nil; el = el.Next() {
			info, exists := fieldMap[el.Key]
			if !exists {
				continue // Skip fields not in column list
			}

			val := el.Value
			if val == nil {
				info.HasNull = true
				continue
			}

			switch v := val.(type) {
			case string:
				info.HasString = true
			case float64:
				info.HasFloat = true
			case int, int64:
				info.HasInt = true
			case bool:
				info.HasBool = true
			case []interface{}:
				info.HasArray = true
				if len(v) > 0 && len(info.ArrayValues) < 10 {
					info.ArrayValues = append(info.ArrayValues, v...)
				}
			case []string:
				info.HasArray = true
				if len(v) > 0 && len(info.ArrayValues) < 10 {
					for _, s := range v {
						info.ArrayValues = append(info.ArrayValues, s)
					}
				}
			}
		}
	}

	// Build schema with explicit column order
	fields := make([]arrow.Field, 0, len(columns))
	for _, col := range columns {
		info := fieldMap[col]
		nullable := info.HasNull || !info.HasString && !info.HasFloat && !info.HasInt && !info.HasBool && !info.HasArray

		var dataType arrow.DataType
		if info.HasArray {
			elemType := inferArrayElementType(info.ArrayValues)
			dataType = arrow.ListOf(elemType)
		} else if info.HasBool {
			dataType = arrow.FixedWidthTypes.Boolean
		} else if info.HasInt && !info.HasFloat {
			dataType = arrow.PrimitiveTypes.Int64
		} else if info.HasFloat || info.HasInt {
			dataType = arrow.PrimitiveTypes.Float64
		} else {
			dataType = arrow.BinaryTypes.String
		}

		fields = append(fields, arrow.Field{
			Name:     col,
			Type:     dataType,
			Nullable: nullable,
		})
	}

	schema := arrow.NewSchema(fields, nil)
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Build each row
	for _, record := range records {
		for colIdx, col := range columns {
			val := getFieldValue(record, col)
			fieldBuilder := builder.Field(colIdx)
			if err := appendValue(fieldBuilder, val, schema.Field(colIdx).Type); err != nil {
				return nil, fmt.Errorf("failed to append value for col %s: %w", col, err)
			}
		}
	}

	rec := builder.NewRecord()
	defer rec.Release()

	// Write to Parquet using in-memory buffer
	var buf bytesBuffer
	parquetProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(
		rec.Schema(),
		&buf,
		parquetProps,
		arrowProps,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	if err := writer.Write(rec); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}
