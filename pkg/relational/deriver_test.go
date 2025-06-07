package relational

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/whacked/yamdb/pkg/codec"
	jio "github.com/whacked/yamdb/pkg/io/jsonl"
)

func TestTableDeriver(t *testing.T) {
	// Try to open the JSONL file
	file, err := os.Open("../../tests/example-2.jsonl")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Create a JSONL reader
	reader := jio.NewReader(io.NopCloser(file))

	// Create a JSONL processor
	processor := codec.NewJSONLProcessor()

	// Process each record
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		_, err = processor.ProcessRecord(record)
		if err != nil {
			t.Fatalf("Failed to process record: %v", err)
		}
	}

	// Create deriver and process history
	deriver := NewTableDeriver()
	err = deriver.ProcessHistory(processor.RecordHistory)
	if err != nil {
		t.Fatalf("Failed to process history: %v", err)
	}

	// Get field information
	fieldInfo := deriver.GetFieldInfo()

	// Log all detected fields and their categories
	t.Log("Detected fields:")
	for name, info := range fieldInfo {
		t.Logf("\nField: %s", name)
		t.Logf("  Type: %v", info.ElementType)
		t.Logf("  Category: %v", GetCategoryString(info.Category))
		t.Logf("  Unique Values: %d", info.UniqueValues)
		t.Logf("  Total Count: %d", info.TotalRecords)
	}

	// Generate and log DDL
	t.Log("\nSQLite DDL:")
	ddl := GenerateSQLiteDDL(fieldInfo, "data")
	t.Log("\n" + ddl)

	// Generate and log INSERT statements
	t.Log("\nSQLite INSERT statements:")
	inserts, err := GenerateSQLiteInserts(fieldInfo, processor.RecordHistory)
	if err != nil {
		t.Fatalf("Failed to generate INSERT statements: %v", err)
	}
	t.Log("\n" + inserts)

	// Verify specific field categorizations
	expectedCategories := map[string]FieldCategory{
		"tags":   FieldCategoryManyToMany,
		"event":  FieldCategoryOneToMany,
		"status": FieldCategoryOneToMany,
	}

	for field, expectedCategory := range expectedCategories {
		info, exists := fieldInfo[field]
		if !exists {
			t.Errorf("Expected field %s to be detected", field)
			continue
		}

		if info.Category != expectedCategory {
			t.Errorf("Field %s: expected category %v, got %v", field, expectedCategory, info.Category)
		}

		// Verify array status for many-to-many fields
		if expectedCategory == FieldCategoryManyToMany && !info.IsArray {
			t.Errorf("Field %s: expected to be an array field", field)
		}

		// Verify score is reasonable
		if info.TotalRecords < 3 {
			t.Errorf("Field %s: expected total count >= 3, got %d", field, info.TotalRecords)
		}
	}

	// Verify no command fields are included
	for field := range fieldInfo {
		if len(field) > 0 && field[0] == '@' {
			t.Errorf("Command field %s should not be included in field info", field)
		}
	}
}

// unit test ───────────────────────────────────────────────────────────────────
func Test_FieldInfo_FromExample2(t *testing.T) {

	file, err := os.Open("../../tests/example-2.jsonl")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	// Create a JSONL reader
	reader := jio.NewReader(file)

	// Create a JSONL processor
	processor := codec.NewJSONLProcessor()

	// Process each record
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		_, err = processor.ProcessRecord(record)
		if err != nil {
			t.Fatalf("Failed to process record: %v", err)
		}
	}

	// Create deriver and process history
	deriver := NewTableDeriver()

	err = deriver.ProcessHistory(processor.RecordHistory)
	if err != nil {
		t.Fatalf("Failed to process history: %v", err)
	}
	fieldInfo := deriver.GetFieldInfo()

	// Expectations distilled from the console output posted in the prompt.
	want := map[string]struct {
		cat  FieldCategory
		arr  bool
		uniq int
	}{
		"location": {FieldCategoryUnknown, false, 4},
		// cat tests/example-2.jsonl | grep '^{' | jq -c '.tags | select(. != null and length > 0)' | jq -r '.[]' | sort | uniq | wc -l
		"tags":  {FieldCategoryManyToMany, true, 8},
		"event": {FieldCategoryOneToMany, false, 5},
	}

	for field, w := range want {
		got, ok := fieldInfo[field]

		if !ok {
			t.Errorf("field %q missing in FieldInfo", field)
			continue
		}
		fmt.Printf("field: %q, got: %+v\n", field, got)
		if got.Category != w.cat {
			t.Errorf("%s: Category = %v, want %v",
				field, GetCategoryString(got.Category), GetCategoryString(w.cat))
		}
		if got.IsArray != w.arr {
			t.Errorf("FIXME %s: IsArray = %v, want %v", field, got.IsArray, w.arr)
		}
		if got.UniqueValues != w.uniq {
			t.Errorf("%s: UniqueValues = %d, want %d", field, got.UniqueValues, w.uniq)
		}
	}
}
