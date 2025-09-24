package yaml

import (
	"strings"
	"testing"

	"github.com/GitRowin/orderedmapjson"
)

func TestYAMLReader_Read(t *testing.T) {
	// TODO: Test basic YAML reading
}

func TestYAMLReader_ReadAll(t *testing.T) {
	// TODO: Test reading all YAML records
}

func TestYAMLReader_Version(t *testing.T) {
	// TODO: Test version handling
}

func TestYAMLReader_Schema(t *testing.T) {
	// TODO: Test schema handling
}

func TestYAMLReader(t *testing.T) {
	// Test multi-document YAML
	input := `
---
name: John
age: 30
address:
  street: 123 Main St
  city: Boston
---
name: Jane
age: 25
address:
  street: 456 Oak Ave
  city: New York
`

	reader := NewReader(strings.NewReader(input))
	docs, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read YAML: %v", err)
	}

	if len(docs) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(docs))
	}

	// Test first document
	doc1 := docs[0]
	if doc1.Data == nil {
		t.Fatal("First document data is nil")
	}

	// Check first document content
	if name, _ := doc1.Data.Get("name"); name != "John" {
		t.Errorf("Expected name 'John', got '%v'", name)
	}
	if age, _ := doc1.Data.Get("age"); age != 30 {
		t.Errorf("Expected age '30', got '%v'", age)
	}

	// Check nested map
	address, _ := doc1.Data.Get("address")
	addrMap, ok := address.(*orderedmapjson.AnyOrderedMap)
	if !ok {
		t.Fatal("Address is not a map")
	}
	if street, _ := addrMap.Get("street"); street != "123 Main St" {
		t.Errorf("Expected street '123 Main St', got '%v'", street)
	}

	// Test second document
	doc2 := docs[1]
	if doc2.Data == nil {
		t.Fatal("Second document data is nil")
	}

	// Check second document content
	if name, _ := doc2.Data.Get("name"); name != "Jane" {
		t.Errorf("Expected name 'Jane', got '%v'", name)
	}
	if age, _ := doc2.Data.Get("age"); age != 25 {
		t.Errorf("Expected age '25', got '%v'", age)
	}

	// Test YAML output
	output := CleanString(doc1.String())
	expected := CleanString(`name: John
age: 30
address:
  street: 123 Main St
  city: Boston
`)
	if output != expected {
		t.Errorf("Expected output:\n%s\nGot:\n%s", expected, output)
	}
}

func TestYAMLReaderWithArrays(t *testing.T) {
	input := `
---
name: Test
items:
  - name: Item 1
    value: 100
  - name: Item 2
    value: 200
  - simple
  - array
`

	reader := NewReader(strings.NewReader(input))
	docs, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read YAML: %v", err)
	}

	if len(docs) != 1 {
		t.Errorf("Expected 1 document, got %d", len(docs))
	}

	doc := docs[0]
	items, _ := doc.Data.Get("items")
	itemsArray, ok := items.([]interface{})
	if !ok {
		t.Fatal("Items is not an array")
	}

	if len(itemsArray) != 4 {
		t.Errorf("Expected 4 items, got %d", len(itemsArray))
	}

	// Check first item (map)
	item1, ok := itemsArray[0].(*orderedmapjson.AnyOrderedMap)
	if !ok {
		t.Fatal("First item is not a map")
	}
	if name, _ := item1.Get("name"); name != "Item 1" {
		t.Errorf("Expected name 'Item 1', got '%v'", name)
	}

	// Check simple items
	if itemsArray[2] != "simple" {
		t.Errorf("Expected 'simple', got '%v'", itemsArray[2])
	}
	if itemsArray[3] != "array" {
		t.Errorf("Expected 'array', got '%v'", itemsArray[3])
	}
}
