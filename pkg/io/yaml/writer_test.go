package yaml

import (
	"bytes"
	"testing"

	"github.com/GitRowin/orderedmapjson"
)

func TestWriter(t *testing.T) {
	// Create a test document
	doc := &Document{
		Data: orderedmapjson.NewAnyOrderedMap(),
	}

	// Add some metadata
	meta := orderedmapjson.NewAnyOrderedMap()
	meta.Set("location", "lab1")
	meta.Set("device", "sensor23")
	doc.Data.Set("_meta", meta)

	// Add version
	doc.Data.Set("_version", 1)

	// Add data array
	data := []interface{}{
		createDataItem("beta-1", 28.4, "warn"),
		createDataItem("beta-2", 29.1, "ok"),
		createDataItem("beta 3", 27.1, "ok"),
	}
	doc.Data.Set("data", data)

	// Test unquoted output
	var buf bytes.Buffer
	writer := NewWriter(&buf)
	if err := writer.Write(doc); err != nil {
		t.Fatalf("Failed to write document: %v", err)
	}

	expected := CleanString(`_meta:
  location: lab1
  device: sensor23
_version: 1
data:
  - {name: beta-1, temperature: 28.4, status: warn}
  - {name: beta-2, temperature: 29.1, status: ok}
  - {name: "beta 3", temperature: 27.1, status: ok}
`)
	received := CleanString(buf.String())

	if received != expected {
		t.Errorf("\nExpected:\n^%s$\nGot:\n^%s$", expected, received)
		// Do byte-by-byte comparison
		actual := received
		for i := 0; i < len(expected) && i < len(actual); i++ {
			if expected[i] != actual[i] {
				t.Errorf("First difference at position %d:\nexpected '%c' (byte: %d), got '%c' (byte: %d)",
					i, expected[i], expected[i], actual[i], actual[i])
				break
			}
		}

		// Handle case where strings are different lengths
		if len(expected) != len(actual) {
			t.Errorf("Strings have different lengths: expected %d, got %d", len(expected), len(actual))
		}
	}

	// Test quoted output
	buf.Reset()
	writer = NewWriter(&buf, WriterOptions{QuoteStrings: true})
	if err := writer.Write(doc); err != nil {
		t.Fatalf("Failed to write document: %v", err)
	}

	expected = CleanString(`_meta:
  location: "lab1"
  device: "sensor23"
_version: 1
data:
  - {"name": "beta-1", "temperature": 28.4, "status": "warn"}
  - {"name": "beta-2", "temperature": 29.1, "status": "ok"}
  - {"name": "beta 3", "temperature": 27.1, "status": "ok"}
`)

	received = CleanString(buf.String())

	if received != expected {
		t.Errorf("\nExpected:\n^%s$\nGot:\n^%s$", expected, received)

		// Print byte lengths
		t.Logf("Expected string length: %d bytes", len(expected))
		t.Logf("Actual string length: %d bytes", len(received))
	}
}

func TestWriterWithNestedData(t *testing.T) {
	// Create a test document with nested data
	doc := &Document{
		Data: orderedmapjson.NewAnyOrderedMap(),
	}

	// Add data array with nested objects
	data := []interface{}{
		createNestedDataItemOrdered("item1", func(details *orderedmapjson.AnyOrderedMap) {
			details.Set("id", 1)
			details.Set("tags", []string{"tag1", "tag2"})
		}),
		createNestedDataItemOrdered("item2", func(details *orderedmapjson.AnyOrderedMap) {
			details.Set("id", 2)
			details.Set("tags", []string{"tag3", "tag4"})
		}),
	}
	doc.Data.Set("data", data)

	// Test unquoted output
	var buf bytes.Buffer
	writer := NewWriter(&buf)
	if err := writer.Write(doc); err != nil {
		t.Fatalf("Failed to write document: %v", err)
	}

	expected := `data:
  - {name: item1, details: {id: 1, tags: [tag1, tag2]}}
  - {name: item2, details: {id: 2, tags: [tag3, tag4]}}
`

	if buf.String() != expected {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, buf.String())
	}
}

// Helper functions

func createDataItem(name string, temp float64, status string) *orderedmapjson.AnyOrderedMap {
	item := orderedmapjson.NewAnyOrderedMap()
	item.Set("name", name)
	item.Set("temperature", temp)
	item.Set("status", status)
	return item
}

func createNestedDataItemOrdered(name string, setupDetails func(*orderedmapjson.AnyOrderedMap)) *orderedmapjson.AnyOrderedMap {
	item := orderedmapjson.NewAnyOrderedMap()
	item.Set("name", name)

	detailsMap := orderedmapjson.NewAnyOrderedMap()
	setupDetails(detailsMap)
	item.Set("details", detailsMap)

	return item
}

// Keep the old function for backward compatibility
func createNestedDataItem(name string, details map[string]interface{}) *orderedmapjson.AnyOrderedMap {
	item := orderedmapjson.NewAnyOrderedMap()
	item.Set("name", name)

	detailsMap := orderedmapjson.NewAnyOrderedMap()
	for k, v := range details {
		detailsMap.Set(k, v)
	}
	item.Set("details", detailsMap)

	return item
}
