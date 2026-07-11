package codec

import (
	"strings"
	"testing"
)

// Document-level _meta on a structured single-document CPD YAML file.
// Spec (spec5.md): "_meta is flattened and merged into each row" on export.
const singleDocMetaYAML = `_meta:
  source: lab1
  device:
    id: xyz
_columns: [time, note]
data:
  - ["2024-06-12T12:00:00Z", "ok"]
  - ["2024-06-12T12:01:00Z", "fine"]
`

func TestParseCPDPopulatesMeta(t *testing.T) {
	doc, err := ParseCPD(strings.NewReader(singleDocMetaYAML))
	if err != nil {
		t.Fatalf("ParseCPD() error = %v", err)
	}
	if doc.Meta == nil || doc.Meta.Len() == 0 {
		t.Fatal("ParseCPD() left doc.Meta empty; document-level _meta was dropped")
	}
	source, ok := doc.Meta.Get("source")
	if !ok {
		t.Fatal("doc.Meta missing key \"source\"")
	}
	if source != "lab1" {
		t.Errorf("doc.Meta[source] = %v, want \"lab1\"", source)
	}
}

func TestCPDToJSONLUnifiedFlattensDocumentMeta(t *testing.T) {
	got, err := CPDToJSONLUnified(strings.NewReader(singleDocMetaYAML))
	if err != nil {
		t.Fatalf("CPDToJSONLUnified() error = %v", err)
	}
	want := `{"_meta.source":"lab1","_meta.device.id":"xyz","time":"2024-06-12T12:00:00Z","note":"ok"}
{"_meta.source":"lab1","_meta.device.id":"xyz","time":"2024-06-12T12:01:00Z","note":"fine"}
`
	if got != want {
		t.Errorf("CPDToJSONLUnified() =\n%s\nwant:\n%s", got, want)
	}
}

// The structured path and the line-oriented streaming path must agree.
func TestUnifiedMatchesStreamingParserForMeta(t *testing.T) {
	unified, err := CPDToJSONLUnified(strings.NewReader(singleDocMetaYAML))
	if err != nil {
		t.Fatalf("CPDToJSONLUnified() error = %v", err)
	}
	streaming, err := CPDToJSONL(strings.NewReader(singleDocMetaYAML))
	if err != nil {
		t.Fatalf("CPDToJSONL() error = %v", err)
	}
	if unified != streaming {
		t.Errorf("parser disagreement:\nunified:\n%s\nstreaming:\n%s", unified, streaming)
	}
}

// Empty _meta mapping must not emit any _meta.* keys (regression guard).
func TestCPDToJSONLUnifiedEmptyMeta(t *testing.T) {
	input := `_meta: {}
_columns: [time, note]
data:
  - ["2024-06-12T12:00:00Z", "ok"]
`
	got, err := CPDToJSONLUnified(strings.NewReader(input))
	if err != nil {
		t.Fatalf("CPDToJSONLUnified() error = %v", err)
	}
	want := `{"time":"2024-06-12T12:00:00Z","note":"ok"}
`
	if got != want {
		t.Errorf("CPDToJSONLUnified() = %q, want %q", got, want)
	}
}

// Multi-document YAML: spec (spec5.md) says _meta is recursively merged
// across documents, and _columns may change between documents. The unified
// converter previously parsed only the first document and silently dropped
// the rest.
func TestCPDToJSONLUnifiedMultiDocument(t *testing.T) {
	input := `_meta:
  source: lab1
_columns: [time, note]
data:
  - ["2024-06-12T12:00:00Z", "ok"]
---
_meta:
  run: 2
data:
  - ["2024-06-12T12:05:00Z", "second doc"]
`
	got, err := CPDToJSONLUnified(strings.NewReader(input))
	if err != nil {
		t.Fatalf("CPDToJSONLUnified() error = %v", err)
	}
	want := `{"_meta.source":"lab1","time":"2024-06-12T12:00:00Z","note":"ok"}
{"_meta.source":"lab1","_meta.run":2,"time":"2024-06-12T12:05:00Z","note":"second doc"}
`
	if got != want {
		t.Errorf("CPDToJSONLUnified() =\n%s\nwant:\n%s", got, want)
	}
}
