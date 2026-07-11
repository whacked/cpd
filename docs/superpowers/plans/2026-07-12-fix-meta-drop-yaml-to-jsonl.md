# Fix `_meta` Drop in Structured YAML→JSONL Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `cpd file.cpd.yaml` carry document-level `_meta:` onto every emitted JSONL row (as the spec promises), and stop silently truncating multi-document YAML streams to their first document.

**Architecture:** Two surgical fixes in `pkg/codec/cpd.go`. (1) `ParseCPD` — the structured full-YAML parser preferred by `CPDToJSONLUnified` — parses `_columns`, `_version`, `_schemas`, and join tables but was never taught to parse `_meta`, so `doc.Meta` stays empty and `ToJSONL()`'s meta-flattening block never fires. Add `_meta` parsing mirroring the line-oriented sibling parser (`parseNextDocument`, cpd.go:985). (2) `CPDToJSONLUnified` decodes only the first YAML document via `ParseCPD` and returns, dropping subsequent `---` documents; detect a second document up front and route multi-document input through `CPDToJSONL`, which already implements spec-conformant carry-forward (`_meta` deep-merge, `_columns` inheritance, join-table merge).

**Tech Stack:** Go, `gopkg.in/yaml.v3` (as `yaml`), existing `yamlutil.ConvertNodeToOrderedMap` and `orderedmapjson.AnyOrderedMap` helpers. Tests with stock `go test`.

## Global Constraints

- Do NOT commit. Leave all changes in the working tree; the orchestrator reviews and commits.
- The working tree has many unrelated dirty/untracked files. Only touch the files listed in your task.
- All existing tests must keep passing: `go test ./...` from repo root `/Users/alexh/cloudsync/main/devsync/yayamdb`.
- Run `gofmt -l pkg/codec/` after editing; it must print nothing.
- Canonical flattened-meta row shape (already emitted by the working JSONL→JSONL path and by `CPDDocument.ToJSONL()`): `_version` first if present, then dotted `_meta.*` keys in document order, then row columns. Example: `{"_version":1,"_meta.location":"lab1","_meta.device.id":"sensor23","name":"beta-1",...}`.
- `pkg/codec/cpd.go` already imports `bytes`, `io`, `fmt`, `strings`, yaml, `yamlutil`, `orderedmapjson` — no new imports needed.

---

### Task 1: `ParseCPD` parses document-level `_meta`

**Files:**
- Modify: `pkg/codec/cpd.go` (inside `ParseCPD`, after the `_version` block at ~line 191–194, before the `_schemas` block at ~line 196)
- Test: `pkg/codec/meta_propagation_test.go` (create)

**Interfaces:**
- Consumes: existing `ParseCPD(r io.Reader) (*CPDDocument, error)`, `findNodeByKey(node *yaml.Node, key string) *yaml.Node`, `yamlutil.ConvertNodeToOrderedMap(node *yaml.Node, om *orderedmapjson.AnyOrderedMap) error`.
- Produces: `ParseCPD` now populates `doc.Meta` (an `*orderedmapjson.AnyOrderedMap`, already initialized empty at cpd.go:168) from a top-level `_meta:` mapping. No signature changes. Task 2 relies on this so the single-document structured path emits meta.

- [ ] **Step 1: Write the failing tests**

Create `pkg/codec/meta_propagation_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/codec/ -run 'TestParseCPDPopulatesMeta|TestCPDToJSONLUnifiedFlattensDocumentMeta|TestUnifiedMatchesStreamingParserForMeta|TestCPDToJSONLUnifiedEmptyMeta' -v`

Expected: `TestParseCPDPopulatesMeta` FAILS with "ParseCPD() left doc.Meta empty"; `TestCPDToJSONLUnifiedFlattensDocumentMeta` and `TestUnifiedMatchesStreamingParserForMeta` FAIL (rows missing `_meta.*` keys). `TestCPDToJSONLUnifiedEmptyMeta` PASSES (guard).

- [ ] **Step 3: Add `_meta` parsing to `ParseCPD`**

In `pkg/codec/cpd.go`, immediately after the `_version` parsing block:

```go
	// Parse version
	if versionNode := findNodeByKey(root, "_version"); versionNode != nil {
		doc.Version = versionNode.Value
	}
```

insert:

```go
	// Parse meta
	if metaNode := findNodeByKey(root, "_meta"); metaNode != nil {
		if err := yamlutil.ConvertNodeToOrderedMap(metaNode, doc.Meta); err != nil {
			return nil, fmt.Errorf("failed to convert _meta: %w", err)
		}
	}
```

(`doc.Meta` is already initialized to an empty `AnyOrderedMap` in the `doc := &CPDDocument{...}` literal, so converting into it directly is correct. This mirrors `parseNextDocument`'s `_meta` handling minus the prev-document merge, which does not apply to a single document.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/codec/ -run 'TestParseCPDPopulatesMeta|TestCPDToJSONLUnifiedFlattensDocumentMeta|TestUnifiedMatchesStreamingParserForMeta|TestCPDToJSONLUnifiedEmptyMeta' -v`

Expected: all four PASS.

- [ ] **Step 5: Run the full suite and gofmt**

Run: `go test ./...` — expected: all packages pass.
Run: `gofmt -l pkg/codec/` — expected: no output.

Do NOT commit (orchestrator commits after review).

---

### Task 2: `CPDToJSONLUnified` stops truncating multi-document YAML

**Files:**
- Modify: `pkg/codec/cpd.go` (function `CPDToJSONLUnified`, ~lines 756–776)
- Test: `pkg/codec/meta_propagation_test.go` (append; created in Task 1)

**Interfaces:**
- Consumes: `CPDToJSONL(r io.Reader) (string, error)` (line-oriented streaming parser with carry-forward — already correct for multi-document input), `ParseCPD` (with Task 1's `_meta` support), `yaml.NewDecoder`.
- Produces: `CPDToJSONLUnified(r io.Reader) (string, error)` — unchanged signature; multi-document input now emits rows for every document with `_meta`/`_columns`/join-table carry-forward.

- [ ] **Step 1: Write the failing test**

Append to `pkg/codec/meta_propagation_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/codec/ -run TestCPDToJSONLUnifiedMultiDocument -v`

Expected: FAIL — output contains only the first document's row.

- [ ] **Step 3: Route multi-document input through the streaming parser**

Replace the body of `CPDToJSONLUnified` in `pkg/codec/cpd.go`:

```go
// CPDToJSONL converts a CPD YAML file to JSONL format
// CPDToJSONLUnified handles both structured YAML documents and line-by-line format
func CPDToJSONLUnified(r io.Reader) (string, error) {
	// Read all data first so we can try both parsing approaches
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("failed to read input: %w", err)
	}

	// Multi-document streams need carry-forward state (_meta, _columns,
	// join tables) across documents; ParseCPD only reads the first
	// document, so route them through the streaming parser instead.
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var first yaml.Node
	if err := dec.Decode(&first); err == nil {
		var second yaml.Node
		if err := dec.Decode(&second); err != io.EOF {
			return CPDToJSONL(bytes.NewReader(data))
		}
	}

	// First, try parsing as a structured YAML document
	doc, err := ParseCPD(bytes.NewReader(data))
	if err == nil {
		// Skip schema validation for now to focus on mixed format support
		// TODO: Fix schema validation to handle null values properly
		// Successfully parsed as structured YAML, convert to JSONL
		return doc.ToJSONL()
	}

	// If structured parsing failed, fall back to line-by-line parsing
	return CPDToJSONL(bytes.NewReader(data))
}
```

Notes: a non-EOF result from the second `Decode` means there is trailing content beyond the first document (either another document or malformed trailing YAML) — both belong to the streaming parser, which either handles them or reports a real error instead of silently dropping them. A failed first `Decode` falls through to the existing ParseCPD→fallback ordering, preserving current behavior for non-YAML/mixed input. `bytes` and `io` are already imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/codec/ -run TestCPDToJSONLUnifiedMultiDocument -v`

Expected: PASS.

- [ ] **Step 5: Run the full suite and gofmt**

Run: `go test ./...` — expected: all packages pass (roundtrip, idempotency, mixed-format, embed golden-example tests included).
Run: `gofmt -l pkg/codec/` — expected: no output.

Do NOT commit (orchestrator commits after review).
