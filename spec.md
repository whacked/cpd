# yamdb: Plaintext-Friendly, Schema-Aware Tabular Data System

## Overview

**yamdb** is a lightweight, human-editable, diff-friendly tabular data format backed by YAML or JSONL. It supports optional schemas, field codecs, and seamless interop with SQLite, JSONL, and delimited formats. The system is optimized for appending records, validating structure, and exporting for richer downstream usage.

It is designed for:

* Developers and data practitioners maintaining small but structured datasets
* Use cases where human-diffable version control is desirable
* Systems that require export to SQLite or JSONL for analytics or integration

---

## Goals

1. **Human-friendly format**: easy to edit, append, and diff in text editors or Git.
2. **Schema-driven**: uses JSON Schema for field/type validation.
3. **Codec-aware**: support transformation pipelines (e.g. trimming, parsing units).
4. **Extensible**: support multiple tables, field stats, key relationships.
5. **Portable**: bidirectional I/O with SQLite, JSONL, CSV/TSV/XSV.

---

## Data Format

### YAML Mode

```yaml
_version: 1

_schemas:
  data:
    type: array
    items:
      type: object
      required: ["field1"]
      properties:
        field1: {type: string}
        field2: {type: integer}

_keys:
  data: ["field1"]

_codecs:
  data:
    field1: toLowerCase
    field2: toNumber

data:
  - {field1: "Foo", field2: "123"}
  - ["Bar", 456]
```

### JSONL Mode

```json
{"_version": 1}
{"_schema": { ... }}
{"field1": "Foo", "field2": 123}
{"field1": "Bar", "field2": 456}
```

---

## Reserved Sections (YAML mode)

* `_version`: file format version
* `_schemas`: maps table names to JSON Schema objects
* `_keys`: primary/foreign key fields per table
* `_codecs`: maps per-table field names to codec names
* `_meta`: optional metadata (creator, created\_at, etc.)

## Reserved Keys (JSONL mode)

* `_version`: version declaration
* `_schema`: in-stream schema update
* `_codec`: in-stream codec update
* `_meta`: optional metadata block

---

## Codec System

Each codec is a named function applied to a field value.
Example built-ins:

* `toLowerCase`, `toUpperCase`, `toCapitalCase`
* `trim`, `toNumber`, `parseUnit`

Codecs apply before validation. They are declared per-field, per-table in `_codecs`.

---

## CLI Commands

### Metadata

* `yamdb validate <file>` — validate format and schema
* `yamdb lint <file>` — find common issues (missing fields, sparse fields)
* `yamdb summarize <file>` — print table/field stats

### Format I/O

* `yamdb to-sqlite <file> -o db.sqlite`
* `yamdb from-sqlite db.sqlite -o data.yaml`
* `yamdb to-jsonl <file>`
* `yamdb from-jsonl <stream>`
* `yamdb to-delimited <file> --format csv|tsv`

### Dev Tools

* `yamdb promote-fields` — suggest adding undeclared fields to `_schemas`
* `yamdb audit-codecs` — preview codec transformations
* `yamdb freeze-schema` — lock current schema + field order

---

## Type System

Based on JSON Schema Draft 7, with support for:

* `string`, `number`, `integer`, `boolean`, `null`
* `pattern`, `enum`, `minLength`, etc.
* `additionalProperties: false` by default

Custom constraints or DSLs (like enum promotion, field ranking) may be supported in extensions.

---

## Row Modeling

Each row may be:

* an **object** (`map[string]interface{}`)
* a **positional list** (`[]interface{}`), ordered per inferred or locked schema

Conversion between the two is governed by declared field order.

---

## SQLite Mapping

* Each YAML table becomes one SQLite table
* Fields are typed per schema (falling back to `TEXT`)
* `_keys` are used to define primary keys and foreign key constraints
* Codecs are not stored in SQLite — only their output

---

## Future Considerations

* Version migration tooling
* Computed/virtual fields
* Field-level history (audit log)
* WASM UI for inline editing
* Git merge driver / 3-way diff based on `_keys`

---

## Implementation Notes (Go)

### Key Libraries

* `gopkg.in/yaml.v3` for YAML parsing
* `encoding/json` for JSONL
* &#x9;github.com/santhosh-tekuri/jsonschema/v5 for schema validation
* `github.com/mattn/go-sqlite3` for SQLite I/O

### Data Structures

* `Record` = `map[string]interface{}`
* `Row` = `Record` or `[]interface{}`
* `Table` = `[]Row`
* `SchemaMap` = `map[string]*gojsonschema.Schema`
* `CodecRegistry` = `map[string]func(interface{}) (interface{}, error)`

### Architecture

* Core modules: parser, validator, codec engine, exporter
* CLI layer wraps around these as commands
* Test fixtures with real YAML and JSONL examples

