# CommonPayloadData Specification (`2025.06-declarative`)

A minimal, declarative, human-readable YAML format for row-oriented structured data with optional join tables. Fully schema-driven. Designed for round-trippable JSONL generation, SQLite compatibility, and toolchain simplicity.

---

## 👉 Overview

A CPD document is a YAML mapping with:

* A **header section** (everything before `data:`)
* A required **`_columns:`** list
* Zero or more **join tables** (must match column names)
* A required **`data:`** array of rows
* Support for **multi-document YAML**, separated by `---`

Each row is a positional record whose values must match the columns declared in `_columns:`.

* &#x20;list
* Zero or more **join tables** (must match column names)
* A required **`data:`** array of rows
* Support for **multi-document YAML**, separated by `---`

Each row is a positional record whose values must match the columns declared in `_columns:`.

---

## 📌 Header Fields

### `_columns:` (required)

Defines the column names and their order in each row.

```yaml
_columns: [time, authors, topic, payload]
```

There are **no reserved names**. However, the following conventions carry common interpretation:

| Column Name | Description                  | Expected Format        |
| ----------- | ---------------------------- | ---------------------- |
| `time`      | ISO 8601 timestamp           | string                 |
| `payload`   | Arbitrary structured content | mapping (YAML object)  |
| join name   | Joins into a declared table  | `int`, `[]`, or `null` |

### `_version:` (optional)

Free-form document version tag. Carried into each row.

### `_meta:` (optional)

Static metadata merged into each row. Merged **recursively and additively** across YAML documents.

```yaml
_meta:
  location: lab1
  device:
    id: sensor3
```

### Join Tables (optional)

Any key in the header that:

* Does **not** start with `_`
* Is **not** `data`
* Appears in `_columns`

Is treated as a **join table**, mapping `string → integer`. For example:

```yaml
authors:
  alice: 1
  bob: 2
topic:
  food: 1
  recovery: 2
```

* Join table keys must be bijective (no duplicate keys or values)
* Join tables are **merged by name** across YAML documents

---

## 📄 `data:` Section

An array of rows, each being a YAML flow-style sequence matching `_columns:` exactly.

### Example

```yaml
_columns: [time, authors, topic, payload]
authors:
  alice: 1
  bob: 2
topic:
  food: 1
data:
  - ["2024-06-12T12:00:00Z", [1, 2], 1, {note: "ate natto"}]
  - ["2024-06-13T13:30:00Z", 1, null, {note: "light snack"}]
```

### Per-Field Join Semantics

* A join column (e.g. `authors`) must:

  * Be `int` → one-to-many
  * Be `[]` → many-to-many
  * Be `null` → no join

* All other fields are treated as scalars or mappings depending on context

* `payload` is expected to be a YAML mapping (object)

---

## 🔁 JSONL Representation

Each row is flattened into a JSON object with keys:

* `_version`, `_meta.*` (flattened)
* Join fields → names instead of IDs
* Payload fields → merged into root

### Example Output

```json
{"time":"2024-06-12T12:00:00Z","authors":["alice","bob"],"topic":"food","note":"ate natto"}
```

---

## 🛡️ SQLite Compatibility

All `_columns` become table columns **except** for payload fields, which may be flattened or stored as JSON.

### Example schema:

```sql
CREATE TABLE data (
  id INTEGER PRIMARY KEY,
  time TEXT,
  topic_id INTEGER,
  payload JSON
);

CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE data_authors (data_id INTEGER, author_id INTEGER);
```

Join fields may be expanded into M\:N or 1\:N join tables as needed.

---

## ❗ Validation Rules

* `_columns:` must exist and match `data:` row length
* Any column that shares its name with a join table must:

  * Use only `int`, `[]`, or `null`
  * Reference only declared join table IDs
* Join tables must map unique `string → int`
* `_meta` is recursively and additively merged across documents
* Join table merges must preserve bijection (no key or ID collisions)

