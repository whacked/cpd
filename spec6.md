# CommonPayloadData Specification (`2024.06-declarative`)

A minimal, declarative, human-readable YAML format for row-oriented structured data with optional join tables. Fully schema-driven. Designed for round-trippable JSONL generation, SQLite compatibility, and toolchain simplicity.

---

## 👉 Overview

A CPD document is a YAML mapping with:

* A **header section** (everything before `data:`)
* A required **`_columns:`** list
* Zero or more **join tables** (must match column names)
* A required **`data:`** array of rows
* Support for **multi-document YAML**, separated by `---`

Each row is a positional record whose values are zipped with `_columns:`. Rows may contain fewer items than `_columns` (trailing columns are omitted). Rows **must not contain more items than columns**.

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

An array of rows, each being a YAML flow-style sequence zipped with `_columns:`.

* Rows may have fewer elements than `_columns:` → trailing columns are skipped
* Rows **must not have more elements than `_columns:`**

---

## 🧠 Field Promotion Strategy

When transforming JSONL into CPD:

### Promote to top-level `_columns:` when:

* The field is always a scalar
* The field appears in most rows
* The field is semantically meaningful or supports indexing
* The field contains repeated categorical values (considered for join table)

### Promote to a **join table** if:

* The field is scalar (usually a string) and
* The value space is small and repeated (e.g. "activity", "device X")
* The values are safe to encode bijectively into `string → int`

### Preserve in **`payload`** if:

* The field is optional, sparse, or freeform
* The field is a nested object or array not meant for join resolution
* The field has heterogeneous or unknown shape
* The field’s name is **not** in `_columns`

### `payload` field:

* `payload` is a special field that captures **residual fields** not otherwise lifted into `_columns`
* Must be a mapping object
* Enables round-trippable and extensible JSONL reconstruction

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

## ⚠️ Determinism and Ordering

* JSON object serialization **must preserve key order** when round-tripping if equality is tested

---

## ❗ Validation Rules

* `_columns:` must exist
* Each row in `data:` must have **at most** the number of entries in `_columns:`
* Any column that shares its name with a join table must:

  * Use only `int`, `[]`, or `null`
  * Reference only declared join table IDs
* Join tables must map unique `string → int`
* `_meta` is recursively and additively merged across documents
* Join table merges must preserve bijection (no key or ID collisions)
* The `payload` column (if present) must be a YAML mapping (JSON object)

