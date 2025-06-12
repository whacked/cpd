# ✅ yamdb: Ingestion & Canonical Storage Specification

## 🎯 Goal

Implement a toolchain that ingests raw JSONL and converts it into a YAML-based storage format (the "yamdb" strategy) with the following properties:

- ✅ Default mode is nearly lossless (ignores whitespace/comments only)
- ✅ Reversibility is testable: input JSONL can be reconstructed from YAML if the YAML follows the yamdb spec
- ✅ Metadata, schema, and enum tables are inferred or declared for further compaction/export
- ✅ Audit-friendliness is prioritized: all transformations must be deterministic and explainable

---

## 🛠️ Components to Implement / Complete

### 1. Ingestion Pipeline (`from-jsonl`)

#### Requirements:

- [ ] Accept JSONL input line-by-line and parse each line into `map[string]interface{}`.
- [ ] Wrap parsed objects into a YAML list under the `data:` key.
- [ ] Ensure `data:` is the final key in the YAML file to enable trivial appending.
- [ ] Recognize and apply `_meta` propagation rules:
  - If a line has a top-level `_meta` key, it replaces or merges with the current metadata context.
  - Metadata merges are recursive and forward-propagated.
- [ ] Top-level `_meta:` key in YAML is treated as global context prior to `data:`.
- [ ] The ingested YAML should match the original JSON structure per record without field modification.

---

### 2. Reconstruction Pipeline (`to-jsonl`)

#### Requirements:

Three output modes:

**(1) Raw**

- [ ] Output each record in `data:` as-is (same order, same content).
- [ ] Do not inject or transform `_meta`, `_version`, or joined fields.

**(2) Compacted**

- [ ] Output each row as a positional array.
- [ ] Use `_columns:` to define the field order.
- [ ] Replace enum-like values (if join tables exist) with numeric IDs.
- [ ] Auto-IDs must start at 1 and match SQL-style semantics.
- [ ] Maintain key order integrity with reference to `_columns`.

**(3) Expanded**

- [ ] Output each row with expanded `_version` and flattened `_meta` fields.
- [ ] Flattened `_meta` fields must use dot notation (e.g. `_meta.foo.bar`).
- [ ] Injected fields must precede original keys in each record.
- [ ] Must handle recursive expansion of nested metadata.
- [ ] Expansion style should be customizable, with dot notation as the default.

#### Audit Test:

- [ ] Ensure `cat input.jsonl | yamdb | yamdb to-jsonl` produces JSONL that is structurally identical to the input (modulo whitespace).
- [ ] Validate that key order, values, and structure are preserved.

---

### 3. Schema Derivation (`yamdb derive-schema`)

#### Requirements:

- [ ] Derive the most general valid JSON Schema that accepts all data records.
- [ ] Include field names, types, and `required` keys.
- [ ] Place the schema under `_schemas:`.
- [ ] Extract key order from real records and store it in `_columns`.
- [ ] `_columns` is used for compact mode, not for raw or expanded JSONL output.

---

### 4. Join Table Inference

#### Requirements:

- [ ] Scan all fields for small-value-count strings.
- [ ] Promote these fields into named join tables with auto-IDs starting at 1.
- [ ] Replace field values with their ID in the main dataset.
- [ ] Store mappings as separate top-level keys (e.g., `event:`).

#### Reversibility Test:

- [ ] Ensure that joined fields can be mapped back to their original values.
- [ ] Include tests that invert the join map and reconstruct exact values.

---

### 5. Compaction (columnar)

#### Format:

```yaml
_columns: [event, time]

event:
  on: 1
  off: 2

data:
  - [1, 5]
  - [2, 105]



✅ Constraints

Comments and blank lines in JSONL are discarded as non-semantic.

_meta propagation and merging must be consistent and testable.

    Schema inference must always produce the same result on identical input.

🔍 Deliverables
End-to-End Tests

Round-trip conversion: jsonl → yamdb → jsonl should pass structural equivalence checks.

    Round-trip must succeed with and without join tables or compacted format.

Unit Tests

_meta scoping, merge rules, and carry-forward propagation.

Join table inference: creation, replacement, and reverse mapping.

Columnar compaction: correctness, ordering, and lossless reconstruction.
