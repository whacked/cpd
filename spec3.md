🧩 1. Ingestion & Transformation Logic (from-jsonl → .yaml)

Applies to:

    pkg/codec/jsonl.go (ProcessRecord, ProcessRecords, ProcessCategories)

    pkg/io/jsonl/reader.go

    pkg/schema/validator.go, pkg/relational/deriver.go

✅ JSONL Parsing

    Accept JSONL line-by-line (ReadAll()), converting each line to types.Record.

    Handle "_version" and "_meta" keys as reserved:

        _version: updates the context version.

        _meta: merges into current metadata scope (deep merge, forward-propagated).

✅ Metadata Semantics

    Maintain a RecordGroup with active _meta and _version.

    Each record inherits the active context unless overridden.

    Distinct context blocks (where _version or _meta changes) must emit separate YAML documents (---) in to-yaml.

✅ Data Representation

    data: must appear as the last key in a YAML document block.

    Records are listed as:

        full objects ({key: val}) in raw mode.

        compact arrays ([id, val]) in compact mode.

🧪 2. Round-Trip Reversibility

Applies to:

    pkg/codec/yaml_test.go, TestYAMLToJSONLConversion, TestJSONLToYAMLConversion

    pkg/codec/jsonl.go → RecordToJSONL, PrintRecordGroupAsJSONL

✅ Raw mode

    Output records with original key ordering (orderedKeys).

    Exclude _meta, _version, and join-table mappings.

✅ Compacted mode

    Emit _columns: as a list of field names.

    Replace string enums with status: {val: id} join-tables.

    Emit record arrays matching _columns order.

    Join-table values must start at ID 1, SQL-style.

✅ Expanded mode

    Emit _version and _meta.<key> fields in each record.

    Flatten nested metadata with dot notation (_meta.device.id).

    Preserve stable output ordering: _version, then _meta.*, then original fields.

📊 3. Schema Derivation

Applies to:

    pkg/relational/deriver.go, ProcessHistory, GetFieldInfo

    pkg/types/table.go, UpdateColumns, GetCurrentColumns

    asdf.go → recordGroupToJSONSchema

✅ Schema Inference

    Derive the broadest valid JSON Schema that accepts all records.

    Capture:

        field types (nullable if necessary),

        required fields (present in all rows),

        field order (_columns:).

    Place schema under _schemas: in YAML.

    Ensure stable output for identical inputs (determinism required).

🔀 4. Join Table Inference

Applies to:

    pkg/relational/deriver.go → GetJoinTableCandidates

    pkg/schema/supplantation.go → SupplantRecord

✅ Criteria

    Fields with low unique-value count and high reuse ratio are eligible.

    Promote them into top-level key maps (status: {on: 1, off: 2}).

    Replace original field value with its ID in compacted rows.

    Join tables must be invertible; test reversal.

🧱 5. Columnar Compaction & Output

Applies to:

    pkg/codec/jsonl.go → PrintRecordAsJSONL, PrintRecordGroupAsJSONL

    asdf.go → printCompactColumnsInfo, printRecordLine

✅ Requirements

    Use _columns: to define field order.

    All records must match this structure.

    Compact representation only allowed when all join-table fields are mapped.

    Include human-readable and compact tabular output (printTableHeader, formatRecordAsTable).

✅ 6. Test Coverage Expectations

Defined in:

    pkg/codec/jsonl_test.go, pkg/relational/deriver_test.go, pkg/io/yaml/reader_test.go

🔬 Unit Tests

    _meta propagation: carry-forward, override, unset (TestMetaPropagation)

    Join inference stability (TestJoinTableInference)

    Column order enforcement (TestParseRecordToValuesWithColumns)

    Schema consistency and upgrades (TestSchemaInference)

    Full round-trip (TestRoundTripConversion)
