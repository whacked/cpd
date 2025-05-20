
`$ as bash
mkdir -p pkg/types/
touch pkg/types/record.go
touch pkg/types/record_test.go
touch pkg/types/table.go
touch pkg/types/table_test.go
touch pkg/types/codec.go
touch pkg/types/codec_test.go
mkdir -p pkg/schema/
touch pkg/schema/validator.go
touch pkg/schema/validator_test.go
touch pkg/schema/parser.go
touch pkg/schema/parser_test.go
mkdir -p pkg/codec/
touch pkg/codec/registry.go
touch pkg/codec/registry_test.go
touch pkg/codec/builtin.go
touch pkg/codec/builtin_test.go
mkdir -p pkg/io/
touch pkg/io/common.go
touch pkg/io/common_test.go
mkdir -p pkg/io/yaml/
touch pkg/io/yaml/reader.go
touch pkg/io/yaml/reader_test.go
touch pkg/io/yaml/writer.go
touch pkg/io/yaml/writer_test.go
mkdir -p pkg/io/jsonl/
touch pkg/io/jsonl/reader.go
touch pkg/io/jsonl/reader_test.go
touch pkg/io/jsonl/writer.go
touch pkg/io/jsonl/writer_test.go
mkdir -p pkg/io/sqlite/
touch pkg/io/sqlite/reader.go
touch pkg/io/sqlite/reader_test.go
touch pkg/io/sqlite/writer.go
touch pkg/io/sqlite/writer_test.go
touch pkg/io/sqlite/mapper.go
touch pkg/io/sqlite/mapper_test.go
mkdir -p pkg/internal/testutil/
touch pkg/internal/testutil/fixtures.go
touch pkg/internal/testutil/fixtures_test.go
mkdir -p cmd/yamdb/commands/
mkdir -p cmd/yamdb/
touch cmd/yamdb/main.go
touch cmd/yamdb/root.go
touch cmd/yamdb/commands/validate.go
touch cmd/yamdb/commands/validate_test.go
touch cmd/yamdb/commands/lint.go
touch cmd/yamdb/commands/lint_test.go
touch cmd/yamdb/commands/summarize.go
touch cmd/yamdb/commands/summarize_test.go
touch cmd/yamdb/commands/convert.go
touch cmd/yamdb/commands/convert_test.go
touch cmd/yamdb/commands/dev.go
touch cmd/yamdb/commands/dev_test.go
`

