#!/usr/bin/env bash
#
# Integration test for the vdcpd VisiData loader.
#
# Proves that opening a CPD YAML file in VisiData (via our loader) yields the
# same rows as the `cpd` binary's canonical JSONL expansion. VisiData is not
# assumed to be on PATH; if it is missing we re-exec under `nix shell`.
#
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CPD="$REPO/cpd"

# Re-exec inside a nix shell that provides vd + a plain python3 for comparison.
if ! command -v vd >/dev/null 2>&1; then
  echo "vd not on PATH; re-exec under nix shell nixpkgs#visidata ..." >&2
  exec nix shell nixpkgs#visidata nixpkgs#python3 -c bash "$REPO/test/test_visidata.sh"
fi

[ -x "$CPD" ] || { echo "FATAL: cpd binary not found/executable at $CPD (build it first)" >&2; exit 2; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# A minimal .visidatarc that mimics what a user would install: add the loader
# dir to sys.path, import it, and point it at this repo's cpd binary.
RC="$WORK/visidatarc"
cat > "$RC" <<EOF
import sys
sys.path.insert(0, "$REPO")
import vdcpd
options.cpd_path = "$CPD"
EOF

FAILED=0

# roundtrip NAME SRC [vd-extra-args...]
#   expected = cpd SRC ; actual = vd [extra] SRC -b -o out.jsonl
# (extra args precede SRC so e.g. `-f cpd` applies to the file)
roundtrip() {
  local name="$1" src="$2"; shift 2
  local exp="$WORK/$name.expected.jsonl" act="$WORK/$name.actual.jsonl"
  "$CPD" "$src" > "$exp"
  if ! HOME="$WORK" vd "$@" "$src" -b -o "$act" --config "$RC" >"$WORK/$name.vdout" 2>"$WORK/$name.vderr"; then
    echo "FAIL: $name (vd exited non-zero)"; sed 's/^/    vderr: /' "$WORK/$name.vderr" >&2; FAILED=1; return
  fi
  if python3 "$REPO/test/compare_jsonl.py" "$exp" "$act"; then
    echo "PASS: $name"
  else
    echo "FAIL: $name (rows differ)"; FAILED=1
  fi
}

# expect_rows NAME SRC EXPECTED.jsonl [vd-extra-args...]
#   compares vd's load of SRC against a caller-supplied expected file
expect_rows() {
  local name="$1" src="$2" exp="$3"; shift 3
  local act="$WORK/$name.actual.jsonl"
  if ! HOME="$WORK" vd "$@" "$src" -b -o "$act" --config "$RC" >"$WORK/$name.vdout" 2>"$WORK/$name.vderr"; then
    echo "FAIL: $name (vd exited non-zero)"; sed 's/^/    vderr: /' "$WORK/$name.vderr" >&2; FAILED=1; return
  fi
  if python3 "$REPO/test/compare_jsonl.py" "$exp" "$act"; then
    echo "PASS: $name"
  else
    echo "FAIL: $name (rows differ)"; FAILED=1
  fi
}

echo "=== vdcpd VisiData loader integration tests ==="

# 1. Auto-open: bare `vd file.cpd.yaml` expands via the loader (flat data).
roundtrip compacted-autoopen "$REPO/examples/compacted.cpd.yaml"

# 2. Auto-open with a `...` splat column (fields flattened into the row).
roundtrip basic-splat-autoopen "$REPO/examples/expected/basic.cpd.yaml"

# 3. Nested / heterogeneous columns (dicts, lists, mixed types per column).
roundtrip nested-autoopen "$REPO/examples/structured-plain-columns.cpd.yaml"

# 4. Explicit `-f cpd` on a file NOT named *.cpd.yaml (bypasses the wrapper,
#    proves open_cpd is reachable via filetype dispatch).
cp "$REPO/examples/compacted.cpd.yaml" "$WORK/forced.yaml"
roundtrip forced-f-cpd "$WORK/forced.yaml" -f cpd

# 5. Negative: a plain (non-CPD) YAML must still load as ordinary YAML; the
#    wrapper must not hijack it or route it through cpd.
cat > "$WORK/plain.yaml" <<'YAML'
- {a: 1, b: two}
- {a: 3, b: four}
YAML
cat > "$WORK/plain.expected.jsonl" <<'JSON'
{"a": 1, "b": "two"}
{"a": 3, "b": "four"}
JSON
expect_rows plain-yaml-not-hijacked "$WORK/plain.yaml" "$WORK/plain.expected.jsonl"

echo "==============================================="
if [ "$FAILED" -ne 0 ]; then
  echo "RESULT: FAILED"; exit 1
fi
echo "RESULT: PASSED"
