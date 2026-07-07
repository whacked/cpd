# Design: `vdcpd` — a VisiData loader for the CPD YAML format

- **Date:** 2026-07-08
- **Status:** Approved (design); pending implementation plan
- **Branch:** `2026-04_add-cue-support`

## Context

`cpd` (aka `yamdb`) is a Go tool for the **CommonPayloadData (CPD)** format — a
minimal, human-editable YAML representation of row-oriented structured data with
optional join tables, a `...` splat/catch-all column, `_schemas`, and
`_meta`/`_version` carry-forward. Its canonical, 100%-reversible round-trip
target is JSONL: `cpd file.cpd.yaml` expands to JSONL on stdout.

We want CPD files to open directly in **VisiData**. As the format's providers we
should ship the loader ourselves, but without asking users to paste and maintain
loader code in their `~/.visidatarc`. The agreed compromise: ship a single
loader library alongside the `cpd` binary, and have the user add one
`sys.path` entry plus `import vdcpd` to their `.visidatarc`.

### Build/install hierarchy (as surveyed)

| File | Role |
|---|---|
| `main.go`, `embed.go`, `cue_demo{,_stub}.go`, `version.go` | Go source for the `cpd` binary; two build variants (plain / `-tags cue`). |
| `Sdflow.yaml` | **Dev** build orchestration (make-like). Builds `./cpd`, codegens `version.go`/schemas, runs round-trip tests. Outputs to the repo root. No `install` concept. |
| `default.nix` | The **Nix derivation** (`buildGoApplication`, `tags=["cue"]`). `postInstall` renames `$out/bin/yamdb` → `$out/bin/cpd`. This is the real installable artifact. |
| `flake.nix` | Wires `default.nix` as `packages.default`/`packages.cpd` + `apps.default`; dev shell from `shell.nix`. |

Sdflow = local dev builds; Nix = the installable artifact. The Nix output
currently ships **only** the binary. `default.nix`'s `postInstall` is the
centralized place to also ship the VisiData loader — the "flake hierarchy" the
loader lives in.

## Goals

- `vd file.cpd.yaml` opens the **expanded** data (as `cpd` would produce), not
  the raw compact YAML structure.
- Zero parallel decoder: the `cpd` binary remains the single source of truth.
- Minimal `.visidatarc` footprint: `sys.path` insert + `import vdcpd`.
- Plain (non-CPD) YAML files continue to load as ordinary YAML, untouched.
- Ship the loader with the binary via Nix now, with a file layout a future
  `make install` / Sdflow `install` target can reuse verbatim.

## Non-goals

- No `make install` / Sdflow `install` target in this pass (layout is chosen so
  one drops in later).
- No changes to the `cpd` binary or the CPD format.
- No Parquet path (see Rejected alternatives).

## Decision record (why JSONL shell-out)

Evaluated three decode strategies across speed, maintainability, robustness:

- **JSONL shell-out (chosen):** run `cpd file.cpd.yaml` → JSONL → feed
  VisiData's native JSON sheet. Single source of truth; lossless for CPD's
  JSON-shaped, heterogeneous, sparse data model; fast enough for this format's
  target sizes (small human-edited DBs). Binary guaranteed present via the
  co-install.
- **Parquet shell-out (rejected):** empirically **hard-fails** on heterogeneous
  columns — the very thing CPD allows. `cpd --to-parquet` on
  `examples/structured-plain-columns.cpd.yaml` (column `score` = `95` in one
  row, `[1,2,3]` in another) aborts with
  `failed to append value at row 0, col score: unsupported array type: int`
  and produces no file. Parquet is also CPD-input-only and footer-indexed (not
  truly streamable into pyarrow). Its typing advantage is illusory for a format
  whose columns aren't guaranteed homogeneous.
- **Pure-Python decoder (rejected):** requires a parallel implementation that
  must track the Go codec's semantics (join-table inversion, `...` splat,
  `_meta`/`_version` carry-forward). Its only edge — "no binary dependency" —
  does not apply here, because the binary ships alongside the loader.

## Architecture

A single file, `vdcpd.py`, imported from `.visidatarc`. On import it registers
everything; no other user code is needed.

### `CpdSheet(JsonSheet)`

Subclasses VisiData's `JsonSheet` and overrides only `iterload`:

```python
class CpdSheet(JsonSheet):
    def iterload(self):
        for line in _run_cpd(self.source):      # streamed cpd stdout
            line = line.strip()
            if line:
                yield json.loads(line, object_hook=AttrDict)
```

By yielding parsed JSON objects, it inherits `JsonSheet`'s column discovery
(`addRow` adds an `ItemColumn` per new key), nested-cell handling,
scalar-row wrapping, and per-line parse-error tolerance. Because `iterload`
re-runs `cpd`, VisiData reload (`Ctrl+R`) re-expands the file, picking up edits
to the `.cpd.yaml`.

### `open_cpd(vd, p)`

```python
@VisiData.api
def open_cpd(vd, p):
    return CpdSheet(_sheet_name(p), source=p)
```

Registered via `@VisiData.api`, so both explicit `vd -f cpd file` and the
auto-open wrapper resolve here. `_sheet_name(p)` uses `p.base_stem` with a
trailing `.cpd` stripped for a clean tab name (`foo.cpd.yaml` → `foo`).

### Auto-open wrapper on `open_yaml`

VisiData dispatches by the *last* suffix, so `foo.cpd.yaml` → ext `yaml` →
`open_yaml`. `open_yaml` is a plain class attribute
(`VisiData.open_yaml = VisiData.open_yml`), so we capture the original and
reassign a delegating wrapper:

```python
_orig_open_yml = VisiData.open_yml

@VisiData.api
def open_yaml(vd, p):
    return vd.open_cpd(p) if _is_cpd(p) else _orig_open_yml(vd, p)

VisiData.open_yml = VisiData.open_yaml   # cover .yml too
```

Real YAML falls through to the original loader unchanged.

### `_is_cpd(p)`

- **Primary (filename):** name ends with `.cpd.yaml` or `.cpd.yml`.
- **Secondary (content sniff, conservative):** in the first few KB, a top-level
  `_columns:` **and** (`data:` or `_schemas:`). Catches CPD files with
  non-conventional names.
- Sniff is gated by option `cpd_sniff` (default on). Filename match alone is
  enough; the "plain YAML untouched" guarantee is preserved because the sniff
  requires the CPD-specific `_columns` marker.

### `_run_cpd(source)` and binary discovery

Discovery order:

1. `vd.options.cpd_path`, if set.
2. Install-relative: `Path(__file__).parent.parent.parent / "bin" / "cpd"`
   (`$out/share/visidata/vdcpd.py` → `$out/bin/cpd`), if executable.
3. `shutil.which("cpd")`.
4. Otherwise `vd.fail(...)` with a message pointing at `options.cpd_path` / PATH.

Invocation:

- For local `.yaml`/`.yml` files: `cpd <path>` (extension drives expansion).
- For content-sniffed odd extensions or non-file sources: pipe the source bytes
  to `cpd` stdin (content auto-detect).

Runtime behavior: stream stdout line-by-line (low memory); capture stderr; a
non-zero exit → `vd.fail(stderr)` so malformed CPD surfaces clearly rather than
showing an empty sheet.

### Registration and options (on `import vdcpd`)

- `@VisiData.api` decorations register `open_cpd` and the `open_yaml`/`open_yml`
  wrapper by mutating the `VisiData` class at import time.
- Options declared: `cpd_path` (str, default `''`), `cpd_sniff`
  (bool, default `True`).
- If PyYAML / the built-in yaml loader is unavailable, skip the wrapper and
  register only `open_cpd` (explicit `-f cpd` still works).

## Install wiring (Nix now, layout for later)

`default.nix` `postInstall` gains:

```sh
install -Dm644 vdcpd.py            $out/share/visidata/vdcpd.py
install -Dm644 visidatarc.example  $out/share/visidata/visidatarc.example
```

`share/visidata/` is deliberate: a later `make install` / Sdflow `install`
target maps to any prefix without moving files (`cpd` → `$prefix/bin`,
`vdcpd.py` → `$prefix/share/visidata`). Install-relative discovery works for both
`nix profile install` (`~/.nix-profile/...`) and `nix build` (`./result/...`).

## User setup (the path hack)

Shipped `visidatarc.example` and a README snippet:

```python
import sys, os
sys.path.insert(0, os.path.expanduser('~/.nix-profile/share/visidata'))
import vdcpd
```

README documents alternate paths (`./result/share/visidata`, a custom prefix)
since the location depends on how the install was run.

## Testing (no local VisiData; via `nix shell`)

`test/test_visidata.sh`, run under `nix shell nixpkgs#visidata`:

- **Round-trip / auto-open (per `examples/*.cpd.yaml`):** run
  `vd <file> -b -o out.jsonl` with a temp `.visidatarc` that loads `vdcpd.py`,
  then compare `out.jsonl` (parsed, order-insensitive) against `cpd <file>`.
  Proves auto-open is lossless vs. the source of truth.
- **Negative:** a plain non-CPD `.yaml` still loads as ordinary YAML (the
  wrapper does not hijack it).
- **Explicit:** `vd -f cpd <file>` smoke test.
- The loader locates the repo-built `./cpd` binary via `options.cpd_path` (set
  in the temp `.visidatarc`) or PATH.

## Docs

A README section: install → `.visidatarc` setup → usage, noting that
`.cpd.yaml` files auto-open once the import is present.

## Files

- **New:** `vdcpd.py`, `visidatarc.example`, `test/test_visidata.sh`
- **Edit:** `default.nix`, `README.md`
