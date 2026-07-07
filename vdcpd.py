"""VisiData loader for the CommonPayloadData (CPD) YAML format.

CPD files (``*.cpd.yaml``) are a compact YAML representation of row-oriented
data with join tables, a ``...`` splat column, ``_schemas``, and
``_meta``/``_version`` carry-forward. Their canonical, lossless expansion is
JSONL, produced by the ``cpd`` binary. This loader shells out to ``cpd`` and
feeds the resulting JSONL into VisiData's native JSON sheet machinery, so the
Go codec stays the single source of truth (no parallel decoder to maintain).

Install: add the directory containing this file to ``sys.path`` in your
``~/.visidatarc`` and ``import vdcpd``. That is all that is required::

    import sys, os
    sys.path.insert(0, os.path.expanduser('~/.nix-profile/share/visidata'))
    import vdcpd

Once imported, ``vd file.cpd.yaml`` auto-expands (the loader wraps the built-in
YAML loader and delegates CPD files to itself; plain YAML is untouched). You can
also force it with ``vd -f cpd <file>``.

Options:
    cpd_path   explicit path to the cpd binary (else install-relative, then PATH)
    cpd_sniff  sniff YAML content for CPD markers to auto-open oddly-named files
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

from visidata import VisiData, JsonSheet, vd

try:
    from visidata import AttrDict
except ImportError:  # pragma: no cover - very old visidata
    from visidata.utils import AttrDict


vd.option('cpd_path', '', 'path to the cpd binary (else install-relative or PATH)')
vd.option('cpd_sniff', True, 'sniff YAML content for CPD markers (_columns) to auto-open')


def _find_cpd():
    """Locate the cpd binary: option, then install-relative, then PATH."""
    configured = vd.options.cpd_path
    if configured:
        return configured

    # Install layout: <prefix>/share/visidata/vdcpd.py -> <prefix>/bin/cpd
    here = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.normpath(os.path.join(here, os.pardir, os.pardir, "bin", "cpd"))
    if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
        return candidate

    found = shutil.which("cpd")
    if found:
        return found

    vd.fail("cpd binary not found: set options.cpd_path or add cpd to PATH")


def _cpd_argv(exe, source):
    """Build the cpd invocation for *source*.

    Returns (argv, stdin_text). For local ``.yaml``/``.yml`` files cpd reads the
    path directly (extension drives expansion); otherwise the raw bytes are fed
    to cpd's stdin, which auto-detects CPD content.
    """
    path = str(source)
    if os.path.isfile(path):
        if path.lower().endswith((".yaml", ".yml")):
            return [exe, path], None
        with open(path, encoding="utf-8", errors="replace") as fp:
            return [exe], fp.read()

    # Non-file source (url/stdin/compressed): read through visidata's Path.
    fp = source.open_text() if hasattr(source, "open_text") else source.open()
    try:
        return [exe], fp.read()
    finally:
        fp.close()


def _run_cpd_lines(source):
    """Yield JSONL text lines from expanding *source* with the cpd binary.

    Fails cleanly (vd.fail with cpd's stderr) on a non-zero exit.
    """
    exe = _find_cpd()
    argv, stdin_text = _cpd_argv(exe, source)

    if stdin_text is not None:
        proc = subprocess.run(
            argv, input=stdin_text, capture_output=True, text=True, encoding="utf-8"
        )
        if proc.returncode != 0:
            vd.fail(f"cpd exited {proc.returncode}: {proc.stderr.strip()}")
        yield from proc.stdout.splitlines()
        return

    # Path case: stream stdout; route stderr to a temp file to avoid any
    # pipe-buffer deadlock, and surface it only on failure.
    errf = tempfile.TemporaryFile(mode="w+")
    try:
        proc = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=errf, text=True, encoding="utf-8"
        )
        for line in proc.stdout:
            yield line
        proc.stdout.close()
        ret = proc.wait()
        if ret != 0:
            errf.seek(0)
            msg = errf.read().strip()
            vd.fail(f"cpd exited {ret}: {msg}" if msg else f"cpd exited {ret}")
    finally:
        errf.close()


class CpdSheet(JsonSheet):
    """A JsonSheet whose rows come from ``cpd``'s expansion of a CPD file.

    Inherits column discovery, nested-cell handling, and per-line error
    tolerance from JsonSheet. Because iterload re-runs cpd, reload (Ctrl+R)
    re-expands the source file, picking up on-disk edits.
    """

    def iterload(self):
        for line in _run_cpd_lines(self.source):
            line = line.strip()
            if line:
                yield json.loads(line, object_hook=AttrDict)


def _sheet_name(p):
    name = p.base_stem
    if name.endswith(".cpd"):
        name = name[: -len(".cpd")]
    return name


@VisiData.api
def open_cpd(vd, p):
    return CpdSheet(_sheet_name(p), source=p)


_CPD_COLUMNS_RE = re.compile(r"(?m)^_columns\s*:")
_CPD_SECTION_RE = re.compile(r"(?m)^(data|_schemas)\s*:")


def _is_cpd(p):
    """True if *p* should be opened as CPD rather than plain YAML."""
    name = str(getattr(p, "given", p)).lower()
    if name.endswith(".cpd.yaml") or name.endswith(".cpd.yml"):
        return True

    if not vd.options.cpd_sniff:
        return False

    path = str(p)
    if not os.path.isfile(path):
        return False
    try:
        with open(path, encoding="utf-8", errors="replace") as fp:
            head = fp.read(4096)
    except OSError:
        return False
    return bool(_CPD_COLUMNS_RE.search(head)) and bool(_CPD_SECTION_RE.search(head))


# --- auto-open: wrap the built-in YAML loader ------------------------------
#
# VisiData dispatches by the LAST suffix, so ``foo.cpd.yaml`` resolves to
# open_yaml. We reassign open_yaml/open_yml directly (NOT via @VisiData.api,
# whose functools.wraps step would copy the old function's __name__ and rebind
# the wrong attribute) so CPD files route to open_cpd and real YAML falls
# through unchanged.
_orig_open_yml = getattr(VisiData, "open_yml", None)

if _orig_open_yml is not None:

    def _open_yaml_dispatch(vd, p):
        if _is_cpd(p):
            return vd.open_cpd(p)
        return _orig_open_yml(vd, p)

    VisiData.open_yaml = _open_yaml_dispatch
    VisiData.open_yml = _open_yaml_dispatch
