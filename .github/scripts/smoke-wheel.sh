#!/usr/bin/env bash
# Builds the sdist and the wheel, and uses the wheel in a clean venv outside the repository.
# Run it from anywhere: CI runs the same script as a local run does.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
python="${PYTHON:-python3}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
unset PYTHONPATH

echo "== build"
"$python" -m venv "$work/tools"
"$work/tools/bin/pip" install --quiet --upgrade build twine
# Builds the sdist, and the wheel from it, so a file missing in the sdist also breaks the wheel.
"$work/tools/bin/python" -m build --outdir "$work/dist" "$repo"
"$work/tools/bin/python" -m twine check --strict "$work"/dist/*

echo "== contents"
"$work/tools/bin/python" - "$work"/dist/*.whl <<'PY'
import sys
import zipfile

names = zipfile.ZipFile(sys.argv[1]).namelist()
assert "datachaos/py.typed" in names, "py.typed is missing"
assert "datachaos/scenarios/schema_drift.py" in names, "a scenario is missing"
outside = [n for n in names if not n.startswith(("datachaos/", "datachaos-"))]
assert not outside, f"the wheel holds other files: {outside}"
PY
# A file, since grep -q closes a pipe early, which pipefail takes for an error in tar.
tar tzf "$work"/dist/*.tar.gz >"$work/sdist.txt"
grep -q "/examples/dbt_shop/run_demo.py" "$work/sdist.txt"
if grep -q "build-plan" "$work/sdist.txt"; then
    echo "the build plan is in the sdist" >&2
    exit 1
fi

echo "== install in a clean venv"
"$python" -m venv "$work/venv"
"$work/venv/bin/pip" install --quiet "$work"/dist/*.whl
mkdir "$work/run"
cd "$work/run"
bin="$work/venv/bin"

"$bin/python" - "$work/venv" <<'PY'
import sys
from pathlib import Path

import datachaos

location = Path(datachaos.__file__).resolve()
assert Path(sys.argv[1]).resolve() in location.parents, f"not from the venv: {location}"
assert "site-packages" in location.parts, f"not installed: {location}"
PY

echo "== commands"
"$bin/datachaos" --version
"$bin/datachaos" --help >/dev/null
for command in generate run list-scenarios describe validate report inject land check-dbt; do
    "$bin/datachaos" "$command" --help >/dev/null
done
test "$("$bin/datachaos" list-scenarios | wc -l)" -eq 8

"$bin/datachaos" generate --out baseline --seed 1 --batches 3
"$bin/datachaos" validate baseline --reproduce

"$bin/datachaos" run --out chaos --seed 1 --batches 3 \
    --scenario null-critical-column --table orders --param column=amount --param count=5
"$bin/datachaos" validate chaos --reproduce

echo "== a changed byte must be found"
printf 'x' >> chaos/data/orders/batch_0000.csv
set +e
"$bin/datachaos" validate chaos >/dev/null
code=$?
set -e
test "$code" -eq 1

echo "smoke test passed"
