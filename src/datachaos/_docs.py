"""Regenerates docs/scenarios.md. Run from the root of the repository."""

from __future__ import annotations

from pathlib import Path

from datachaos import engine

DOCS_FILE = Path("docs") / "scenarios.md"


def main() -> int:
    with open(DOCS_FILE, "w", encoding="utf-8", newline="") as file:
        file.write(engine.scenarios_markdown())
    print(f"wrote {DOCS_FILE.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
