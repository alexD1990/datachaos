"""Describes scenarios from their specs: for `describe`, `list-scenarios` and docs/scenarios.md."""

from __future__ import annotations

from collections.abc import Iterable

from datachaos.scenarios.base import ParamSpec, ScenarioSpec, type_name


def spec_to_dict(spec: ScenarioSpec) -> dict[str, object]:
    return {
        "name": spec.name,
        "version": spec.version,
        "summary": spec.summary,
        "detects": spec.detects,
        "typical_dbt_test": spec.typical_dbt_test,
        "applies_to": sorted(spec.applies_to),
        "params": [
            {
                "name": param.name,
                "type": type_name(param.type),
                "required": param.required,
                "default": _default(param),
                "help": param.help,
            }
            for param in spec.params
        ],
    }


def render_text(spec: ScenarioSpec) -> str:
    lines = [
        f"{spec.name} (version {spec.version})",
        spec.summary,
        "",
        f"Detects: {spec.detects}",
        f"Typical dbt test: {spec.typical_dbt_test}",
        f"Applies to: {', '.join(sorted(spec.applies_to))} tables",
        "",
        "Parameters:",
    ]
    for param in spec.params:
        state = "required" if param.required else f"default: {_show_default(param)}"
        lines.append(f"  {param.name} ({type_name(param.type)}, {state})")
        lines.append(f"      {param.help}")
    return "\n".join(lines) + "\n"


def render_markdown(specs: Iterable[ScenarioSpec]) -> str:
    lines = [
        "# Scenarios",
        "",
        "Generated from the scenario specs by `python -m datachaos._docs`. Do not edit by hand.",
    ]
    for spec in specs:
        lines += [
            "",
            f"## `{spec.name}`",
            "",
            spec.summary,
            "",
            f"- **Version:** {spec.version}",
            f"- **Detects:** {spec.detects}",
            f"- **Typical dbt test:** `{spec.typical_dbt_test}`",
            f"- **Applies to:** {', '.join(sorted(spec.applies_to))} tables",
            "",
            "| Parameter | Type | Required | Default | Description |",
            "|---|---|---|---|---|",
        ]
        for param in spec.params:
            required = "yes" if param.required else "no"
            default = "" if param.required else f"`{_show_default(param)}`"
            description = param.help.replace("|", "\\|")
            lines.append(
                f"| `{param.name}` | {type_name(param.type)} | {required} | {default} | "
                f"{description} |"
            )
    return "\n".join(lines) + "\n"


def _default(param: ParamSpec) -> object:
    return list(param.default) if isinstance(param.default, tuple) else param.default


def _show_default(param: ParamSpec) -> str:
    value = _default(param)
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
