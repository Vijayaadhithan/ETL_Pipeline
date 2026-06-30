from __future__ import annotations

import os
from pathlib import Path


def read_env_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def resolve_env_value(
    env_name: str,
    *,
    env_file: Path,
    default: str | None = None,
    required: bool = True,
    context: str = "",
) -> str:
    file_values = read_env_values(env_file)
    value = os.environ.get(env_name, file_values.get(env_name, default))
    if required and not value:
        suffix = f" for {context}" if context else ""
        raise RuntimeError(
            f"Missing credential {env_name!r}{suffix}; configure it in {env_file} or the environment."
        )
    return value or ""
