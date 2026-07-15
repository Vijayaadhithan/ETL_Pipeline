from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import PipelineConfig, load_company_config
from .credentials import read_env_values


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def run_status_path(config: PipelineConfig) -> Path:
    return config.output.reports / "run_status.json"


def run_history_path(config: PipelineConfig) -> Path:
    return config.output.reports / "run_history.jsonl"


def start_run(config: PipelineConfig, *, command: list[str]) -> dict[str, Any]:
    payload = {
        "run_id": uuid4().hex,
        "company_id": config.company_id,
        "status": "RUNNING",
        "stage": "starting",
        "started_at": utc_now(),
        "heartbeat_at": utc_now(),
        "finished_at": "",
        "pid": os.getpid(),
        "command": command,
        "error_type": "",
        "error": "",
    }
    atomic_write_json(run_status_path(config), payload)
    return payload


def update_run(config: PipelineConfig, *, stage: str, **fields: Any) -> dict[str, Any]:
    payload = read_json(run_status_path(config)) or {
        "run_id": uuid4().hex,
        "company_id": config.company_id,
        "status": "RUNNING",
        "started_at": utc_now(),
    }
    payload.update(fields)
    payload["stage"] = stage
    payload["heartbeat_at"] = utc_now()
    atomic_write_json(run_status_path(config), payload)
    return payload


def finish_run(
    config: PipelineConfig,
    *,
    status: str,
    error: Exception | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = read_json(run_status_path(config)) or {
        "run_id": uuid4().hex,
        "company_id": config.company_id,
        "started_at": utc_now(),
    }
    payload.update(
        {
            "status": status,
            "stage": "complete" if status == "PASS" else "failed",
            "heartbeat_at": utc_now(),
            "finished_at": utc_now(),
            "error_type": type(error).__name__ if error else "",
            "error": str(error) if error else "",
        }
    )
    if summary:
        payload["summary"] = summary
    atomic_write_json(run_status_path(config), payload)
    history = run_history_path(config)
    history.parent.mkdir(parents=True, exist_ok=True)
    with history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    return payload


def _available_memory_mb() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    try:
        pages = os.sysconf("SC_AVPHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None
    return int(pages * page_size // (1024 * 1024))


def _env_file(config: PipelineConfig) -> Path:
    value = Path(config.credentials.get("env_file", ".env"))
    return value if value.is_absolute() else config.project_root / value


def preflight(config: PipelineConfig) -> dict[str, Any]:
    settings = dict(config.operations.get("preflight", {}))
    min_disk_mb = int(settings.get("min_free_disk_mb", 2048))
    min_memory_mb = int(settings.get("min_available_memory_mb", 256))
    disk = shutil.disk_usage(config.output.root if config.output.root.exists() else config.project_root)
    disk_free_mb = int(disk.free // (1024 * 1024))
    memory_mb = _available_memory_mb()
    env_file = _env_file(config)
    env_mode = stat.S_IMODE(env_file.stat().st_mode) if env_file.exists() else None
    failures: list[str] = []
    warnings: list[str] = []
    if disk_free_mb < min_disk_mb:
        failures.append(f"free disk {disk_free_mb} MB is below required {min_disk_mb} MB")
    if memory_mb is not None and memory_mb < min_memory_mb:
        failures.append(f"available memory {memory_mb} MB is below required {min_memory_mb} MB")
    if not env_file.exists():
        failures.append(f"credential file is missing: {env_file}")
    elif env_mode is not None and env_mode & 0o077:
        failures.append(f"credential file permissions must be 600 or stricter: {env_file}")
    env_values = read_env_values(env_file)

    def credential(setting: dict[str, Any], key: str, default: str) -> tuple[str, str]:
        name = str(setting.get(key, default))
        return name, os.environ.get(name, env_values.get(name, ""))

    if str(config.source.get("backend", "csv")) in {"mysql", "postgres"}:
        source_defaults = (
            ("host_env", "MYSQL_HOST"), ("port_env", "MYSQL_PORT"),
            ("database_env", "MYSQL_DATABASE"), ("user_env", "MYSQL_USER"),
            ("password_env", "MYSQL_PASSWORD"),
        ) if config.source["backend"] == "mysql" else (
            ("host_env", "POSTGRES_HOST"), ("port_env", "POSTGRES_PORT"),
            ("database_env", "POSTGRES_DATABASE"), ("user_env", "POSTGRES_USER"),
            ("password_env", "POSTGRES_PASSWORD"),
        )
        missing_source = [
            name
            for key, default in source_defaults
            for name, value in [credential(config.source, key, default)]
            if key not in {"host_env", "port_env"} and not value
        ]
        if missing_source:
            failures.append("missing source credentials: " + ", ".join(missing_source))
    destination_backend = str(config.destination.get("backend", "mysql"))
    destination_defaults = (
        ("host_env", "MYSQL_HOST"), ("port_env", "MYSQL_PORT"),
        ("database_env", "MYSQL_DATABASE"), ("user_env", "MYSQL_USER"),
        ("password_env", "MYSQL_PASSWORD"),
    ) if destination_backend == "mysql" else (
        ("host_env", "POSTGRES_HOST"), ("port_env", "POSTGRES_PORT"),
        ("database_env", "POSTGRES_DATABASE"), ("user_env", "POSTGRES_USER"),
        ("password_env", "POSTGRES_PASSWORD"),
    )
    missing_destination = [
        name
        for key, default in destination_defaults
        for name, value in [credential(config.destination, key, default)]
        if key not in {"host_env", "port_env"} and not value
    ]
    if missing_destination:
        failures.append("missing destination credentials: " + ", ".join(missing_destination))
    source_backend = str(config.source.get("backend", "csv")).lower()
    source_user_default = "MYSQL_USER" if source_backend == "mysql" else "POSTGRES_USER"
    destination_user_default = (
        "MYSQL_USER" if destination_backend == "mysql" else "POSTGRES_USER"
    )
    source_user = credential(config.source, "user_env", source_user_default)[1]
    destination_user = credential(
        config.destination,
        "user_env",
        destination_user_default,
    )[1]
    if (
        source_backend == destination_backend
        and source_user
        and destination_user
        and source_user == destination_user
    ):
        same_user_message = (
            "source and destination use the same database user; this is allowed, "
            "but that account must have both source SELECT and destination publish privileges"
        )
        if bool(settings.get("require_separate_database_users", False)):
            failures.append(same_user_message)
        else:
            warnings.append(same_user_message)
    pending = config.output.reports / "pending_source_run.json"
    journal = config.output.root / "source_sync" / "apply_journal.json"
    if pending.exists():
        warnings.append(f"a pending source generation will be resumed: {pending}")
    if journal.exists():
        warnings.append(f"an interrupted source apply requires recovery: {journal}")
    return {
        "status": "PASS" if not failures else "FAIL",
        "checked_at": utc_now(),
        "company_id": config.company_id,
        "disk_free_mb": disk_free_mb,
        "minimum_disk_mb": min_disk_mb,
        "available_memory_mb": memory_mb,
        "minimum_available_memory_mb": min_memory_mb,
        "env_file": str(env_file),
        "env_mode": f"{env_mode:03o}" if env_mode is not None else "",
        "failures": failures,
        "warnings": warnings,
    }


def send_alert(config: PipelineConfig, message: str, *, severity: str = "error") -> dict[str, Any]:
    webhook_env = str(config.operations.get("alert_webhook_env", "RAG_HT_ALERT_WEBHOOK_URL"))
    env_values = read_env_values(_env_file(config))
    webhook = os.environ.get(webhook_env, env_values.get(webhook_env, "")).strip()
    payload = {
        "company_id": config.company_id,
        "severity": severity,
        "message": message,
        "sent_at": utc_now(),
    }
    delivery = "not_configured"
    if webhook:
        request = urllib.request.Request(
            webhook,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
        delivery = "webhook"
    else:
        recipient = os.environ.get("RAG_HT_ALERT_EMAIL", env_values.get("RAG_HT_ALERT_EMAIL", ""))
    if delivery == "not_configured" and recipient and shutil.which("sendmail"):
        body = (
            f"To: {recipient}\nSubject: [{severity.upper()}] RAG HT ETL {config.company_id}\n\n"
            f"{message}\n"
        )
        subprocess.run(["sendmail", "-t"], input=body, text=True, check=True)
        delivery = "email"
    return {**payload, "delivery": delivery}


def health(config: PipelineConfig) -> dict[str, Any]:
    status = read_json(run_status_path(config)) or {
        "company_id": config.company_id,
        "status": "UNKNOWN",
        "error": "No run status has been recorded.",
    }
    publish = read_json(config.output.reports / "publish_report.json") or {}
    validation = read_json(config.output.reports / "final_output_correctness_report.json") or {}
    pending = read_json(config.output.reports / "pending_source_run.json")
    effective_status = str(status.get("status", "UNKNOWN"))
    timestamp_value = status.get("heartbeat_at") or status.get("finished_at")
    stale_after_hours = float(config.operations.get("max_status_age_hours", 26))
    stale = False
    age_hours: float | None = None
    if timestamp_value:
        try:
            timestamp = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
            age_hours = (
                datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)
            ).total_seconds() / 3600
            stale = age_hours > stale_after_hours
        except ValueError:
            pass
    if stale:
        effective_status = "STALE"
    return {
        "status": effective_status,
        "company_id": config.company_id,
        "stale": stale,
        "status_age_hours": round(age_hours, 2) if age_hours is not None else None,
        "maximum_status_age_hours": stale_after_hours,
        "run": status,
        "last_publish": publish,
        "last_validation": validation,
        "pending_source_generation": pending,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RAG HT ETL operational controls.")
    parser.add_argument(
        "command",
        choices=["preflight", "status", "notify", "status-path", "env-file"],
    )
    parser.add_argument("--company", default="gainr")
    parser.add_argument("--message", default="")
    parser.add_argument("--severity", choices=["info", "warning", "error"], default="error")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_company_config(args.company)
    if args.command == "status-path":
        print(run_status_path(config))
        return
    if args.command == "env-file":
        print(_env_file(config))
        return
    if args.command == "preflight":
        result = preflight(config)
    elif args.command == "status":
        result = health(config)
    else:
        result = send_alert(config, args.message or "ETL notification", severity=args.severity)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    if result.get("status") == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
