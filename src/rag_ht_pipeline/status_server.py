from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from .config import (
    DEFAULT_COMPANIES_DIR,
    DEFAULT_COMPANY,
    PipelineConfig,
    discover_company_profiles,
    load_company_config,
    validate_company_slug,
)
from .operations import health


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve local ETL health and status JSON.")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--company", default=None)
    selection.add_argument(
        "--all-companies",
        action="store_true",
        help="Serve one aggregate endpoint plus /companies/<slug>/health routes.",
    )
    parser.add_argument("--companies-dir", type=Path, default=DEFAULT_COMPANIES_DIR)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    return parser.parse_args()


def aggregate_health(configs: dict[str, PipelineConfig]) -> dict[str, object]:
    companies = {slug: health(config) for slug, config in sorted(configs.items())}
    statuses = [str(payload["status"]) for payload in companies.values()]
    healthy = bool(statuses) and all(status in {"PASS", "RUNNING"} for status in statuses)
    if not statuses:
        overall = "UNKNOWN"
    elif not healthy:
        overall = "FAIL"
    elif "RUNNING" in statuses:
        overall = "RUNNING"
    else:
        overall = "PASS"
    return {
        "status": overall,
        "healthy": healthy,
        "company_count": len(companies),
        "companies": companies,
    }


def status_route(
    path: str,
    configs: dict[str, PipelineConfig],
    *,
    aggregate: bool,
) -> tuple[int, dict[str, object]]:
    route = urlsplit(path).path.rstrip("/") or "/"
    if route in {"/health", "/status"}:
        if aggregate:
            payload = aggregate_health(configs)
        else:
            payload = health(next(iter(configs.values())))
        healthy = payload["status"] in {"PASS", "RUNNING"}
        return (200 if healthy else 503), payload

    parts = route.strip("/").split("/")
    if aggregate and len(parts) == 3 and parts[0] == "companies" and parts[2] in {"health", "status"}:
        try:
            slug = validate_company_slug(parts[1])
        except ValueError:
            return 404, {"status": "NOT_FOUND", "error": "Unknown company."}
        config = configs.get(slug)
        if config is None:
            return 404, {"status": "NOT_FOUND", "company_id": slug, "error": "Unknown company."}
        payload = health(config)
        healthy = payload["status"] in {"PASS", "RUNNING"}
        return (200 if healthy else 503), payload
    return 404, {"status": "NOT_FOUND", "error": "Unknown status route."}


def main() -> None:
    args = parse_args()
    if args.all_companies:
        profiles = discover_company_profiles(args.companies_dir)
        configs = {
            slug: load_company_config(slug, companies_dir=args.companies_dir)
            for slug in profiles
        }
        if not configs:
            raise SystemExit(f"No company profiles found under {args.companies_dir}.")
    else:
        company = args.company or DEFAULT_COMPANY
        configs = {company: load_company_config(company, companies_dir=args.companies_dir)}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            status_code, payload = status_route(
                self.path,
                configs,
                aggregate=args.all_companies,
            )
            body = json.dumps(payload, indent=2, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *arguments: object) -> None:
            return

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
