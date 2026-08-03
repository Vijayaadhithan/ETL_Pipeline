#!/usr/bin/env python3
"""Validate repository-local links and anchors in Markdown documentation."""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

LINK_PATTERN = re.compile(r"(?<!!)\[[^]]*]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
INLINE_MARKUP_PATTERN = re.compile(r"[`*_~]")
NON_ANCHOR_PATTERN = re.compile(r"[^\w\- ]", re.UNICODE)
DEFAULT_PATHS = (Path("README.md"), Path("PRODUCTION_MANUAL_RUNBOOK.md"))


def github_anchor(heading: str) -> str:
    """Return the GitHub-style base anchor for a Markdown heading."""
    without_markup = INLINE_MARKUP_PATTERN.sub("", heading).casefold()
    without_punctuation = NON_ANCHOR_PATTERN.sub("", without_markup)
    return without_punctuation.replace(" ", "-")


def markdown_anchors(path: Path) -> set[str]:
    """Return anchors, including GitHub's numeric duplicate suffixes."""
    anchors: set[str] = set()
    duplicates: Counter[str] = Counter()
    for heading in HEADING_PATTERN.findall(path.read_text(encoding="utf-8")):
        base = github_anchor(heading)
        count = duplicates[base]
        anchors.add(base if count == 0 else f"{base}-{count}")
        duplicates[base] += 1
    return anchors


def markdown_files(root: Path, paths: list[Path]) -> list[Path]:
    """Expand files and directories into a stable Markdown file list."""
    files: set[Path] = set()
    for relative_path in paths:
        candidate = root / relative_path
        if candidate.is_dir():
            files.update(candidate.rglob("*.md"))
        elif candidate.suffix.casefold() == ".md" and candidate.is_file():
            files.add(candidate)
    return sorted(files)


def local_link_errors(root: Path, source: Path) -> list[str]:
    """Validate local link targets and optional Markdown anchors."""
    errors: list[str] = []
    text = source.read_text(encoding="utf-8")
    for raw_target in LINK_PATTERN.findall(text):
        target = raw_target.strip().strip("<>")
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc:
            continue

        relative_target = unquote(parsed.path)
        destination = (
            source
            if not relative_target
            else (source.parent / relative_target).resolve()
        )
        try:
            destination.relative_to(root)
        except ValueError:
            errors.append(
                f"{source.relative_to(root)}: link escapes repository: {target}"
            )
            continue
        if not destination.is_file():
            errors.append(f"{source.relative_to(root)}: missing target: {target}")
            continue
        if parsed.fragment and destination.suffix.casefold() == ".md":
            fragment = unquote(parsed.fragment).casefold()
            if fragment not in markdown_anchors(destination):
                errors.append(
                    f"{source.relative_to(root)}: missing anchor: {target}"
                )
    return errors


def check_markdown(root: Path, paths: list[Path]) -> list[str]:
    """Return every validation error for the selected Markdown paths."""
    resolved_root = root.resolve()
    errors: list[str] = []
    for path in markdown_files(resolved_root, paths):
        errors.extend(local_link_errors(resolved_root, path))
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path, default=list(DEFAULT_PATHS))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors = check_markdown(args.root, args.paths)
    if errors:
        for error in errors:
            print(error)
        return 1
    checked = len(markdown_files(args.root.resolve(), args.paths))
    print(f"Markdown validation passed ({checked} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
