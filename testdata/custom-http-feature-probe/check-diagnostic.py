#!/usr/bin/env python3
"""Recognize a completed, expected root-crate compiler rejection only."""
import json
from pathlib import Path
import sys


def rejected(records, source, code):
    finished = [record for record in records if record.get("reason") == "build-finished"]
    errors = [record for record in records
              if record.get("reason") == "compiler-message"
              and record.get("message", {}).get("level") == "error"]
    return (len(finished) == 1 and finished[0].get("success") is False
            and bool(errors) and all(
                Path(record.get("target", {}).get("src_path", "")).resolve() == source.resolve()
                and (record["message"].get("code") or {}).get("code") == code
                for record in errors))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: check-diagnostic.py <cargo-json> <probe-source> <expected-code>")
    try:
        records = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines() if line.strip()]
        accepted = rejected(records, Path(sys.argv[2]), sys.argv[3])
    except (ValueError, OSError):
        accepted = False
    raise SystemExit(0 if accepted else 1)
