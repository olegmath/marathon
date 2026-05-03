#!/usr/bin/env python3
"""Quick smoke-check for the Soholms backend rating payload."""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from urllib.parse import urlencode
from urllib.request import urlopen


BACKEND_URL = os.getenv("SOHOLMS_BACKEND_URL", "http://127.0.0.1:8787").rstrip("/")


def fetch_json(path: str, params: dict[str, str] | None = None) -> dict:
    query = f"?{urlencode(params)}" if params else ""
    with urlopen(f"{BACKEND_URL}{path}{query}", timeout=180) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    period_from = os.getenv("SOHOLMS_PERIOD_FROM", "")
    period_to = os.getenv("SOHOLMS_PERIOD_TO", "")
    params = {}
    if period_from:
        params["periodFrom"] = period_from
    if period_to:
        params["periodTo"] = period_to

    groups_payload = fetch_json("/api/groups", {"configured": "1"})
    ratings_payload = fetch_json("/api/ratings", params)

    groups = groups_payload.get("groups", [])
    rows = ratings_payload.get("rows", [])
    errors = ratings_payload.get("errors", [])
    missing = ratings_payload.get("missingConfigNames") or groups_payload.get("missingConfigNames") or []
    period = ratings_payload.get("period", {})

    print(f"backend: {BACKEND_URL}")
    print(f"period: {period.get('from', '-') } -> {period.get('to', '-')}")
    print(f"configured groups: {len(groups)}")
    print(f"rating groups: {len(ratings_payload.get('groups', []))}")
    print(f"students: {len(rows)}")
    print(f"errors: {len(errors)}")
    print(f"missing config: {missing}")

    by_subject = Counter(row.get("subject") or "без предмета" for row in rows)
    by_level = Counter((row.get("subject") or "без предмета", row.get("level") or "") for row in rows)

    print("\nby subject:")
    for subject, count in sorted(by_subject.items()):
        print(f"  {subject}: {count}")

    print("\nby subject/level:")
    for (subject, level), count in sorted(by_level.items()):
        print(f"  {subject} {level}: {count}")

    if errors:
        print("\nfirst errors:")
        for error in errors[:10]:
            print(f"  {error.get('groupId')} {error.get('group')}: {error.get('error')}")

    return 1 if errors or missing or not rows else 0


if __name__ == "__main__":
    sys.exit(main())
