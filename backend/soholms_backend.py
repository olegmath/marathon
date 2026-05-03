#!/usr/bin/env python3
"""Small Soholms proxy/parser backend for the marathon rating site.

The server keeps the Soholms Authorization token on the backend side, downloads
attendance XLSX exports, parses them, and returns JSON rows compatible with the
current frontend rating calculations.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from openpyxl import load_workbook


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_env_file(os.path.join(os.path.dirname(__file__), ".env"))

API_BASE = os.getenv("SOHOLMS_API_BASE", "https://api.soholms.com").rstrip("/")
DEFAULT_CACHE_SECONDS = int(os.getenv("SOHOLMS_CACHE_SECONDS", "900"))
DEFAULT_CONCURRENCY = int(os.getenv("SOHOLMS_CONCURRENCY", "4"))
MAX_GROUPS_PER_REQUEST = int(os.getenv("SOHOLMS_MAX_GROUPS", "80"))
DEADLINE_SHIFT_DAYS = int(os.getenv("SOHOLMS_DEADLINE_SHIFT_DAYS", "1"))
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "groups.config.json")
BACKEND_ADMIN_KEY = os.getenv("BACKEND_ADMIN_KEY", "").strip()

GROUP_TREE_PATH = "/api/v1/learning_group/get_tree"
ATTENDANCE_PATH = "/master/api/learning/attendance-sheet/excel/data"

SUBJECT_ALIASES = (
    ("математика", ("матем", "мат11", "м2", "м1")),
    ("русский язык", ("рус", "ря", "русский")),
    ("физика", ("физ", "физика")),
    ("информатика", ("инф", "информ")),
    ("обществознание", ("общ", "обществ")),
    ("история", ("ист", "история")),
)

MONTHS_RU = {
    1: "янв",
    2: "фев",
    3: "мар",
    4: "апр",
    5: "мая",
    6: "июн",
    7: "июл",
    8: "авг",
    9: "сен",
    10: "окт",
    11: "ноя",
    12: "дек",
}

_CACHE: dict[str, tuple[float, Any]] = {}


class BackendError(Exception):
    def __init__(self, message: str, status: int = 500):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class GroupInfo:
    id: int
    name: str
    subject: str
    teacher: str
    parent_group_ids: tuple[int, ...]
    student_count: int


def clean_authorization_header(value: str) -> str:
    token = value.strip()
    token = re.sub(r"^\s*Authorization:\s*", "", token, flags=re.IGNORECASE).strip()
    token = token.strip("\"'")
    while token.endswith("\\"):
        token = token[:-1].strip()
        token = token.strip("\"'")
    token = token.strip("\"'")
    return token


def get_authorization_header(kind: str = "api") -> str:
    specific_name = "SOHOLMS_EXCEL_TOKEN" if kind == "excel" else "SOHOLMS_API_TOKEN"
    token = clean_authorization_header(os.getenv(specific_name, "")) or clean_authorization_header(os.getenv("SOHOLMS_TOKEN", ""))
    if not token:
        raise BackendError(
            f"{specific_name} or SOHOLMS_TOKEN is not configured",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    return token


def soholms_headers(accept: str = "application/json", kind: str = "api") -> dict[str, str]:
    return {
        "accept": accept,
        "authorization": get_authorization_header(kind),
        "origin": "https://master.soholms.com",
        "referer": "https://master.soholms.com/",
        "user-agent": "marathon-rating-backend/1.0",
    }


def cached(key: str, ttl_seconds: int, loader):
    now = time.time()
    item = _CACHE.get(key)
    if item and now - item[0] < ttl_seconds:
        return item[1]
    try:
        value = loader()
        _CACHE[key] = (now, value)
        return value
    except Exception:
        if item:
            return item[1]
        raise


def clear_cache() -> None:
    _CACHE.clear()


def token_fingerprint(name: str) -> dict[str, Any]:
    value = os.getenv(name, "")
    stripped = clean_authorization_header(value)
    return {
        "configured": bool(stripped),
        "length": len(stripped),
        "startsWithBearer": stripped.lower().startswith("bearer "),
        "preview": f"{stripped[:10]}...{stripped[-6:]}" if len(stripped) > 20 else "",
    }


def request_json(method: str, path: str, **kwargs) -> Any:
    url = f"{API_BASE}{path}"
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.request(method, url, timeout=45, **kwargs)
            if response.status_code >= 400:
                raise BackendError(
                    f"Soholms API error {response.status_code}: {response.text[:500]}",
                    response.status_code,
                )
            return response.json()
        except BackendError:
            raise
        except requests.RequestException as error:
            last_error = error
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
    raise BackendError(f"Soholms API request failed: {last_error}", HTTPStatus.BAD_GATEWAY)


def fetch_group_tree() -> list[dict[str, Any]]:
    def load():
        data = request_json(
            "POST",
            GROUP_TREE_PATH,
            headers={**soholms_headers(), "content-type": "application/json"},
            data="",
        )
        groups = data.get("learningGroups")
        if not isinstance(groups, list):
            raise BackendError("Unexpected get_tree response: missing learningGroups")
        return groups

    return cached("group_tree", DEFAULT_CACHE_SECONDS, load)


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_group_name(value: Any) -> str:
    return normalize_text(value).casefold()


def compact_group_name(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-яё]+", "", normalize_group_name(value))


def load_group_config() -> dict[str, Any]:
    path = os.getenv("SOHOLMS_GROUP_CONFIG", DEFAULT_CONFIG_PATH)
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def children_by_parent(groups: list[dict[str, Any]]) -> dict[int, list[int]]:
    children: dict[int, list[int]] = defaultdict(list)
    for group in groups:
        group_id = group.get("id")
        if not isinstance(group_id, int):
            continue
        for parent_id in group.get("parentGroupIds") or []:
            if isinstance(parent_id, int):
                children[parent_id].append(group_id)
    return children


def descendant_ids(root_ids: set[int], groups: list[dict[str, Any]]) -> set[int]:
    children = children_by_parent(groups)
    result: set[int] = set()
    stack = list(root_ids)
    while stack:
        group_id = stack.pop()
        for child_id in children.get(group_id, []):
            if child_id in result:
                continue
            result.add(child_id)
            stack.append(child_id)
    return result


def similar_group_names(name: str, groups: list[dict[str, Any]], limit: int = 6) -> list[str]:
    target = compact_group_name(name)
    if not target:
        return []

    scored: list[tuple[float, str]] = []
    seen: set[str] = set()
    for group in groups:
        group_name = normalize_text(group.get("name"))
        if not group_name or group_name in seen:
            continue
        seen.add(group_name)
        compact = compact_group_name(group_name)
        score = SequenceMatcher(None, target, compact).ratio()
        if target in compact or compact in target:
            score += 0.35
        scored.append((score, group_name))

    return [name for score, name in sorted(scored, reverse=True)[:limit] if score >= 0.45]


def resolve_config_group_ids(groups: list[dict[str, Any]], config: dict[str, Any]) -> tuple[set[int], list[str], dict[str, list[str]]]:
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    existing_ids = {group.get("id") for group in groups if isinstance(group.get("id"), int)}
    for group in groups:
        by_name[normalize_group_name(group.get("name"))].append(group)

    selected: set[int] = set()
    missing: list[str] = []
    candidates: dict[str, list[str]] = {}

    for raw_name in config.get("groupNames") or []:
        matches = by_name.get(normalize_group_name(raw_name), [])
        if not matches:
            missing_name = str(raw_name)
            missing.append(missing_name)
            candidates[missing_name] = similar_group_names(missing_name, groups)
            continue
        selected.update(group["id"] for group in matches if isinstance(group.get("id"), int))

    descendant_roots: set[int] = set()
    for raw_name in config.get("includeDescendantsOfNames") or []:
        matches = by_name.get(normalize_group_name(raw_name), [])
        if not matches:
            missing_name = str(raw_name)
            missing.append(missing_name)
            candidates[missing_name] = similar_group_names(missing_name, groups)
            continue
        descendant_roots.update(group["id"] for group in matches if isinstance(group.get("id"), int))
    selected.update(descendant_ids(descendant_roots, groups))

    for group_id in config.get("groupIds") or []:
        try:
            numeric_id = int(group_id)
        except (TypeError, ValueError):
            missing_name = str(group_id)
            missing.append(missing_name)
            candidates[missing_name] = []
            continue
        if numeric_id not in existing_ids:
            missing_name = f"groupId:{numeric_id}"
            missing.append(missing_name)
            candidates[missing_name] = []
            continue
        selected.add(numeric_id)

    return selected, missing, candidates


def teacher_name(teacher: dict[str, Any]) -> str:
    parts = [
        normalize_text(teacher.get("lastName")),
        normalize_text(teacher.get("firstName")),
        normalize_text(teacher.get("middleName")),
    ]
    return " ".join([part for part in parts if part])


def group_teacher_label(group: dict[str, Any]) -> str:
    names: list[str] = []
    seen: set[str] = set()
    for teacher in group.get("teachers") or []:
        if teacher.get("isDisabled"):
            continue
        name = teacher_name(teacher)
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return ", ".join(names) or "Без преподавателя"


def infer_subject(value: str, parent_subject: str = "") -> str:
    text = f"{value} {parent_subject}".lower()
    for subject, aliases in SUBJECT_ALIASES:
        if any(alias in text for alias in aliases):
            return subject
    return "без предмета"


def infer_level(value: str) -> str:
    text = value.upper()
    if "ОГЭ" in text:
        return "ОГЭ"
    if "ЕГЭ" in text:
        return "ЕГЭ"
    if re.search(r"(^|[^0-9])9([^0-9]|$)", text):
        return "ОГЭ"
    if re.search(r"(^|[^0-9])11([^0-9]|$)", text):
        return "ЕГЭ"
    return ""


def discipline_matches_group(discipline: Any, group: GroupInfo) -> bool:
    text = normalize_text(discipline)
    lowered = text.casefold()
    if not text:
        return True
    if "основн" in lowered:
        return False

    discipline_subject = infer_subject(text)
    if group.subject != "без предмета" and discipline_subject != "без предмета" and discipline_subject != group.subject:
        return False

    expected_level = infer_level(group.name)
    discipline_level = infer_level(text)
    if expected_level and discipline_level and discipline_level != expected_level:
        return False

    return True


def build_group_index(groups: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    result = {}
    for group in groups:
        group_id = group.get("id")
        if isinstance(group_id, int):
            result[group_id] = group
    return result


def parent_subject_name(group: dict[str, Any], by_id: dict[int, dict[str, Any]]) -> str:
    for parent_id in group.get("parentGroupIds") or []:
        parent = by_id.get(parent_id)
        if parent:
            subject = infer_subject(parent.get("name", ""))
            if subject != "без предмета":
                return subject
    return ""


def selected_groups(
    group_ids: set[int] | None = None,
    subjects: set[str] | None = None,
    include_virtual: bool = False,
    origins: set[str] | None = None,
) -> list[GroupInfo]:
    groups = fetch_group_tree()
    by_id = build_group_index(groups)
    result: list[GroupInfo] = []

    for group in groups:
        group_id = group.get("id")
        if not isinstance(group_id, int):
            continue
        if group_ids is not None and group_id not in group_ids:
            continue
        if not include_virtual and group.get("purpose") != "PhysicalLearning":
            continue
        if origins and normalize_text(group.get("origin")) not in origins:
            continue
        if not group.get("studentIds"):
            continue

        teacher = group_teacher_label(group)
        if teacher == "Без преподавателя" and group_ids is None:
            continue

        parent_subject = parent_subject_name(group, by_id)
        subject = infer_subject(group.get("name", ""), parent_subject)
        if subjects and subject not in subjects:
            continue

        result.append(
            GroupInfo(
                id=group_id,
                name=normalize_text(group.get("name")),
                subject=subject,
                teacher=teacher,
                parent_group_ids=tuple(group.get("parentGroupIds") or []),
                student_count=len(group.get("studentIds") or []),
            )
        )

    return sorted(result, key=lambda item: (item.subject, item.name, item.id))


def searchable_group_row(group: dict[str, Any], by_id: dict[int, dict[str, Any]]) -> dict[str, Any]:
    parent_subject = parent_subject_name(group, by_id)
    return {
        "id": group.get("id"),
        "name": normalize_text(group.get("name")),
        "subject": infer_subject(group.get("name", ""), parent_subject),
        "teacher": group_teacher_label(group),
        "purpose": group.get("purpose"),
        "origin": group.get("origin"),
        "parent_group_ids": group.get("parentGroupIds") or [],
        "student_count": len(group.get("studentIds") or []),
    }


def search_groups(search: str) -> list[dict[str, Any]]:
    groups = fetch_group_tree()
    by_id = build_group_index(groups)
    query = normalize_group_name(search)
    compact_query = compact_group_name(search)
    result: list[dict[str, Any]] = []

    for group in groups:
        name = group.get("name")
        if not normalize_text(name):
            continue
        normalized_name = normalize_group_name(name)
        compact_name = compact_group_name(name)
        if query in normalized_name or compact_query in compact_name:
            result.append(searchable_group_row(group, by_id))

    return sorted(result, key=lambda item: (item["subject"], item["name"], item["id"] or 0))


def fetch_attendance_xlsx(group_id: int, period_from: str, period_to: str) -> bytes:
    params = {
        "learningGroupId": group_id,
        "periodFrom": period_from,
        "periodTo": period_to,
        "isInteractiveLessons": "true",
        "isAcademicLessons": "true",
        "isStudentsOutOfPeriod": "false",
        "withHomeworks": "true",
        "academicDisciplineIds": "",
        "masterClientIds": "",
    }
    url = f"{API_BASE}{ATTENDANCE_PATH}?{urlencode(params)}"
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                headers=soholms_headers(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
                    kind="excel",
                ),
                timeout=90,
            )
            if response.status_code >= 400:
                raise BackendError(
                    f"Soholms XLSX error {response.status_code} for group {group_id}: {response.text[:500]}",
                    response.status_code,
                )
            return response.content
        except BackendError:
            raise
        except requests.RequestException as error:
            last_error = error
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
    raise BackendError(f"Soholms XLSX request failed for group {group_id}: {last_error}", HTTPStatus.BAD_GATEWAY)


def score_value(*values: Any) -> float | None:
    for value in values:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            match = re.match(r"^\s*(\d+(?:[,.]\d+)?)\s*/\s*(\d+(?:[,.]\d+)?)\s*$", value)
            if match:
                got = float(match.group(1).replace(",", "."))
                total = float(match.group(2).replace(",", "."))
                return 0.0 if total == 0 else got / total * 100
            try:
                return float(value.replace(",", "."))
            except ValueError:
                continue
    return None


def iso_date(value: Any, shift_days: int = 0) -> str:
    if isinstance(value, datetime):
        return (value.date() + timedelta(days=shift_days)).isoformat()
    if isinstance(value, date):
        return (value + timedelta(days=shift_days)).isoformat()
    return normalize_text(value)


def date_label(value: Any, shift_days: int = 0) -> str:
    if isinstance(value, datetime):
        d = value.date() + timedelta(days=shift_days)
    elif isinstance(value, date):
        d = value + timedelta(days=shift_days)
    else:
        return normalize_text(value)
    return f"{d.day:02d}.{MONTHS_RU.get(d.month, str(d.month))}"


def parse_lesson_number(value: Any) -> int:
    match = re.match(r"^\s*(\d+)\.", normalize_text(value))
    return int(match.group(1)) if match else 0


def is_day_lesson(value: Any) -> bool:
    return "день" in normalize_text(value).lower()


def lesson_day_key(lesson: Any, lesson_date: Any) -> str:
    day_order = parse_lesson_number(lesson)
    if day_order:
        return f"day:{day_order}"
    date_key = iso_date(lesson_date)
    if date_key:
        return f"date:{date_key}"
    return f"lesson:{normalize_text(lesson)}"


def header_index(headers: list[Any], *names: str, default: int | None = None) -> int | None:
    normalized_headers = [normalize_text(header).casefold() for header in headers]
    for name in names:
        normalized_name = normalize_text(name).casefold()
        for index, header in enumerate(normalized_headers):
            if header == normalized_name:
                return index
    return default


def row_value(row: tuple[Any, ...], index: int | None) -> Any:
    if index is None or index >= len(row):
        return None
    return row[index]


def late_days(lesson_date: Any, submitted_at: Any) -> int:
    if not isinstance(lesson_date, (date, datetime)) or not isinstance(submitted_at, (date, datetime)):
        return 0

    lesson_day = lesson_date.date() if isinstance(lesson_date, datetime) else lesson_date
    submitted_day = submitted_at.date() if isinstance(submitted_at, datetime) else submitted_at
    due_day = lesson_day + timedelta(days=DEADLINE_SHIFT_DAYS)
    return max(0, (submitted_day - due_day).days)


def late_penalty(lesson_date: Any, submitted_at: Any) -> int:
    return 1 if late_days(lesson_date, submitted_at) > 0 else 0


def parse_attendance_xlsx(content: bytes, group: GroupInfo) -> list[dict[str, Any]]:
    workbook = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
    worksheet = workbook.active
    headers = [cell.value for cell in next(worksheet.iter_rows(min_row=3, max_row=3))]
    columns = {
        "student_id": header_index(headers, "ID ученика", default=0),
        "name": header_index(headers, "Ученик", default=1),
        "group": header_index(headers, "Учебная группа", default=2),
        "discipline": header_index(headers, "Дисциплина", default=3),
        "lesson": header_index(headers, "Урок", default=4),
        "lesson_date": header_index(headers, "Дата урока", default=5),
        "lesson_score": header_index(headers, "Оценка за урок", default=8),
        "homework_score": header_index(headers, "Оценка за ДЗ", default=9),
        "checkpoint_score": header_index(headers, "Оценка за СР", default=10),
        "control_score": header_index(headers, "Оценка за КР", default=11),
        "assignment_status": header_index(headers, "Статус сдачи", default=17),
        "assignment_score": header_index(headers, "Оценка", default=18),
        "submitted_at": header_index(headers, "Дата сдачи", default=19),
    }
    students: dict[str, dict[str, Any]] = {}
    group_day_keys: list[str] = []
    group_day_key_set: set[str] = set()
    day_key_orders: dict[str, int] = {}
    current_day: dict[str, Any] | None = None

    def register_group_day(lesson: Any, lesson_date: Any) -> tuple[str, int]:
        raw_day_order = parse_lesson_number(lesson)
        day_key = lesson_day_key(lesson, lesson_date)
        if day_key not in group_day_key_set:
            group_day_key_set.add(day_key)
            group_day_keys.append(day_key)
            day_key_orders[day_key] = raw_day_order or len(group_day_keys)
        return day_key, day_key_orders[day_key]

    for row in worksheet.iter_rows(min_row=4, values_only=True):
        student_id = row_value(row, columns["student_id"])
        name = row_value(row, columns["name"])
        xlsx_group = row_value(row, columns["group"])
        discipline = row_value(row, columns["discipline"])
        lesson = row_value(row, columns["lesson"])
        lesson_date = row_value(row, columns["lesson_date"])
        lesson_score = row_value(row, columns["lesson_score"])
        homework_score = row_value(row, columns["homework_score"])
        checkpoint_score = row_value(row, columns["checkpoint_score"])
        control_score = row_value(row, columns["control_score"])
        assignment_status = row_value(row, columns["assignment_status"])
        assignment_score = row_value(row, columns["assignment_score"])
        submitted_at = row_value(row, columns["submitted_at"])
        score = score_value(checkpoint_score, control_score, homework_score, lesson_score)
        is_potential_day = is_day_lesson(lesson) or isinstance(lesson_date, (date, datetime))
        is_scored_day = score is not None and is_potential_day

        if not name:
            if current_day and (normalize_text(assignment_status) or assignment_score not in (None, "")):
                lesson_late_days = late_penalty(current_day.get("lessonDate"), submitted_at)
                late_by_lesson = current_day["item"].setdefault("_lateDaysByLesson", {})
                lesson_key = current_day["dayOrder"]
                previous = late_by_lesson.get(lesson_key)
                if previous is None or lesson_late_days < previous:
                    late_by_lesson[lesson_key] = lesson_late_days
                current_day["dailyScore"]["lateDays"] = lesson_late_days
            continue

        if current_day and not is_day_lesson(lesson) and (normalize_text(assignment_status) or assignment_score not in (None, "")):
            lesson_late_days = late_penalty(current_day.get("lessonDate"), submitted_at)
            late_by_lesson = current_day["item"].setdefault("_lateDaysByLesson", {})
            lesson_key = current_day["dayOrder"]
            previous = late_by_lesson.get(lesson_key)
            if previous is None or lesson_late_days < previous:
                late_by_lesson[lesson_key] = lesson_late_days
                current_day["dailyScore"]["lateDays"] = lesson_late_days
            continue

        if not discipline_matches_group(discipline, group):
            current_day = None
            continue

        day_key = ""
        day_order = 0
        if is_potential_day:
            day_key, day_order = register_group_day(lesson, lesson_date)

        if not name or not is_scored_day:
            current_day = None
            continue

        student_key = str(int(student_id)) if isinstance(student_id, (int, float)) else normalize_text(name)
        item = students.setdefault(
            student_key,
            {
                "subject": group.subject if group.subject != "без предмета" else infer_subject(discipline or xlsx_group or group.name),
                "level": infer_level(discipline) or infer_level(group.name),
                "group": normalize_text(xlsx_group) or group.name,
                "name": normalize_text(name),
                "teacher": group.teacher,
                "dailyScores": [],
                "_lateDaysByLesson": {},
            },
        )
        daily_score = {
            "dateKey": iso_date(lesson_date, shift_days=DEADLINE_SHIFT_DAYS),
            "dateLabel": date_label(lesson_date, shift_days=DEADLINE_SHIFT_DAYS),
            "dateOrder": day_order,
            "dayKey": day_key,
            "score": score,
            "lateDays": 0,
        }
        item["dailyScores"].append(daily_score)
        current_day = {
            "item": item,
            "dayOrder": day_order,
            "lessonDate": lesson_date,
            "dailyScore": daily_score,
        }

    rows: list[dict[str, Any]] = []
    group_days_total = len(group_day_keys)
    for item in students.values():
        daily_scores = item["dailyScores"]
        scores_by_day: dict[str, float] = {}
        for day in daily_scores:
            day_key = str(day.get("dayKey") or day.get("dateOrder") or "")
            if not day_key:
                continue
            scores_by_day[day_key] = max(scores_by_day.get(day_key, 0.0), float(day["score"]))
        scores = list(scores_by_day.values())
        days_done = len(scores_by_day)
        days_total = group_days_total or days_done
        quality = sum(scores) / days_done if days_done else 0.0
        coefficient = days_done / days_total if days_total else 0.0
        base_score = quality * coefficient
        penalty = float(sum(item.get("_lateDaysByLesson", {}).values()))
        final_score = max(0.0, base_score - penalty)
        item.pop("_lateDaysByLesson", None)
        for day in daily_scores:
            day.pop("dayKey", None)

        item.update(
            {
                "daysDone": days_done,
                "daysTotal": days_total,
                "coefficient": coefficient,
                "quality": quality,
                "baseScore": base_score,
                "penalty": penalty,
                "finalScore": final_score,
                "score": final_score,
            }
        )
        rows.append(item)

    return rows


def inspect_attendance_xlsx(content: bytes, sample_limit: int = 12) -> dict[str, Any]:
    workbook = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
    worksheet = workbook.active
    headers = [normalize_text(cell.value) for cell in next(worksheet.iter_rows(min_row=3, max_row=3))]
    columns = {
        "name": header_index(headers, "Ученик", default=1),
        "lesson": header_index(headers, "Урок", default=4),
        "lesson_date": header_index(headers, "Дата урока", default=5),
        "assignment_status": header_index(headers, "Статус сдачи", default=17),
        "assignment_score": header_index(headers, "Оценка", default=18),
        "submitted_at": header_index(headers, "Дата сдачи", default=19),
    }
    total_rows = 0
    day_rows = 0
    assignment_rows = 0
    submitted_rows = 0
    named_assignment_rows = 0
    unnamed_assignment_rows = 0
    samples: list[dict[str, Any]] = []

    for row in worksheet.iter_rows(min_row=4, values_only=True):
        total_rows += 1
        name = row_value(row, columns["name"])
        lesson = row_value(row, columns["lesson"])
        status = row_value(row, columns["assignment_status"])
        assignment_score = row_value(row, columns["assignment_score"])
        submitted_at = row_value(row, columns["submitted_at"])
        has_assignment = bool(normalize_text(status)) or assignment_score not in (None, "")

        if is_day_lesson(lesson):
            day_rows += 1
        if has_assignment:
            assignment_rows += 1
            if name:
                named_assignment_rows += 1
            else:
                unnamed_assignment_rows += 1
        if submitted_at:
            submitted_rows += 1
        if has_assignment and len(samples) < sample_limit:
            samples.append({
                "name": normalize_text(name),
                "lesson": normalize_text(lesson),
                "lessonDate": iso_date(row_value(row, columns["lesson_date"])),
                "status": normalize_text(status),
                "assignmentScore": normalize_text(assignment_score),
                "submittedAt": iso_date(submitted_at),
            })

    return {
        "headers": headers,
        "columns": columns,
        "stats": {
            "totalRows": total_rows,
            "dayRows": day_rows,
            "assignmentRows": assignment_rows,
            "submittedRows": submitted_rows,
            "namedAssignmentRows": named_assignment_rows,
            "unnamedAssignmentRows": unnamed_assignment_rows,
        },
        "samples": samples,
    }


def add_places(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_school: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        by_group[(row.get("subject", ""), row.get("level", ""), row.get("group", ""))].append(row)
        by_school[(row.get("subject", ""), row.get("level", ""))].append(row)

    def apply_place(items: list[dict[str, Any]], key: str) -> None:
        sorted_items = sorted(items, key=lambda row: float(row.get("finalScore") or 0), reverse=True)
        previous_score = None
        place = 0
        for item in sorted_items:
            score = float(item.get("finalScore") or 0)
            if score != previous_score:
                place += 1
                previous_score = score
            item[key] = place

    for items in by_group.values():
        apply_place(items, "groupPlace")
    for items in by_school.values():
        apply_place(items, "schoolPlace")

    return rows


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "subject": row.get("subject", ""),
        "level": row.get("level", ""),
        "name": row.get("name", ""),
        "teacher": row.get("teacher", "Без преподавателя"),
        "score": row.get("score", 0),
        "finalScore": row.get("finalScore", row.get("score", 0)),
        "groupPlace": row.get("groupPlace", 0),
        "schoolPlace": row.get("schoolPlace", 0),
    }


def strip_for_public(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "rows": [public_row(row) for row in payload.get("rows", [])],
    }


def load_ratings(
    period_from: str,
    period_to: str,
    group_ids: set[int] | None,
    subjects: set[str] | None,
    include_virtual: bool,
    origins: set[str] | None,
    limit: int | None,
) -> dict[str, Any]:
    groups = selected_groups(
        group_ids=group_ids,
        subjects=subjects,
        include_virtual=include_virtual,
        origins=origins,
    )
    effective_limit = limit if limit is not None else MAX_GROUPS_PER_REQUEST
    if effective_limit > 0 and len(groups) > effective_limit:
        groups = groups[:effective_limit]

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def load_group(group: GroupInfo):
        content = fetch_attendance_xlsx(group.id, period_from, period_to)
        return group, parse_attendance_xlsx(content, group)

    with ThreadPoolExecutor(max_workers=max(1, DEFAULT_CONCURRENCY)) as executor:
        futures = {executor.submit(load_group, group): group for group in groups}
        for future in as_completed(futures):
            group = futures[future]
            try:
                _, group_rows = future.result()
                rows.extend(group_rows)
            except Exception as error:
                errors.append({"groupId": group.id, "group": group.name, "error": str(error)})

    add_places(rows)

    return {
        "ok": True,
        "period": {"from": period_from, "to": period_to},
        "groups": [group.__dict__ for group in groups],
        "rows": rows,
        "errors": errors,
    }


def parse_int_set(value: str) -> set[int] | None:
    if not value:
        return None
    result = {int(part) for part in re.split(r"[,\s]+", value.strip()) if part}
    return result or None


def parse_str_set(value: str) -> set[str] | None:
    if not value:
        return None
    result = {infer_subject(part.strip()) for part in value.split(",") if part.strip()}
    result.discard("без предмета")
    return result or None


def parse_origin_set(value: str) -> set[str] | None:
    if not value:
        return None
    allowed = {
        "manual": "ManualGroup",
        "manualgroup": "ManualGroup",
        "auto": "AutoGroup",
        "autogroup": "AutoGroup",
    }
    result = set()
    for part in value.split(","):
        key = part.strip().lower()
        if not key:
            continue
        result.add(allowed.get(key, part.strip()))
    return result or None


def parse_limit(value: str) -> int | None:
    if not value:
        return None
    try:
        return max(0, int(value))
    except ValueError:
        raise BackendError("limit must be a number", HTTPStatus.BAD_REQUEST)


def current_month_range() -> tuple[str, str]:
    today = date.today()
    start = today.replace(day=1)
    if today.month == 12:
        end = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def admin_key_matches(value: str) -> bool:
    return bool(BACKEND_ADMIN_KEY) and value.strip() == BACKEND_ADMIN_KEY


class Handler(BaseHTTPRequestHandler):
    server_version = "SoholmsMarathonBackend/1.0"

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}

            if parsed.path == "/health":
                return self.send_json({"ok": True})

            if parsed.path == "/api/debug/auth":
                self.require_admin(query)
                return self.send_json({
                    "ok": True,
                    "apiToken": token_fingerprint("SOHOLMS_API_TOKEN"),
                    "excelToken": token_fingerprint("SOHOLMS_EXCEL_TOKEN"),
                    "fallbackToken": token_fingerprint("SOHOLMS_TOKEN"),
                    "cacheItems": len(_CACHE),
                })

            if parsed.path == "/api/cache/clear":
                self.require_admin(query)
                clear_cache()
                return self.send_json({"ok": True, "cacheItems": len(_CACHE)})

            if parsed.path == "/api/debug/xlsx":
                self.require_admin(query)
                group_id_value = query.get("groupId", "")
                period_from = query.get("periodFrom") or os.getenv("SOHOLMS_PERIOD_FROM") or load_group_config().get("periodFrom") or current_month_range()[0]
                period_to = query.get("periodTo") or os.getenv("SOHOLMS_PERIOD_TO") or load_group_config().get("periodTo") or current_month_range()[1]
                try:
                    group_id = int(group_id_value)
                except ValueError:
                    raise BackendError("groupId is required", HTTPStatus.BAD_REQUEST)
                content = fetch_attendance_xlsx(group_id, period_from, period_to)
                return self.send_json({
                    "ok": True,
                    "groupId": group_id,
                    "period": {"from": period_from, "to": period_to},
                    **inspect_attendance_xlsx(content),
                })

            if parsed.path == "/api/groups":
                search = query.get("search", "").strip()
                if search:
                    return self.send_json({
                        "ok": True,
                        "groups": search_groups(search),
                    })

                config = load_group_config()
                use_config = query.get("configured") == "1"
                configured_ids, missing_names, missing_candidates = resolve_config_group_ids(fetch_group_tree(), config) if use_config else (None, [], {})
                include_virtual = query.get("includeVirtual") == "1" or (use_config and bool(config.get("includeVirtual")))
                groups = selected_groups(
                    group_ids=parse_int_set(query.get("groupIds", "")) or configured_ids,
                    subjects=parse_str_set(query.get("subjects", "")),
                    include_virtual=include_virtual,
                    origins=parse_origin_set(query.get("origins", "")),
                )
                return self.send_json({
                    "ok": True,
                    "groups": [group.__dict__ for group in groups],
                    "missingConfigNames": missing_names,
                    "missingConfigCandidates": missing_candidates,
                })

            if parsed.path == "/api/ratings":
                default_from, default_to = current_month_range()
                config = load_group_config()
                configured_ids, missing_names, missing_candidates = resolve_config_group_ids(fetch_group_tree(), config)
                period_from = query.get("periodFrom") or os.getenv("SOHOLMS_PERIOD_FROM") or config.get("periodFrom") or default_from
                period_to = query.get("periodTo") or os.getenv("SOHOLMS_PERIOD_TO") or config.get("periodTo") or default_to
                group_ids = parse_int_set(query.get("groupIds", "")) or configured_ids or None
                subjects = parse_str_set(query.get("subjects", ""))
                include_virtual = query.get("includeVirtual") == "1" or bool(config.get("includeVirtual"))
                origins = parse_origin_set(query.get("origins", ""))
                limit = parse_limit(query.get("limit", ""))
                cache_key = f"ratings:{period_from}:{period_to}:{group_ids}:{subjects}:{include_virtual}:{origins}:{limit}"
                payload = cached(
                    cache_key,
                    DEFAULT_CACHE_SECONDS,
                    lambda: load_ratings(period_from, period_to, group_ids, subjects, include_virtual, origins, limit),
                )
                if query.get("public") == "1":
                    payload = strip_for_public(payload)
                if missing_names:
                    payload = {
                        **payload,
                        "missingConfigNames": missing_names,
                        "missingConfigCandidates": missing_candidates,
                    }
                return self.send_json(payload)

            self.send_json({"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)
        except BackendError as error:
            self.send_json({"ok": False, "error": str(error)}, error.status)
        except Exception as error:
            self.send_json({"ok": False, "error": str(error)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", os.getenv("CORS_ORIGIN", "*"))
        self.send_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "content-type,authorization,x-admin-key")
        self.send_header("Access-Control-Allow-Private-Network", "true")

    def require_admin(self, query: dict[str, str]) -> None:
        if not BACKEND_ADMIN_KEY:
            return
        value = self.headers.get("x-admin-key", "") or query.get("adminKey", "")
        if not admin_key_matches(value):
            raise BackendError("Forbidden", HTTPStatus.FORBIDDEN)

    def send_json(self, payload: Any, status: int = HTTPStatus.OK):
        body = json_bytes(payload)
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args):
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), fmt % args))


def main():
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8787"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Soholms backend listening on http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
