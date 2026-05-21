"""Тест агрегации журнала: completion (сделано/было) + качество для ДЗ и КР.

Запуск: python3 test_journal_completion.py
"""
import soholms_backend as sb


def _ev(student, group, lesson, ev_type, *, g_dz=None, g_kr=None, status="Был", subs=None):
    return {
        "student": student,
        "group": group,
        "_subject": "Информатика",
        "_teacher": "Учитель",
        "_level": "ege",
        "_grade": "10",
        "discipline": "Основной курс по информатике",
        "lesson": lesson,
        "type": ev_type,
        "date": "2025-09-01",
        "status": status,
        "g_lesson": None,
        "g_dz": g_dz,
        "g_sr": None,
        "g_kr": g_kr,
        "sub_rows": subs or [],
    }


def _sub(status):
    return {"task_kind": "квиз", "task_class": "", "status": status}


def build_events():
    A, B, G = "Иванов Иван", "Петров Пётр", "ГР1"
    return [
        # ДЗ 1
        _ev(A, G, "Домашка 1", "Домашка", g_dz=5,
            subs=[_sub("Принято"), _sub("Принято"), _sub("Не открывали")]),
        _ev(B, G, "Домашка 1", "Домашка", g_dz=4, subs=[_sub("Принято")]),
        # ДЗ 2
        _ev(A, G, "Домашка 2", "Домашка", g_dz=4, subs=[_sub("Принято")]),
        _ev(B, G, "Домашка 2", "Домашка", g_dz=5, subs=[_sub("Принято")]),
        # КР: уроки «Занятие N» с g_kr
        _ev(A, G, "Занятие 1", "Занятие", g_kr=80),
        _ev(B, G, "Занятие 1", "Занятие", g_kr=100),
        _ev(A, G, "Занятие 2", "Занятие", g_kr=90),
        # B не писал Занятие 2 → в группе всего 2 КР-урока
    ]


def by_name(students, name_part):
    return next(s for s in students if name_part in s["name"])


def main():
    res = sb.aggregate_school_journal(build_events(), "students")
    students = res["students"]
    A = by_name(students, "Иванов")
    B = by_name(students, "Петров")

    # --- Качество (как раньше, нормировка по max в группе) ---
    assert A["hwAvg"] == 90.0, A["hwAvg"]          # (100 + 80) / 2
    assert A["krAvg"] == 90.0, A["krAvg"]          # (80 + 100) / 2
    assert B["hwAvg"] == 90.0, B["hwAvg"]          # (80 + 100) / 2
    assert B["krAvg"] == 100.0, B["krAvg"]

    # --- НОВОЕ: ДЗ сделано % = «Принято» / все задания внутри ДЗ (по подзадачам) ---
    assert A["hwDonePct"] == 75.0, A["hwDonePct"]  # 3 Принято из 4 подзадач
    assert B["hwDonePct"] == 100.0, B["hwDonePct"] # 2 из 2

    # --- НОВОЕ: КР сделано % = написанные КР / все КР-уроки группы (=2) ---
    assert A["krTotal"] == 2, A["krTotal"]
    assert B["krTotal"] == 2, B["krTotal"]
    assert A["krDonePct"] == 100.0, A["krDonePct"] # 2/2
    assert B["krDonePct"] == 50.0, B["krDonePct"]  # 1/2

    # --- Групповая сводка содержит средние по новым полям ---
    groups = sb.aggregate_school_journal(build_events(), "groups")["groups"]
    g = groups[0]
    assert g["hwDonePct"] == 87.5, g["hwDonePct"]  # (75 + 100) / 2
    assert g["krDonePct"] == 75.0, g["krDonePct"]  # (100 + 50) / 2

    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
