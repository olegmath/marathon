"""Тесты аудита учебной статистики: невидимые ДЗ мат10, dense ranking, посещаемость.

Запуск: python3 test_journal_metrics.py
"""
import soholms_backend as sb


def _ev(student, group, lesson, ev_type=None, *, g_dz=None, g_kr=None,
        status="Был", date="2025-10-01", subs=None, subject="математика"):
    return {
        "student": student,
        "group": group,
        "_subject": subject,
        "_teacher": "Учитель",
        "_level": "ege",
        "_grade": "10",
        "discipline": "Основной курс по математике",
        "lesson": lesson,
        "type": ev_type if ev_type is not None else (lesson.split()[0] if lesson else ""),
        "date": date,
        "status": status,
        "g_lesson": None,
        "g_dz": g_dz,
        "g_sr": None,
        "g_kr": g_kr,
        "sub_rows": subs or [],
    }


def _sub(status):
    return {"task_kind": "квиз", "task_class": "", "status": status}


def _agg_for(events, student, group):
    hw_max, kr_max = sb._journal_max_per_lesson(events)
    student_events = [e for e in events if e["student"] == student and e["group"] == group]
    return sb._journal_aggregate_one(student_events, hw_max, kr_max)


# --- 1. Формат мат10: несданные ДЗ должны быть видны в total/trend -----------

def test_mat10_invisible_homework():
    G = "мат10ТЕСТ"
    # Уроки названы «2-4.» — type не «Домашка». У A оценка только за один из двух
    # ДЗ-уроков группы; у B оценки за оба. Третий урок — занятие без ДЗ вовсе.
    events = [
        _ev("Ученик А", G, "2-4. Домашка 5. Тема", "2-4.", g_dz=8,
            subs=[_sub("Принято")]),
        _ev("Ученик А", G, "2-5. Домашка 6. Тема", "2-5.",
            subs=[_sub("Не открывали")]),          # не сдал: g_dz пустой
        _ev("Ученик А", G, "1-1. Лекция", "1-1."),  # не ДЗ-урок (ни у кого нет g_dz)
        _ev("Ученик Б", G, "2-4. Домашка 5. Тема", "2-4.", g_dz=10,
            subs=[_sub("Принято")]),
        _ev("Ученик Б", G, "2-5. Домашка 6. Тема", "2-5.", g_dz=10,
            subs=[_sub("Принято")]),
        _ev("Ученик Б", G, "1-1. Лекция", "1-1."),
    ]
    a = _agg_for(events, "Ученик А", G)
    b = _agg_for(events, "Ученик Б", G)

    # До фикса: у А total == 1 («3/3 = 100%» эффект). Должно быть 2.
    assert a["homework"]["total"] == 2, a["homework"]
    assert a["homework"]["count"] == 1, a["homework"]
    # Несданный урок виден в тренде как not_done
    not_done = [t for t in a["homework"]["trend"] if t["not_done"]]
    assert len(not_done) == 1 and not_done[0]["lesson"].startswith("2-5."), a["homework"]["trend"]
    # Подзадачи несданного урока участвуют в done_pct: 1 Принято из 2 подзадач
    assert a["homework"]["done_pct"] == 50.0, a["homework"]
    # Лекция без ДЗ ни у кого — не считается ДЗ-уроком
    assert b["homework"]["total"] == 2 and b["homework"]["count"] == 2, b["homework"]


# --- 2. Регрессия стандартного формата: type == «Домашка» --------------------

def test_standard_homework_regression():
    G = "инф10ТЕСТ"
    events = [
        _ev("Ученик А", G, "Домашка 1", "Домашка", g_dz=100, subs=[_sub("Принято")]),
        _ev("Ученик А", G, "Домашка 2", "Домашка", subs=[_sub("Не открывали")]),
        _ev("Ученик А", G, "Занятие 1", "Занятие", g_kr=80),
        _ev("Ученик Б", G, "Домашка 1", "Домашка", g_dz=50, subs=[_sub("Принято")]),
        _ev("Ученик Б", G, "Занятие 1", "Занятие", g_kr=100),
    ]
    a = _agg_for(events, "Ученик А", G)
    assert a["homework"]["total"] == 2 and a["homework"]["count"] == 1, a["homework"]
    assert a["homework"]["avg"] == 100.0, a["homework"]     # 100 из группового max 100
    assert a["homework"]["done_pct"] == 50.0, a["homework"]  # 1 Принято из 2 подзадач
    # Занятие с КР не становится ДЗ-уроком
    assert all("Занятие" not in t["lesson"] for t in a["homework"]["trend"])
    # КР нормируется по max группы
    assert a["kr"]["avg"] == 80.0 and a["kr"]["count"] == 1, a["kr"]
    # КР-знаменатель группы: 1 КР-урок
    assert a["kr"]["total"] == 1, a["kr"]


# --- 3. Посещаемость: обе цифры, болезнь не в основном проценте --------------

def test_attendance_two_percentages():
    G = "мат10ТЕСТ"
    events = []
    statuses = ["Был"] * 26 + ["Не был"] * 4 + ["Болел"] * 2
    for i, st in enumerate(statuses):
        events.append(_ev("Ученик А", G, f"1-{i}. Урок", "1-" + str(i) + ".",
                          status=st, date=f"2025-10-{(i % 28) + 1:02d}"))
    a = _agg_for(events, "Ученик А", G)
    att = a["attendance"]
    assert att["pct"] == 81.2, att            # 26/32 — с болезнями (справочно)
    assert att["pct_excl_sick"] == 86.7, att  # 26/30 — основной
    assert att["total"] == 32 and att["sick"] == 2, att
    # Помесячно тоже обе цифры
    m = att["by_month"][0]
    assert "pct_excl_sick" in m, m
    # Интеграл строится на проценте без болезней (все ДЗ/КР пустые)
    assert a["integral"] == 86.7, a["integral"]


# --- 4. Dense ranking в рангах журнала ---------------------------------------

def test_dense_ranking():
    G = "мат10ТЕСТ"
    events = []
    # Интеграл = посещаемость (ДЗ/КР нет): A=100, B=100, C=50
    for student, statuses in [
        ("Ученик А", ["Был", "Был"]),
        ("Ученик Б", ["Был", "Был"]),
        ("Ученик В", ["Был", "Не был"]),
    ]:
        for i, st in enumerate(statuses):
            events.append(_ev(student, G, f"1-{i}. Урок", f"1-{i}.",
                              status=st, date=f"2025-10-{i + 1:02d}"))
    res = sb.aggregate_student_journal(events, "Ученик В")
    ranks = res["subjects"][0]["ranks"]
    assert ranks["in_group"]["place"] == 2, ranks   # dense: 1, 1, 2
    assert ranks["in_group"]["of"] == 3, ranks
    assert ranks["in_school"]["place"] == 2, ranks

    res_a = sb.aggregate_student_journal(events, "Ученик А")
    res_b = sb.aggregate_student_journal(events, "Ученик Б")
    assert res_a["subjects"][0]["ranks"]["in_group"]["place"] == 1
    assert res_b["subjects"][0]["ranks"]["in_group"]["place"] == 1


def main():
    test_mat10_invisible_homework()
    test_standard_homework_regression()
    test_attendance_two_percentages()
    test_dense_ranking()
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()


