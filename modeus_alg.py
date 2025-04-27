import os
import json
import re
import datetime
import itertools

required_types = {
    "Введение в инженерную деятельность": {"Практика"},
    "Векторный анализ": {"Лекция", "Практика"},
    "Дополнительные главы математики": {"Лекция", "Практика"},
    "Сбор и верификация данных": {"Практика"}
}

def find_json_files(directory):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.lower().endswith(".json"):
                files.append(os.path.join(root, filename))
    return files

def load_events(file_path):
    events = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return events
        for event in data:
            if not all(key in event for key in ("предмет", "день", "Место и время", "команда")):
                continue
            s = event.get("Место и время", "")
            m = re.search(r'(\d{1,2}:\d{2})\s*[–-]\s*(\d{1,2}:\d{2})', s)
            if m:
                event["start"] = m.group(1).lstrip("0")
                event["end"] = m.group(2).lstrip("0")
            elif "время" in event and event["время"].strip():
                m2 = re.search(r'(\d{1,2}:\d{2})\s*[–-]\s*(\d{1,2}:\d{2})', event["время"])
                if m2:
                    event["start"] = m2.group(1).lstrip("0")
                    event["end"] = m2.group(2).lstrip("0")
                else:
                    continue
            else:
                continue
            event["day"] = event.get("день").strip().lower()
            events.append(event)
    except Exception:
        pass
    return events

def load_all_events(folder):
    files = find_json_files(folder)
    all_events = []
    for f in files:
        all_events.extend(load_events(f))
    return all_events

def build_subject_dict(events):
    subject_dict = {}
    for event in events:
        subject = event.get("предмет") or event.get("subject") or event.get("заголовок")
        if not subject:
            continue
        team = event.get("команда") or event.get("team")
        subject_dict.setdefault(subject, set()).add(team)
    return subject_dict

def generate_selections(subject_dict, limit_comb=10**6):
    subjects = list(subject_dict.keys())
    teams = [list(subject_dict[subj]) for subj in subjects]
    count = 0
    for combination in itertools.product(*teams):
        yield dict(zip(subjects, combination))
        count += 1
        if count >= limit_comb:
            break

def build_schedule_from_selection(events, selection, time_map):
    schedule = {}
    for event in events:
        subject = event.get("предмет") or event.get("subject") or event.get("заголовок")
        if subject in selection and (event.get("команда") or event.get("team")) == selection[subject]:
            if subject in required_types and event.get("тип занятия") not in required_types[subject]:
                continue
            day = event.get("day")
            if not day:
                continue
            time_start = event.get("start")
            if not time_start:
                continue
            slot = time_map.get(time_start)
            if not slot:
                continue
            if day not in schedule:
                schedule[day] = {}
            schedule[day][slot] = event
    return schedule

def verify_required_sessions(schedule, selection):
    all_events = []
    for day in schedule:
        for evt in schedule[day].values():
            all_events.append(evt)
    subjects_in_schedule = {}
    for event in all_events:
        subject = event.get("предмет") or event.get("subject") or event.get("заголовок")
        session_type = event.get("тип занятия")
        if subject not in subjects_in_schedule:
            subjects_in_schedule[subject] = set()
        subjects_in_schedule[subject].add(session_type)
        sel_team = selection.get(subject)
        if sel_team and (event.get("команда") or event.get("team")) != sel_team:
            raise ValueError(f"Несовпадение команды для {subject}: ожидается {sel_team}")
    for subject, req in required_types.items():
        if subject in selection:
            existing = subjects_in_schedule.get(subject, set())
            if not req.issubset(existing):
                raise ValueError(f"Отсутствуют обязательные занятия для {subject}. Требуется {req}, найдено {existing}")

def check_conflicts(schedule):
    for day, slots in schedule.items():
        events_by_team = {}
        for event in slots.values():
            team = event.get("команда") or event.get("team")
            if team not in events_by_team:
                events_by_team[team] = []
            try:
                start_time = datetime.datetime.strptime(event.get("start"), "%H:%M")
                end_time = datetime.datetime.strptime(event.get("end"), "%H:%M")
            except Exception:
                continue
            events_by_team[team].append((start_time, end_time))
        for team, events in events_by_team.items():
            events.sort(key=lambda x: x[0])
            for i in range(len(events) - 1):
                if events[i][1] > events[i + 1][0]:
                    raise ValueError(f"Временной конфликт для группы {team} в {day}")

def generate_valid_schedules(events, subject_dict, time_map, limit=10):
    valid_schedules = []
    for selection in generate_selections(subject_dict):
        sched = build_schedule_from_selection(events, selection, time_map)
        try:
            verify_required_sessions(sched, selection)
            check_conflicts(sched)
        except ValueError:
            continue
        valid_schedules.append((selection, sched))
        if len(valid_schedules) >= limit:
            break
    return valid_schedules

def schedule_to_chronological(schedule):
    week_order = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    slot_order = {"1 пара": 1, "2 пара": 2, "3 пара": 3, "4 пара": 4, "5 пара": 5, "6 пара": 6, "7 пара": 7, "8 пара": 8}
    ordered = {}
    for day in week_order:
        if day in schedule:
            sorted_slots = sorted(schedule[day].items(), key=lambda x: slot_order.get(x[0], 99))
            ordered[day] = [event for slot, event in sorted_slots]
    return ordered

def rename_time_keys(schedule):
    new_schedule = {}
    for day, slots in schedule.items():
        new_slots = {}
        for slot, event in slots.items():
            new_event = dict(event)
            if "start" in new_event:
                new_event["начало"] = new_event.pop("start")
            if "end" in new_event:
                new_event["конец"] = new_event.pop("end")
            if "day" in new_event:
                new_event.pop("day")
            new_slots[slot] = new_event
        new_schedule[day] = new_slots
    return new_schedule

def main():
    input_dir = "input_schedules"
    output_dir = "output_schedules"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    events = load_all_events(input_dir)
    subject_dict = build_subject_dict(events)
    time_map = {
        "8:30": "1 пара",
        "10:15": "2 пара",
        "12:00": "3 пара",
        "14:15": "4 пара",
        "16:00": "5 пара",
        "17:40": "6 пара",
        "19:15": "7 пара",
        "20:50": "8 пара"
    }
    valid_schedules = generate_valid_schedules(events, subject_dict, time_map, limit=10)
    result = {}
    schedule_number = 1
    for selection, sched in valid_schedules:
        renamed_sched = rename_time_keys(sched)
        result[f"{schedule_number} расписание"] = {"выбранные группы": selection, "расписание": renamed_sched}
        schedule_number += 1
    output_file = os.path.join(output_dir, "schedule.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
