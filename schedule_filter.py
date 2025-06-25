# schedule_filter.py
import os
import json
import requests
import re
import logging
from config import YANDEX_GPT_API_KEY, YANDEX_GPT_URL, SESSIONS_DIR
from utils import time_to_minutes, normalize_day_name, parse_time
from collections import defaultdict

logger = logging.getLogger(__name__)

PROMPT_TEMPLATE = """Ты — помощник по составлению расписания. На основе пожеланий пользователя сформируй JSON с фильтрами.

Важно:
1. Все предметы обязательные - НЕ фильтруем по названиям предметов.
2. Работаем только с параметрами расписания и преподавателями.
3. Ответ должен содержать ТОЛЬКО валидный JSON.
4. Не добавляй фильтры, которые не указаны в запросе пользователя.
5. Не добавляй в фильтры то, что указано в скобочках, если этого не указывал пользователь.
6. Если пользователь указывает предмет с преподавателем - используй ТОЛЬКО фильтр preferred_subject_teachers
7. Никогда не используй teacher_roles (этот фильтр запрещен)
8. Все временные ограничения должны быть преобразованы в соответствующие фильтры
9. Если пользователь указывает "до X часов", используй preferred_end_time
10. Если "после X часов" - preferred_start_time
11. Комбинации типа "с X до Y" - оба фильтра
12. Все временные ограничения типа "до 16" преобразуй в "preferred_end_time": "16:00"
13. Дни недели указывай в нижнем регистре (понедельник, вторник)
14. Для исключения групп используй фильтр excluded_groups в формате: {"excluded_groups": [{"предмет": "Математика", "группа": "АТ-01"}]}

Доступные фильтры:
- exclude_days: ["понедельник"] - дни для исключения
- max_classes_per_day: {{"пятница": 1}} - макс. пар в день
- preferred_days: ["вторник", "четверг"] - только эти дни
- no_morning_classes: true - нет пар до 10:00
- no_evening_classes: true - нет пар после 18:00
- preferred_start_time: "10:00" - начало не раньше
- preferred_end_time: "16:00" - конец не позже
- min_break: 20 - мин. перерыв между парами (мин)
- no_gaps: true - нет окон между парами
- max_daily_teachers: 4 - макс. преподавателей в день
- preferred_teachers: ["Лавров"] - только эти преподав.
- excluded_teachers: ["Смирнов"] - исключить этих преподав.
- preferred_subject_teachers: {{"Математика": ["Иванов"]}} - преподаватели для конкретных предметов
- excluded_groups: [{{"предмет": "Математика", "группа": "АТ-01"}}] - исключить эти группы
- max_teacher_classes_per_day: {{"Лавров": 1}} - макс. пар/день
- no_consecutive_teacher_classes: true - не две подряд

Примеры запросов и соответствующих фильтров:
1. "Не хочу пар в понедельник" → {{"exclude_days": ["понедельник"]}}
2. "Пары только по вторникам и четвергам" → {{"preferred_days": ["вторник", "четверг"]}}
3. "Хочу, чтобы лекции вел только Лавров" → {{"preferred_teachers": ["Лавров"]}}
4. "Хочу чтобы математику вел Иванов" → {{"preferred_subject_teachers": {{"Математика": ["Иванов"]}}}}
5. "хочу выходной в среду" → {"exclude_days": ["среда"]}
6. "пары до 15" → {"preferred_end_time": "15:00"}
7. "не хочу пар в пятницу и после 16" → {"exclude_days": ["пятница"], "preferred_end_time": "16:00"}
8. "исключи группу Векторный анализ АТ-03" → {"excluded_groups": [{"предмет": "Векторный анализ", "группа": "АТ-03"}]}

Пожелания пользователя: "{user_input}"
"""


def generate_filters(user_input):
    """Генерирует фильтры на основе пользовательского ввода"""
    headers = {
        "Authorization": f"Api-Key {YANDEX_GPT_API_KEY}",
        "Content-Type": "application/json"
    }

    prompt = PROMPT_TEMPLATE.replace("{user_input}", user_input.replace("{", "{{").replace("}", "}}"))

    data = {
        "modelUri": "gpt://b1gu5hu4elo0ishbti6b/yandexgpt-lite",
        "completionOptions": {
            "stream": False,
            "temperature": 0.3,
            "maxTokens": 1000
        },
        "messages": [{
            "role": "user",
            "text": prompt
        }]
    }

    try:
        response = requests.post(YANDEX_GPT_URL, headers=headers, json=data, timeout=15)
        response.raise_for_status()
        result = response.json()
        text = result['result']['alternatives'][0]['message']['text'].strip()
        if not text:
            raise ValueError("Пустой ответ от Yandex GPT API")

        # Очистка ответа
        text = text.strip('`').strip()
        if text.startswith('json'):
            text = text[4:].strip()

        return json.loads(text)
    except Exception as e:
        logger.error(f"Ошибка генерации фильтров: {e}")
        return _fallback_filters(user_input)


def _fallback_filters(user_input):
    """Фолбэк-режим для генерации фильтров"""
    filters = {}

    # Обработка дней недели
    days = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
    exclude_days = [day for day in days if day in user_input.lower()]
    if exclude_days:
        filters["exclude_days"] = exclude_days

    # Обработка времени
    time_patterns = [
        (r"до (\d{1,2})", "preferred_end_time"),
        (r"после (\d{1,2})", "preferred_start_time"),
        (r"с (\d{1,2}) до (\d{1,2})", "time_range")
    ]

    for pattern, filter_type in time_patterns:
        match = re.search(pattern, user_input)
        if match:
            if filter_type == "time_range":
                start = match.group(1).zfill(2)
                end = match.group(2).zfill(2)
                filters["preferred_start_time"] = f"{start}:00"
                filters["preferred_end_time"] = f"{end}:00"
            else:
                hour = match.group(1).zfill(2)
                filters[filter_type] = f"{hour}:00"

    # Обработка связки предмет-преподаватель
    subject_teacher_pattern = r"((?:[А-Яа-я]+\s?)+)\s+(?:вести|вестил?|преподавать?|вести\s+)?\s*([А-Яа-я]+)"
    matches = re.findall(subject_teacher_pattern, user_input)
    if matches:
        preferred_subject_teachers = {}
        for subject, teacher in matches:
            subject = subject.strip().lower().capitalize()
            teacher = teacher.strip()
            if subject not in preferred_subject_teachers:
                preferred_subject_teachers[subject] = []
            preferred_subject_teachers[subject].append(teacher)
        filters["preferred_subject_teachers"] = preferred_subject_teachers
        
    # Обработка исключения групп
    exclude_pattern = r"исключи(?:те)?\s+группу?\s*([\w\s]+)\s+([\w\d-]+)"
    matches = re.findall(exclude_pattern, user_input, re.IGNORECASE)
    if matches:
        excluded_groups = []
        for subject, group in matches:
            excluded_groups.append({
                "предмет": subject.strip(),
                "группа": group.strip()
            })
        filters["excluded_groups"] = excluded_groups

    return filters





def apply_filters_to_list(user_id, filters, schedules_list):
    """Применяет фильтры к уже отфильтрованному списку расписаний"""
    output_file = f"{SESSIONS_DIR}/{user_id}/matched_schedules.json"
    
    try:
        # Применяем основные фильтры
        matched = [s for s in schedules_list if _matches_filters(s, filters)]
        
        # Применяем фильтр исключения групп
        if "excluded_groups" in filters:
            filtered = []
            for schedule in matched:
                keep = True
                for exclusion in filters["excluded_groups"]:
                    # Нормализуем название предмета и группы
                    if isinstance(exclusion, dict):
                        subject = normalize_name(exclusion.get("предмет", ""))
                        group = normalize_group(exclusion.get("группа", ""))
                    else:
                        # Обработка строкового формата
                        parts = exclusion.split(" ", 1)
                        subject = normalize_name(parts[0]) if len(parts) > 0 else ""
                        group = normalize_group(parts[1]) if len(parts) > 1 else ""
                    
                    # Пропускаем пустые значения
                    if not subject or not group:
                        continue
                        
                    for s_subject in schedule["предметы"]:
                        # Нормализуем название предмета в расписании
                        s_name = normalize_name(s_subject["название_предмета"])
                        s_group = normalize_group(str(s_subject["группа"]))
                        
                        if s_name == subject and s_group == group:
                            keep = False
                            break
                    if not keep:
                        break
                if keep:
                    filtered.append(schedule)
            matched = filtered

        result = {
            "filters": filters,
            "matched_schedules": matched,
            "count": len(matched)
        }

        # Сохраняем результат
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return len(matched)
    
    except Exception as e:
        logger.error(f"Ошибка в apply_filters_to_list: {e}")
        return 0

def normalize_name(name: str) -> str:
    """Нормализует название предмета для сравнения"""
    if not name:
        return ""
    # Приводим к нижнему регистру, удаляем пробелы, заменяем разделители
    return name.strip().lower().replace(" ", "").replace("_", "").replace("-", "")

def normalize_group(group: str) -> str:
    """Нормализует название группы для сравнения"""
    if not group:
        return ""
    # Приводим к верхнему регистру, удаляем пробелы
    return group.strip().upper().replace(" ", "")

def apply_filters(user_id, filters):
    """Применяет фильтры к ВСЕМ расписаниям пользователя"""
    schedules_file = f"{SESSIONS_DIR}/{user_id}/schedules.json"
    output_file = f"{SESSIONS_DIR}/{user_id}/matched_schedules.json"

    try:
        # Чтение всех расписаний из файла
        with open(schedules_file, 'r', encoding='utf-8-sig') as f:
            schedules = json.load(f)
        
        return apply_filters_to_list(user_id, filters, schedules)
    
    except Exception as e:
        logger.error(f"Критическая ошибка в apply_filters: {e}")
        raise


def _matches_filters(schedule, filters):
    """Проверяет соответствие расписания фильтрам"""
    if not filters or not schedule:
        return False

    # Собираем статистику по дням
    day_stats = {}
    subject_teachers = {}  # Для фильтрации по связке предмет-преподаватель

    for subject in schedule["предметы"]:
        subject_name = subject["название_предмета"]
        subject_teachers[subject_name] = set()

        for cls in subject["занятия"]:
            day = normalize_day_name(cls["день"])

            if day not in day_stats:
                day_stats[day] = {
                    'classes': [],
                    'teachers': set()
                }

            # Парсим время с помощью parse_time
            start_time, end_time = 0, 0
            if "время" in cls and cls["время"]:
                try:
                    time_range = parse_time(cls["время"])
                    if time_range:
                        (start_hour, start_min), (end_hour, end_min) = time_range
                        start_time = start_hour * 60 + start_min
                        end_time = end_hour * 60 + end_min
                except Exception as e:
                    logger.warning(f"Ошибка парсинга времени: {e}")

            # Сохраняем данные о занятии
            teachers_list = [t.strip() for t in cls["преподаватели"]]
            class_data = {
                'start': start_time,
                'end': end_time,
                'teachers': teachers_list,
                'subject': subject_name
            }

            day_stats[day]['classes'].append(class_data)
            day_stats[day]['teachers'].update(teachers_list)
            subject_teachers[subject_name].update(teachers_list)

    # 1. Проверка фильтра по связке предмет-преподаватель
    if "preferred_subject_teachers" in filters:
        for subject_name, required_teachers in filters["preferred_subject_teachers"].items():
            subject_found = False
            for s in schedule["предметы"]:
                if s["название_предмета"] != subject_name:
                    continue

                # Проверяем преподавателей для этого предмета
                teacher_match = False
                for cls in s["занятия"]:
                    if any(t in required_teachers for t in cls["преподаватели"]):
                        teacher_match = True
                        break

                if not teacher_match:
                    return False
                subject_found = True
                break

            if not subject_found:
                return False

    # 2. Исключенные дни
    if "exclude_days" in filters:
        for day in filters["exclude_days"]:
            if day in day_stats:
                return False

    # 3. Время начала/окончания
    if "preferred_start_time" in filters:
        min_start = time_to_minutes(filters["preferred_start_time"])
        for day, stats in day_stats.items():
            for cls in stats['classes']:
                if cls['start'] < min_start:
                    return False

    if "preferred_end_time" in filters:
        max_end = time_to_minutes(filters["preferred_end_time"])
        for day, stats in day_stats.items():
            for cls in stats['classes']:
                if cls['end'] > max_end:
                    return False

    # 4. Общие фильтры по преподавателям
    if "preferred_teachers" in filters:
        found = False
        for day, stats in day_stats.items():
            for cls in stats['classes']:
                if any(teacher in filters["preferred_teachers"] for teacher in cls['teachers']):
                    found = True
                    break
            if found:
                break
        if not found:
            return False

    if "excluded_teachers" in filters:
        for day, stats in day_stats.items():
            for cls in stats['classes']:
                if any(teacher in filters["excluded_teachers"] for teacher in cls['teachers']):
                    return False

    return True


def generate_report(user_id):
    """Генерирует отчет для пользователя"""
    input_file = f"{SESSIONS_DIR}/{user_id}/matched_schedules.json"
    output_file = f"{SESSIONS_DIR}/{user_id}/schedules_report.txt"

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        matched_schedules = data.get("matched_schedules", [])
        report_lines = ["Вам подходят следующие расписания:\n\n"]

        for i, schedule in enumerate(matched_schedules, 1):
            subjects_info = []
            for subject in schedule["предметы"]:
                subject_str = f"{subject['название_предмета']} ({subject['группа']})"
                if subject_str not in subjects_info:
                    subjects_info.append(subject_str)

            schedule_line = f"Расписание #{i}:\n"
            schedule_line += f"• Предметы: {', '.join(subjects_info)}\n"

            # Добавляем информацию о преподавателях
            teachers_info = defaultdict(list)
            for subject in schedule["предметы"]:
                for cls in subject["занятия"]:
                    for teacher in cls["преподаватели"]:
                        if subject['название_предмета'] not in teachers_info[teacher]:
                            teachers_info[teacher].append(subject['название_предмета'])

            if teachers_info:
                schedule_line += "• Преподаватели:\n"
                for teacher, subjects in teachers_info.items():
                    schedule_line += f"  - {teacher}: {', '.join(subjects)}\n"

            report_lines.append(schedule_line + "\n")

        with open(output_file, 'w', encoding='utf-8') as f_out:
            f_out.writelines(report_lines)

        return output_file

    except Exception as e:
        logger.error(f"Ошибка генерации отчета: {e}")
        return None