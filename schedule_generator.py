import os
import json
import re
import random
import logging
from collections import defaultdict
from config import MAX_SCHEDULES, SESSIONS_DIR
from utils import parse_time, normalize_day_name

logger = logging.getLogger(__name__)


def generate_schedules(user_id):
    """Генерирует расписания для пользователя"""
    logger.info(f"Начало генерации расписаний для пользователя {user_id}")
    input_dir = f"{SESSIONS_DIR}/{user_id}/input_schedules"
    output_file = f"{SESSIONS_DIR}/{user_id}/schedules.json"

    if not os.path.exists(input_dir):
        logger.error(f"Директория не существует: {input_dir}")
        return 0

    subject_teams = defaultdict(lambda: defaultdict(list))
    file_count = 0

    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            file_count += 1
            json_file = os.path.join(input_dir, filename)
            logger.info(f"Обработка файла: {json_file}")

            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    subject_name = os.path.splitext(filename)[0]

                    for lesson in data:
                        # Проверка обязательных полей
                        if 'день' not in lesson or 'команда' not in lesson:
                            logger.warning(f"Пропущен урок без дня или команды в {filename}")
                            continue

                        lesson['день'] = normalize_day_name(lesson['день'])
                        team = lesson['команда']

                        # Добавляем название предмета в урок
                        lesson['предмет'] = subject_name
                        subject_teams[subject_name][team].append(lesson)

                logger.info(f"Файл {filename} обработан: {len(data)} уроков")
            except Exception as e:
                logger.error(f"Ошибка обработки файла {filename}: {e}")

    logger.info(f"Обработано файлов: {file_count}, предметов: {len(subject_teams)}")

    if not subject_teams:
        return 0

    try:
        schedules = _generate_valid_schedules(subject_teams)
        logger.info(f"Найдено валидных расписаний: {len(schedules)}")

        saved_count = _save_schedules_to_json(schedules, output_file, MAX_SCHEDULES)
        logger.info(f"Сохранено расписаний: {saved_count}")
        return saved_count
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}")
        return 0


def _generate_valid_schedules(subject_teams):
    """Генерирует валидные расписания с оптимизацией"""
    if not subject_teams:
        return []

    subjects = list(subject_teams.keys())
    valid_schedules = []
    schedule_count = 0
    max_attempts = MAX_SCHEDULES * 10

    logger.info(f"Генерация расписаний для {len(subjects)} предметов")

    for attempt in range(max_attempts):
        all_lessons = []
        schedule_info = []
        valid = True

        for subject in subjects:
            teams = list(subject_teams[subject].items())
            if not teams:
                valid = False
                break

            team, lessons = random.choice(teams)
            all_lessons.extend(lessons)
            schedule_info.append((subject, team))

        if not valid:
            continue

        if _validate_full_schedule(all_lessons):
            valid_schedules.append((schedule_info, all_lessons))
            schedule_count += 1

            if schedule_count >= MAX_SCHEDULES:
                logger.info(f"Достигнут лимит после {attempt + 1} попыток")
                break

    logger.info(f"Сгенерировано расписаний: {len(valid_schedules)}")
    return valid_schedules


def _validate_full_schedule(lessons):
    """Проверяет полное расписание на пересечения"""
    day_lessons = defaultdict(list)

    for lesson in lessons:
        day = lesson.get('день', '')
        day_lessons[day].append(lesson)

    for day, day_list in day_lessons.items():
        if not _validate_day_schedule(day_list):
            return False

    return True


def _validate_day_schedule(lessons):
    """Проверяет расписание на один день"""
    lessons.sort(key=lambda x: parse_time(x.get('Место и время', '')) or ((0, 0), (0, 0)))

    prev_end = 0
    for lesson in lessons:
        time_range = parse_time(lesson.get('Место и время', ''))
        if not time_range:
            continue

        (start_hour, start_min), (end_hour, end_min) = time_range
        start_time = start_hour * 60 + start_min
        end_time = end_hour * 60 + end_min

        if start_time < prev_end:
            return False

        prev_end = end_time

    return True


def _save_schedules_to_json(valid_schedules, output_file, max_schedules):
    """Сохраняет расписания в JSON"""
    if not valid_schedules:
        return 0

    schedules_to_save = valid_schedules[:max_schedules]
    output_data = []

    for i, (schedule_info, all_lessons) in enumerate(schedules_to_save, 1):
        schedule_data = {"id_расписания": i, "предметы": []}
        subject_lessons = defaultdict(list)

        for lesson in all_lessons:
            key = (lesson.get('предмет', ''), lesson.get('команда', ''))
            subject_lessons[key].append(lesson)

        for (subject, team), lessons in subject_lessons.items():
            subject_data = {
                "название_предмета": subject,
                "группа": team,
                "занятия": []
            }

            for lesson in lessons:
                time_str = lesson.get('Место и время', '')
                time_val = ""
                location = ""

                if time_str:
                    match = re.search(r'\d{1,2}:\d{2}–\d{1,2}:\d{2}', time_str)
                    if match:
                        time_val = match.group(0)
                        location = time_str.replace(time_val, '').strip()

                lesson_data = {
                    "тип_занятия": lesson.get('тип занятия', ''),
                    "день": lesson.get('день', ''),
                    "время": time_val,
                    "преподаватели": lesson.get('преподаватели', []),
                    "аудитория": location
                }
                subject_data["занятия"].append(lesson_data)

            schedule_data["предметы"].append(subject_data)

        output_data.append(schedule_data)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)

    return len(schedules_to_save)