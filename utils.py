import os
import shutil
import re
import logging
from datetime import datetime
from config import SESSIONS_DIR
import functools

logger = logging.getLogger(__name__)


# Кэш для ускорения преобразования времени
@functools.lru_cache(maxsize=512)
def time_to_minutes_cached(time_str):
    """Кэшированная версия преобразования времени в минуты"""
    return _time_to_minutes_impl(time_str)


def _time_to_minutes_impl(time_str):
    """Конвертирует время в минуты (для начала занятия)"""
    try:
        # Удаляем все пробелы
        clean_str = re.sub(r'\s+', '', time_str)

        # Пытаемся распарсить как диапазон
        if '–' in clean_str or '-' in clean_str:
            separator = '–' if '–' in clean_str else '-'
            time_part = clean_str.split(separator)[0]
        else:
            time_part = clean_str

        # Парсим время
        if ':' in time_part:
            parts = time_part.split(':')
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
        else:
            # Если время без разделителя (например, "900")
            if len(time_part) == 3:
                hours = int(time_part[0])
                minutes = int(time_part[1:3])
            elif len(time_part) == 4:
                hours = int(time_part[0:2])
                minutes = int(time_part[2:4])
            else:
                logger.warning(f"Неподдерживаемый формат времени: {time_str}")
                return 0

        return hours * 60 + minutes
    except Exception as e:
        logger.error(f"Ошибка преобразования времени: {time_str} - {e}")
        return 0


def time_to_minutes(time_str):
    """Публичная функция с кэшированием"""
    return time_to_minutes_cached(time_str)


def create_user_session(user_id):
    """Создает папку сессии для пользователя"""
    session_dir = f"{SESSIONS_DIR}/{user_id}"
    input_dir = f"{session_dir}/input_schedules"
    os.makedirs(input_dir, exist_ok=True)
    return session_dir


def cleanup_user_session(user_id):
    """Очищает папку сессии пользователя"""
    session_dir = f"{SESSIONS_DIR}/{user_id}"
    if os.path.exists(session_dir):
        shutil.rmtree(session_dir)


def normalize_day_name(day):
    """Нормализует название дня недели"""
    days_map = {
        'пн': 'понедельник',
        'вт': 'вторник',
        'ср': 'среда',
        'чт': 'четверг',
        'пт': 'пятница',
        'сб': 'суббота',
        'вс': 'воскресенье'
    }
    day = day.lower().strip()
    return days_map.get(day, day)


def parse_time(time_str):
    """Парсит строку времени в формате 'HH:MM–HH:MM'"""
    try:
        # Ищем время в строке
        match = re.search(r'(\d{1,2}):(\d{2})[–-](\d{1,2}):(\d{2})', time_str)
        if match:
            start_hour = int(match.group(1))
            start_min = int(match.group(2))
            end_hour = int(match.group(3))
            end_min = int(match.group(4))
            return (start_hour, start_min), (end_hour, end_min)

        # Альтернативный формат без минут
        match = re.search(r'(\d{1,2})[–-](\d{1,2})', time_str)
        if match:
            start_hour = int(match.group(1))
            end_hour = int(match.group(2))
            return (start_hour, 0), (end_hour, 0)

        logger.warning(f"Не удалось распарсить время: {time_str}")
        return None
    except Exception as e:
        logger.error(f"Ошибка парсинга времени: {e}")
        return None