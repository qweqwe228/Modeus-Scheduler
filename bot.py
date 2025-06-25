# bot.py
import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from html import escape
from typing import List, Dict, Any

from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

from config import BOT_TOKEN, SESSIONS_DIR
from schedule_filter import generate_filters, apply_filters, apply_filters_to_list
from schedule_generator import generate_schedules
from utils import create_user_session, cleanup_user_session

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния беседы
UPLOADING, FILTERING, REVIEWING = range(3)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик команды /start - начало работы с ботом."""
    user = update.message.from_user
    logger.info(f"Начало сессии для пользователя {user.id}")

    # Очистка предыдущей сессии
    cleanup_user_session(user.id)
    create_user_session(user.id)
    
    # Сброс данных пользователя
    context.user_data.clear()

    await update.message.reply_text(
        "👋 Привет! Я помогу тебе составить идеальное расписание.\n\n"
        "📤 Пожалуйста, отправь мне все JSON-файлы с расписаниями предметов. "
        "Когда закончишь, нажми /done.\n\n"
        "❗ Важно: каждый файл должен содержать расписание одного предмета "
        "с разными группами и преподавателями.\n\n"
        "-----------------------------------\n\n"
        "Ссылка на загрузку файлов демо-варианта:\n\n"
        "https://drive.google.com/drive/folders/1qNJ8Opc5M2NMcnF-rMag1-RW3lLMytWJ\n\n"
        "-----------------------------------\n\n"
        "(бот работает в тестовом режиме)"
    )
    return UPLOADING

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик загрузки документов."""
    user = update.message.from_user
    document = update.message.document

    # Проверка формата
    if not document.file_name.lower().endswith('.json'):
        await update.message.reply_text("❌ Пожалуйста, отправляй только JSON-файлы.")
        return UPLOADING

    # Проверка размера файла
    if document.file_size > 2 * 1024 * 1024:  # 2 MB
        await update.message.reply_text("❌ Файл слишком большой! Максимальный размер 2 МБ.")
        return UPLOADING

    # Создание директории
    input_dir = f"{SESSIONS_DIR}/{user.id}/input_schedules"
    os.makedirs(input_dir, exist_ok=True)

    # Скачивание файла
    file = await context.bot.get_file(document.file_id)
    file_path = os.path.join(input_dir, document.file_name)
    await file.download_to_drive(file_path)

    await update.message.reply_text(
        f"✅ Файл {document.file_name} успешно получен! "
        "Можешь отправить следующий файл или нажми /done для завершения загрузки."
    )

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            json.load(f)  # Проверяем валидность JSON
    except json.JSONDecodeError:
        os.remove(file_path)
        await update.message.reply_text("❌ Файл повреждён. Отправьте корректный JSON-файл.")
    return UPLOADING

async def done_uploading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик завершения загрузки файлов."""
    user = update.message.from_user
    await update.message.reply_text("⏳ Начинаю генерацию расписаний... Это может занять несколько минут.")

    try:
        logger.info(f"Запуск генерации расписаний для {user.id}")
        await update.message.reply_chat_action(action="typing")

        # Запускаем генерацию с таймаутом
        try:
            count = await asyncio.wait_for(
                asyncio.to_thread(generate_schedules, user.id),
                timeout=300  # 5 минут таймаут
            )
        except asyncio.TimeoutError:
            await update.message.reply_text(
                "⏱️ Генерация заняла слишком много времени. "
                "Попробуй уменьшить количество групп в файлах и начни заново /start"
            )
            return ConversationHandler.END

        if count == 0:
            await update.message.reply_text(
                "😢 Не удалось сгенерировать ни одного расписания. Возможные причины:\n"
                "• Нет файлов в папке или они повреждены\n"
                "• В расписаниях есть конфликты времени\n"
                "• Слишком много возможных комбинаций групп\n\n"
                "Попробуй уменьшить количество групп и начни заново /start"
            )
            return ConversationHandler.END

        await update.message.reply_text(
            f"🎉 Успешно сгенерировано {count} расписаний!\n\n"
            "📝 Теперь расскажи, какое расписание ты хочешь?\n"
            "Примеры запросов:\n"
            "• 'Не хочу пар в понедельник'\n"
            "• 'Пары только до 16 часов'\n"
            "• 'Хочу чтобы математику вел Иванов'\n"
            "• 'Исключить преподавателя Петрова'"
        )
        return FILTERING
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}", exc_info=True)
        await update.message.reply_text("😢 Произошла ошибка при генерации расписаний. Попробуй еще раз /start")
        return ConversationHandler.END

async def handle_preferences(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик предпочтений пользователя по фильтрации расписаний."""
    user = update.message.from_user
    user_input = update.message.text

    try:
        await update.message.reply_text("🔍 Анализирую твои пожелания...")
        await update.message.reply_chat_action(action="typing")
        
        # Определяем тип запроса
        is_adjustment = context.user_data.get('is_adjustment', False)
        is_exclusion = context.user_data.get('is_exclusion', False)
        
        # Для корректировки объединяем запросы
        if is_adjustment:
            original_query = context.user_data.get('original_query', "")
            full_query = f"{original_query}. {user_input}"
        elif is_exclusion:
            # Для исключения групп используем специальный формат
            full_query = f"Исключи группу: {user_input}"
        else:
            full_query = user_input
            context.user_data['original_query'] = user_input

        # Генерируем фильтры
        filters_data = generate_filters(full_query)
        
        # Для корректировки объединяем с предыдущими фильтрами
        if is_adjustment or is_exclusion:
            prev_filters = context.user_data.get('current_filters', {})
            
            # Упрощенное объединение фильтров
            merged_filters = {}
            
            # Копируем все простые значения из предыдущих фильтров
            for key, value in prev_filters.items():
                if not isinstance(value, (list, dict)):
                    merged_filters[key] = value
            
            # Копируем все простые значения из новых фильтров
            for key, value in filters_data.items():
                if not isinstance(value, (list, dict)):
                    merged_filters[key] = value
            
            # Обрабатываем списки
            for key in set(prev_filters.keys()) | set(filters_data.keys()):
                merged_list = []
                
                # Добавляем значения из предыдущих фильтров
                if key in prev_filters and isinstance(prev_filters[key], list):
                    for item in prev_filters[key]:
                        # Для словарей используем кортежи для хеширования
                        if isinstance(item, dict):
                            merged_list.append(tuple(sorted(item.items())))
                        else:
                            merged_list.append(item)
                
                # Добавляем значения из новых фильтров
                if key in filters_data and isinstance(filters_data[key], list):
                    for item in filters_data[key]:
                        if isinstance(item, dict):
                            merged_list.append(tuple(sorted(item.items())))
                        else:
                            merged_list.append(item)
                
                # Удаляем дубликаты
                if merged_list:
                    unique_list = []
                    seen = set()
                    for item in merged_list:
                        if item not in seen:
                            seen.add(item)
                            unique_list.append(item)
                    
                    # Преобразуем кортежи обратно в словари
                    final_list = []
                    for item in unique_list:
                        if isinstance(item, tuple):
                            final_list.append(dict(item))
                        else:
                            final_list.append(item)
                    
                    merged_filters[key] = final_list
            
            filters_data = merged_filters

        # Применяем фильтры
        if is_adjustment or is_exclusion:
            # Берем предыдущий отфильтрованный набор
            previous_matched = context.user_data.get('matched_schedules', [])
            matched_count = apply_filters_to_list(
                user.id, 
                filters_data, 
                previous_matched
            )
        else:
            # При первом запросе применяем ко всем расписаниям
            matched_count = apply_filters(user.id, filters_data)
        
        # Получаем matched_schedules
        matched_schedules = _get_matched_schedules(user.id)
        
        # Сохраняем данные для пагинации и корректировки
        context.user_data['matched_schedules'] = matched_schedules
        context.user_data['total_count'] = matched_count
        context.user_data['current_filters'] = filters_data
        context.user_data['shown_index'] = 0  # Сбрасываем индекс показа
        
        # Показываем первые 3 расписания
        await _send_schedules_message(
            update=update,
            context=context,
            schedules=matched_schedules,
            start_index=0,
            limit=3,
            total_count=matched_count
        )
        
        # Переходим в состояние просмотра результатов
        return REVIEWING

    except Exception as e:
        logger.error(f"Ошибка фильтрации: {e}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте сформулировать запрос иначе.")
        return FILTERING


def _get_matched_schedules(user_id: int) -> List[dict]:
    """Получает отфильтрованные расписания из файла."""
    matched_file = f"{SESSIONS_DIR}/{user_id}/matched_schedules.json"
    
    try:
        with open(matched_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("matched_schedules", [])
    except Exception as e:
        logger.error(f"Ошибка чтения файла расписаний: {e}")
        return []

async def _send_schedules_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    schedules: List[dict],
    total_count: int,
    start_index: int = 0,
    limit: int = 3
) -> None:
    """Функция отправки расписаний с пагинацией."""
    try:
        # Рассчитываем индексы для отображения
        end_index = min(start_index + limit, len(schedules))
        
        # Формируем заголовок с информацией о пагинации
        header = (
            f"<b>🎯 Найдено {total_count} вариантов (показаны {start_index+1}-{end_index}):</b>\n\n"
            "Формат каждого расписания:\n"
            "1. <b>Предмет</b> (группа)\n"
            "2. <b>Преподаватели</b>\n"
            "3. <b>Расписание по дням</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )
        await update.message.reply_text(header, parse_mode=ParseMode.HTML)

        # Обрабатываем запрошенный диапазон расписаний
        for i, schedule in enumerate(schedules[start_index:end_index], start_index+1):
            try:
                subjects = defaultdict(lambda: {'groups': set(), 'teachers': set()})
                days = defaultdict(list)
                
                for subject in schedule["предметы"]:
                    name = clean_text(subject["название_предмета"])
                    raw_group = subject["группа"]
                    
                    # Обработка группы в зависимости от предмета
                    if "английский" in name.lower():
                        group = extract_english_group(str(raw_group))
                    else:
                        group = clean_text(raw_group)
                    
                    subjects[name]['groups'].add(group)
                    
                    for cls in subject["занятия"]:
                        # Преподаватели
                        for teacher in cls["преподаватели"]:
                            if teacher.strip():
                                subjects[name]['teachers'].add(clean_text(teacher))
                        
                        # Расписание
                        day = clean_text(cls.get("день", "")).lower()
                        time = clean_text(cls.get("время", "??:??")).replace("–", "-")
                        lesson_type = clean_text(cls.get("тип_занятия", ""))
                        
                        if day and time:
                            days[day].append(f"{time} {name} (гр. {group}, {lesson_type})")

                # Формируем сообщение
                message = [
                    f"<b>📋 Вариант {i}:</b>",
                    "<b>📚 Предметы:</b>"
                ]
                
                # Предметы и группы
                for name, data in subjects.items():
                    groups = ", ".join(sorted(data['groups']))
                    message.append(f"• <b>{name}</b> ({groups})")
                
                # Преподаватели
                message.append("\n<b>👨‍🏫 Преподаватели:</b>")
                for name, data in subjects.items():
                    teachers = ", ".join(sorted(data['teachers'])) if data['teachers'] else "не указаны"
                    message.append(f"• <b>{name}</b>: {teachers}")
                
                # Расписание
                message.append("\n<b>🗓 Расписание:</b>")
                for day in ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота"]:
                    if day in days:
                        message.append(f"\n▸ <b>{day.capitalize()}:</b>")
                        for lesson in sorted(days[day]):
                            message.append(f"   ‣ {lesson}")

                # Отправляем сообщение
                full_msg = "\n".join(message)
                if len(full_msg) > 4000:
                    for part in [full_msg[i:i+4000] for i in range(0, len(full_msg), 4000)]:
                        await update.message.reply_text(part, parse_mode=ParseMode.HTML)
                else:
                    await update.message.reply_text(full_msg, parse_mode=ParseMode.HTML)

            except Exception as e:
                logger.error(f"Ошибка обработки расписания #{i}: {e}")
                continue

        # Обновляем индекс последнего показанного
        context.user_data['shown_index'] = end_index
        
        # Финальное сообщение с опциями
        footer = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<b>Доступные действия:</b>\n"
            "• /next - Показать следующие 3 расписания\n"
            "• /adjust - Уточнить критерии поиска\n"
            "• /exclude - Исключить группу\n"
            "• /new - Начать новый поиск"
        )
        await update.message.reply_text(footer, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        await update.message.reply_text(
            "⚠️ Произошла ошибка при отображении расписаний",
            parse_mode=ParseMode.HTML
        )

def clean_text(text: str) -> str:
    """Очистка текста от спецсимволов."""
    if not text:
        return "не указано"
    return escape(str(text).strip())

def extract_english_group(group: str) -> str:
    """Специальная обработка групп для английского языка."""
    group = str(group).upper().replace(" ", "")
    
    # Варианты написания "CP" (кириллица и латиница)
    cp_patterns = [
        r"(СР|CP)[-_]?\d{2,3}",  # СР-15, CP17, СР_05
        r"\d{2,3}/\d{2,3}",       # 15/17
        r"(?<=СР|CP)\d{2,3}",     # СР15, CP05
    ]
    
    for pattern in cp_patterns:
        match = re.search(pattern, group)
        if match:
            # Нормализуем формат: CP-XX
            found = match.group()
            if "/" in found:
                return f"CP-{found.split('/')[0]}"
            elif "_" in found:
                return found.replace("_", "-")
            elif not "-" in found and any(c.isalpha() for c in found):
                nums = re.sub(r"\D", "", found)
                prefix = "CP" if "CP" in found else "СР"
                return f"{prefix}-{nums}"
            return found
    
    # Если не нашли стандартный формат, возвращаем первые цифры
    numbers = re.search(r"\d{2,3}", group)
    return f"CP-{numbers.group()}" if numbers else "CP-?"

async def next_schedules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показать следующие расписания"""
    user = update.message.from_user
    user_data = context.user_data
    
    shown_index = user_data.get('shown_index', 0)
    matched_schedules = user_data.get('matched_schedules', [])
    total_count = user_data.get('total_count', 0)
    
    # Проверяем, есть ли еще расписания
    if shown_index >= len(matched_schedules):
        await update.message.reply_text("ℹ️ Больше нет доступных расписаний.")
        return REVIEWING
    
    # Показываем следующие 3 расписания
    await _send_schedules_message(
        update=update,
        context=context,
        schedules=matched_schedules,
        start_index=shown_index,
        limit=3,
        total_count=total_count
    )
    
    return REVIEWING

async def adjust_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запустить корректировку запроса"""
    context.user_data['is_adjustment'] = True
    await update.message.reply_text(
        "📝 Введите уточнения к вашему запросу:\n"
        "Пример: 'И добавьте чтобы не было пар после 17:00'"
    )
    return FILTERING

async def exclude_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запустить исключение группы"""
    context.user_data['is_exclusion'] = True
    await update.message.reply_text(
        "🚫 Введите группу для исключения в формате:\n"
        "Предмет Группа\n"
        "Пример: 'Векторный анализ АТ-03'\n\n"
        "❗ Важно: используйте точное название предмета, как в расписании\n"
        "Если название содержит подчеркивание, вводите его с подчеркиванием"
    )
    return FILTERING

async def new_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начать новый поиск"""
    return await start(update, context)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик команды отмены."""
    user = update.message.from_user
    cleanup_user_session(user.id)
    context.user_data.clear()
    await update.message.reply_text("🗑️ Сессия завершена. Начни заново с /start.")
    return ConversationHandler.END

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    error = context.error
    logger.error("Ошибка в боте:", exc_info=error)
    
    if isinstance(error, json.JSONDecodeError):
        msg = "⚠️ Ошибка обработки данных. Попробуйте заново отправить файлы."
    else:
        msg = "💥 Произошла непредвиденная ошибка. Попробуй /start"
    
    if update and hasattr(update, 'message'):
        await update.message.reply_text(msg)

    if isinstance(error, TypeError) and "unhashable type" in str(error):
        msg = "⚠️ Произошла ошибка обработки фильтров. Попробуйте другой запрос."
        await update.message.reply_text(msg)

def main():
    """Основная функция запуска бота."""
    # Создаем папку сессий, если не существует
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    application = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            UPLOADING: [
                MessageHandler(filters.Document.ALL, handle_document),
                CommandHandler('done', done_uploading)
            ],
            FILTERING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_preferences)
            ],
            REVIEWING: [
                CommandHandler('next', next_schedules),
                CommandHandler('adjust', adjust_query),
                CommandHandler('exclude', exclude_group),
                CommandHandler('new', new_search),
                CommandHandler('cancel', cancel)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    logger.info("Бот запущен и ожидает сообщений...")
    application.run_polling()

if __name__ == '__main__':
    main()