from playwright.sync_api import sync_playwright
import json
import re
import os
from collections import defaultdict

def normalize_day(day_str: str) -> str:
    mapping = {
        "пн": "понедельник",
        "вт": "вторник",
        "ср": "среда",
        "чт": "четверг",
        "пт": "пятница",
        "сб": "суббота",
        "вс": "воскресенье"
    }
    parts = day_str.split()
    if not parts:
        return day_str
    abbrev = parts[0].lower().replace(".", "")
    return mapping.get(abbrev, parts[0])

def parse_schedule(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=0)
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=120000)
            if "sso.urfu.ru" in page.url:
                page.wait_for_url("**/schedule-calendar/**", timeout=120000)
            page.wait_for_selector(".fc-view-container", timeout=30000)
            page.wait_for_selector(".fc-event", timeout=30000)
            headers = []
            for th in page.query_selector_all(".fc-day-header"):
                d = th.get_attribute("data-date")
                day_el = th.query_selector("span")
                t = day_el.inner_text().strip() if day_el else ""
                if d and t:
                    headers.append((d, t))
            data = defaultdict(list)
            events = page.locator(".fc-event")
            total = events.count()
            for i in range(total):
                e = events.nth(i)
                td_idx = e.evaluate(
                    """el => {
                        const td = el.closest('td');
                        return td && td.parentElement ? Array.from(td.parentElement.children).indexOf(td) : -1;
                    }"""
                )
                raw_day = headers[td_idx - 1][1] if 1 <= td_idx <= len(headers) else "?"
                day_name = normalize_day(raw_day)
                e.hover()
                page.wait_for_timeout(100)
                raw_title = e.locator(".fc-title").inner_text().strip() if e.locator(".fc-title").count() > 0 else ""
                title_no_num = re.sub(r'\s*\d+\s*$', '', raw_title)
                subject = raw_title.split("/")[0].strip() if "/" in raw_title else title_no_num
                time_txt = e.locator(".fc-time").inner_text().strip() if e.locator(".fc-time").count() > 0 else ""
                start = e.locator(".fc-time").get_attribute("data-start") if e.locator(".fc-time").count() > 0 else None
                if not start:
                    m = re.search(r"\d{1,2}:\d{2}", time_txt)
                    start = m.group(0) if m else "?"
                kind = "Лекция" if "лекц" in raw_title.lower() else "Практика"
                e.click()
                page.wait_for_timeout(300)
                team_raw = page.locator("p:has-text('Команда') span.pull-right.team")
                team = team_raw.inner_text().strip() if team_raw.count() > 0 else ""
                m = re.search(r"(АТ[-]?\d+)", team, re.I)
                team = m.group(1) if m else "?"
                t_loc = page.locator("li:has(.title:has-text('Преподаватели:')) div.ng-star-inserted")
                teachers = set()
                for j in range(t_loc.count()):
                    raw = t_loc.nth(j).inner_text().strip()
                    for part in raw.splitlines():
                        n = part.strip()
                        if n and "преподаватели" not in n.lower():
                            teachers.add(n)
                a_loc = page.locator("li:has-text('Место и время')")
                place_and_time = ""
                if a_loc.count() > 0:
                    raw_text = a_loc.first.inner_text().strip()
                    place_and_time = re.sub(r"^\(?\s*Место и время\s*\)?\s*", "", raw_text, flags=re.IGNORECASE).strip()
                else:
                    a_loc = page.locator("li:has-text('Место')")
                    if a_loc.count() > 0:
                        raw_text = a_loc.first.inner_text().strip()
                        place_and_time = re.sub(r"^\(?\s*Место\s*\)?\s*", "", raw_text, flags=re.IGNORECASE).strip()
                entry = {
                    "предмет": subject,
                    "тип занятия": kind,
                    "день": day_name,
                    "преподаватели": list(teachers),
                    "Место и время": place_and_time,
                    "команда": team
                }
                data[team].append(entry)
                page.click("body")
                page.wait_for_timeout(100)
            return data
        except Exception as e:
            print("Ошибка:", e)
            return None
        finally:
            context.close()
            browser.close()

if __name__ == "__main__":
    url = input("Введите ссылку расписания: ")
    result = parse_schedule(url)
    if result:
        directory = "input_schedules"
        if not os.path.exists(directory):
            os.makedirs(directory)
        subject_data = {}
        for team, entries in result.items():
            for entry in entries:
                subj = entry.get("предмет", "Без названия")
                subject_data.setdefault(subj, []).append(entry)
        for subj, entries in subject_data.items():
            safe_name = re.sub(r'[\\/*?:"<>|]', "", subj)
            filename = os.path.join(directory, f"{safe_name}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2, ensure_ascii=False)
            print("Сохранено:", filename)
    else:
        print("Ошибка парсинга")

