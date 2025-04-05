from playwright.sync_api import sync_playwright
import json
import re
from collections import defaultdict


def parse_schedule(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
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
                date = th.get_attribute("data-date")
                day = th.query_selector("span")
                txt = day.inner_text().strip() if day else ""
                if date and txt:
                    headers.append((date, txt))

            events = page.locator(".fc-event")
            total = events.count()

            data = defaultdict(lambda: {"Лекция": [], "Практика": []})

            for i in range(total):
                e = events.nth(i)
                td_idx = e.evaluate(
                    """el => {
                        const td = el.closest('td');
                        if (!td || !td.parentElement) return -1;
                        return Array.from(td.parentElement.children).indexOf(td);
                    }"""
                )
                day_name = headers[td_idx - 1][1] if 1 <= td_idx <= len(headers) else "?"

                e.hover()
                page.wait_for_timeout(200)

                title_el = e.locator(".fc-title")
                time_el = e.locator(".fc-time")

                title = title_el.inner_text().strip() if title_el else ""
                time_txt = time_el.inner_text().strip() if time_el else ""
                start = time_el.get_attribute("data-start") if time_el else None
                if not start:
                    m = re.search(r"\d{1,2}:\d{2}", time_txt)
                    start = m.group(0) if m else "?"

                kind = "Лекция" if "лекц" in title.lower() else "Практика"

                e.click()
                page.wait_for_timeout(600)

                team_el = page.locator("p:has-text('Команда') span.pull-right.team")
                team_el.wait_for(state="visible", timeout=5000)
                team = team_el.inner_text().strip() if team_el else ""
                m = re.search(r"(АТ[-]?\d+)", team, re.I)
                team = m.group(1) if m else "?"

                t_loc = page.locator("li:has(.title:has-text('Преподаватели:')) div.ng-star-inserted")
                teachers = set()
                for j in range(t_loc.count()):
                    t = t_loc.nth(j).inner_text().strip()
                    if t and "преподаватели" not in t.lower():
                        teachers.add(t)

                entry = {
                    "день недели": day_name,
                    "время": start,
                    "заголовок": title,
                    "преподаватели": list(teachers)
                }

                data[team][kind].append(entry)

                page.click("body")
                page.wait_for_timeout(200)

            for team in data:
                for kind in data[team]:
                    seen = set()
                    for ev in data[team][kind]:
                        ev["преподаватели"] = [t for t in ev["преподаватели"] if not (t in seen or seen.add(t))]

            return data

        except Exception:
            return None
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    url = (
        "https://urfu.modeus.org/schedule-calendar/my?"
        "eventsFilter=%7B%22courseUnit%22:%5B%7B%22key%22:%22dd548c3d-2b69-4191-9a9e-ad348a9844ec%22%7D%5D,"
        "%22attendee%22:%5B%5D%7D&timeZone=%22Asia%2FYekaterinburg%22"
        "&calendar=%7B%22view%22:%22agendaWeek%22,%22date%22:%222025-03-17%22%7D&grid=%22Grid.07%22"
    )

    result = parse_schedule(url)
    if result:
        with open("schedule_by_team.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
