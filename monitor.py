import sqlite3
import time
import os
import requests
import threading
import queue
import sys
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

MAX_ROWS = 14
CHECK_INTERVAL = 5

# Эти параметры будут переопределены из sys.argv
DB_NAME = 'cars.db'
OUTPUT_FILE = 'allowed.txt'
MAX_CHAT_ID = os.getenv('MAX_CHAT_ID')
CITY_NAME = "КРЫМ-АВТО" # По умолчанию

MAX_BOT_TOKEN = os.getenv('MAX_BOT_TOKEN')

# Очередь для сообщений в MAX
message_queue = queue.Queue()

def max_worker():
    """Фоновый поток для отправки сообщений в MAX."""
    while True:
        item = message_queue.get()
        if item is None:
            break
        
        message, chat_id = item
            
        if not MAX_BOT_TOKEN or not chat_id:
            message_queue.task_done()
            continue
            
        url = "https://platform-api.max.ru/messages"
        params = {
            "v": "1.0.0",
            "chat_id": int(chat_id)
        }
        headers = {
            "Authorization": MAX_BOT_TOKEN,
            "Content-Type": "application/json"
        }
        payload = {
            "text": message,
            "attachments": [],
            "link": None,
            "format": "markdown"
        }
        
        try:
            resp = requests.post(url, params=params, headers=headers, json=payload, timeout=15)
            if resp.status_code != 200:
                print(f"MAX API Error {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"MAX communication error: {e}")
        finally:
            message_queue.task_done()

# Запуск фонового потока
threading.Thread(target=max_worker, daemon=True).start()

def get_rows(db_path):
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT type, model, number, event_time, id
            FROM events
            WHERE event_date = ?
            ORDER BY id DESC
            LIMIT ?
        """, (datetime.now().strftime("%d.%m.%Y"), MAX_ROWS))
        rows = cursor.fetchall()
        conn.close()
        return list(reversed(rows))
    except sqlite3.OperationalError:
        return []

def format_row(index, move_type, model, number, event_time):
    short_model = str(model).split()[0] if model else "---"
    return f"{str(index):<3} | {move_type:<7} | {short_model:<10} | {number:<12} | {event_time:<9}"

def write_file(rows, output_file, city):
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    header = (
        f"ПРОПУСКА {city.upper()} | Дата: {now_str}\n"
        + "-" * 55 + "\n"
        + f"{'№':<3} | {'Тип':<7} | {'Модель':<10} | {'Номер':<12} | {'Время':<9}\n"
        + "-" * 55 + "\n"
    )

    content = ""
    for i, row in enumerate(rows, 1):
        move_type, model, number, event_time, _id = row
        content += format_row(i, move_type, model, number, event_time) + "\n"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(header + content)

def main():
    if len(sys.argv) < 5:
        print("Ошибка! Формат: python monitor.py <ПУТЬ_К_БАЗЕ> <ВЫХОДНОЙ_ФАЙЛ> <CHAT_ID> <ГОРОД>")
        sys.exit(1)

    db_path = sys.argv[1]
    output_file = sys.argv[2]
    chat_id = sys.argv[3]
    city = sys.argv[4]

    print(f"Монитор запущен ({city}). Ожидание данных в {db_path}...")
    
    last_processed_id = None

    while True:
        try:
            rows = get_rows(db_path)
            if rows:
                write_file(rows, output_file, city)

                # Логика уведомлений MAX
                if last_processed_id is None:
                    last_processed_id = max(row[4] for row in rows)
                else:
                    new_rows = [row for row in rows if row[4] > last_processed_id]
                    for row in new_rows:
                        move_type, model, number, event_time, row_id = row
                        message = (
                            f"**ПРОПУСК ({city}): {move_type}**\n"
                            f"**Номер:** {number or '---'}\n"
                            f"**Модель:** {model or '---'}\n"
                            f"**Время:** {event_time}"
                        )
                        message_queue.put((message, chat_id))
                        print(f"[{city}] [MAX] Уведомление отправлено: {number} ({move_type})")
                        if row_id > last_processed_id:
                            last_processed_id = row_id
            elif os.path.exists(db_path):
                 # Если база есть, но записей на сегодня нет, все равно пишем пустой заголовок
                 write_file([], output_file, city)

        except Exception as e:
            print(f"Ошибка монитора ({city}): {e}")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
