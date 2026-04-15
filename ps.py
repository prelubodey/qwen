import imaplib
import email
import sqlite3
import time
import os
import sys
import re
from email.header import decode_header
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
IMAP_SERVER = 'mail.rbauto.ru'
CHECK_INTERVAL = 15

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Таблица данных
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id TEXT UNIQUE,
            type TEXT,
            model TEXT,
            number TEXT,
            vin TEXT,
            client TEXT,
            document TEXT,
            repair_type TEXT,
            event_date TEXT,
            event_time TEXT,
            authorized_by TEXT,
            reason TEXT,
            raw_subject TEXT
        )
    ''')
    # Таблица состояния
    cursor.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)')
    # Индекс для производительности
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_msg_id ON events(msg_id)')
    conn.commit()
    return conn


def get_last_uid(cursor):
    cursor.execute("SELECT value FROM state WHERE key = 'last_uid'")
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def set_last_uid(cursor, uid):
    cursor.execute(
        "INSERT OR REPLACE INTO state (key, value) VALUES ('last_uid', ?)",
        (str(uid),)
    )


def extract_field(field_name, text):
    """Ищет значение до конца строки, игнорирует регистр."""
    pattern = fr"(?i){field_name}\s*:\s*([^\n\r]+)"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else "---"


def get_email_body(msg):
    """Надёжный выбор между HTML и Plain Text, игнорируя вложения."""
    html = None
    plain = None

    for part in msg.walk():
        ctype = part.get_content_type()
        cdisp = str(part.get("Content-Disposition") or "")
        if "attachment" in cdisp.lower():
            continue

        payload = part.get_payload(decode=True)
        if payload is None:
            continue

        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, "replace")
        except Exception:
            text = payload.decode("utf-8", "replace")

        if ctype == "text/html":
            html = text
        elif ctype == "text/plain":
            plain = text

    body = html or plain
    if not body:
        return ""

    if html:
        soup = BeautifulSoup(body, 'html.parser')
        for br in soup.find_all("br"):
            br.replace_with("\n")
        return soup.get_text()
    return body


def decode_mime_header(s):
    """Компактное декодирование MIME-заголовков."""
    parts = decode_header(s or "")
    decoded = ""
    for part, enc in parts:
        if isinstance(part, bytes):
            decoded += part.decode(enc or 'utf-8', 'replace')
        else:
            decoded += str(part)
    return decoded


def process_emails(target_folder, db_path):
    conn = init_db(db_path)
    cursor = conn.cursor()

    current_last_uid = get_last_uid(cursor)
    max_uid_processed = current_last_uid
    added_count = 0
    mail = None

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select(f'"{target_folder}"', readonly=True)

        # Поиск новых UID
        search_query = f"UID {current_last_uid + 1}:*" if current_last_uid > 0 else "UID 1:*"
        status, data = mail.uid('search', None, search_query)

        if status != "OK" or not data or not data[0]:
            return 0

        uids = sorted(
            [int(u) for u in data[0].split() if int(u) > current_last_uid]
        )
        total = len(uids)

        if total == 0:
            return 0

        for idx, uid in enumerate(uids, 1):
            try:
                status, msg_data = mail.uid('fetch', str(uid), '(RFC822)')
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject = decode_mime_header(msg.get("Subject"))
                body_text = get_email_body(msg)

                move_type = (
                    "ЗАЕЗД" if "ЗАЕЗД" in subject.upper()
                    else "ВЫЕЗД" if "ВЫЕЗД" in subject.upper()
                    else "---"
                )

                # Парсинг полей
                m = extract_field("Модель", body_text)
                n = extract_field("номер", body_text).upper().replace(" ", "")
                v = extract_field("VIN", body_text)
                c = extract_field("Клиент", body_text)
                d = extract_field("Документ", body_text)
                r = extract_field("Вид ремонта", body_text)
                a = extract_field("Разрешил", body_text)
                p = extract_field("Причина", body_text)

                dt_raw = extract_field("Дата", body_text).split()
                date_val = dt_raw[0] if len(dt_raw) > 0 else "---"
                time_val = dt_raw[1] if len(dt_raw) > 1 else "--:--:--"

                cursor.execute('''
                    INSERT OR IGNORE INTO events (
                        msg_id, type, model, number, vin, client,
                        document, repair_type, event_date, event_time,
                        authorized_by, reason, raw_subject
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(uid), move_type, m, n, v, c,
                    d, r, date_val, time_val, a, p, subject
                ))

                # Если запись реально добавлена
                if cursor.rowcount > 0:
                    added_count += 1

                # UID считается обработанным ВСЕГДА, если fetch прошёл успешно
                max_uid_processed = uid

                print(f" Обработка: [{idx}/{total}] UID {uid}...", end="\r")

            except Exception as e:
                print(f"\n[!] Ошибка на UID {uid}: {e}")
                continue

        # Финализация
        if max_uid_processed > current_last_uid:
            set_last_uid(cursor, max_uid_processed)
            conn.commit()

        if added_count > 0:
            print(f"\n[OK] Сессия завершена. Добавлено новых записей: {added_count}")
        return added_count

    except Exception as e:
        print(f"\n[Критическая ошибка]: {e}")
        return 0
    finally:
        try:
            if mail is not None:
                mail.logout()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Ошибка! Формат: python ps.py <ИМЯ_ПАПКИ> <ПУТЬ_К_БАЗЕ>")
        sys.exit(1)

    TARGET_FOLDER = sys.argv[1]
    DB_NAME = sys.argv[2]

    print(f"--- Мониторинг активен: {TARGET_FOLDER} (База: {DB_NAME}) ---")
    while True:
        process_emails(TARGET_FOLDER, DB_NAME)
        for i in range(CHECK_INTERVAL, 0, -1):
            print(f" Ожидание: {i} сек...   ", end="\r")
            time.sleep(1)
