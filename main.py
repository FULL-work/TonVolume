import asyncio
import sqlite3
import requests
from datetime import datetime, timedelta
import re
import os
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# Настройка логирования
log_directory = 'loggs'  # Укажите директорию, в которой будет храниться файл логов
log_filename = os.path.join(log_directory, 'app.log')

# Проверяем, существует ли директория, если нет — создаем
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,  # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат сообщений
    filename=log_filename,  # Указываем путь к файлу логов
    filemode='a'  # Режим записи (a - добавление в файл, w - перезапись файла)
)
logger = logging.getLogger(__name__)


# Подключение к Google Sheets
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = r'C:\Users\PycharmProjects\volumes\1.json' # Укажите путь к вашему JSON-файлу с данными по google sheets api
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1Fwgid=0' # укажите ссылку на вашу таблицу


credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPE)
gs_client = gspread.authorize(credentials)
spreadsheet = gs_client.open_by_url(SPREADSHEET_URL)
worksheet_wallets = spreadsheet.worksheet('wallets') #wallets-название листа

# Функции для работы с Google Sheets
def write_wallet_data_to_google_sheets(jetton_address_raw):
    try:
        conn = sqlite3.connect('database1.db')
        cursor = conn.cursor()

        # Запрос кошельков, отсортированных по объему
        cursor.execute('SELECT base_address, volume, sell_vol, buys_vol, saldo_vol FROM wallet ORDER BY volume DESC')
        wallets = cursor.fetchall()

        # Заголовки таблицы с добавлением столбца для номера
        rows = [['Place', 'address', 'volume', 'sells', 'buys', 'trade_balance']]

        # Формирование данных
        for idx, wallet in enumerate(wallets, start=1):
            if wallet[0] == jetton_address_raw:
                continue  # Пропускаем адрес токена
            # Формируем номер строки (просто номер)
            number = str(idx)
            # Добавляем строку
            rows.append([number, *wallet])

        # Обновление Google Sheets
        worksheet_wallets.clear()
        worksheet_wallets.update('A1', rows)

        logger.info("Данные wallet успешно обновлены в Google Sheets.")
    except Exception as e:
        logger.error(f"Ошибка обновления данных в Google Sheets: {e}")
    finally:
        conn.close()

# Инициализация базы данных
def initialize_database():
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    # Таблица транзакций
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS all_transactions (
      event_id TEXT PRIMARY KEY,
      address TEXT NOT NULL REFERENCES wallet(raw_address),
      type TEXT CHECK(type IN ('sell', 'buy')) NOT NULL,
      amount_token REAL NOT NULL,
      amount_ton REAL NOT NULL,
      timeoftransaction TEXT
    )
    ''')

    # Таблица кошельков
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wallet (
        base_address TEXT,
        raw_address TEXT PRIMARY KEY,
        volume REAL DEFAULT 0,
        sell_vol REAL DEFAULT 0,
        buys_vol REAL DEFAULT 0,
        saldo_vol REAL DEFAULT 0
    )
    ''')

    conn.commit()
    conn.close()

initialize_database()

# API-запросы
API_KEY = "Bearer AH6O4F2CCHO5UEIAAAANSXBQ6VH5SVYGAB3R6TJMGIV3D2VMDVW4VNIRNVI4Y" # Надо вписать свой api ключ для tonapi

def make_request(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка при запросе {url}: {e}")
        return None

def get_raw_address(address):
    url = f"https://toncenter.com/api/v2/detectAddress?address={address}"
    data = make_request(url)
    if data and data.get("ok") and data.get("result"):
        return data["result"].get("raw_form")
    return None

def get_event_details(event_id):
    time.sleep(1)
    url = f"https://tonapi.io/v2/events/{event_id}"
    headers = {"Authorization": API_KEY}
    return make_request(url, headers=headers)

# Функции работы с транзакциями
def parse_transaction(details, last_transaction_time, start_date):
    timeoftransaction = details.get('timestamp')
    transaction_time = datetime.utcfromtimestamp(timeoftransaction) + timedelta(hours=3)
    if (last_transaction_time and transaction_time <= last_transaction_time) or transaction_time < start_date:
        return None

    formatted_time = transaction_time.strftime("%Y-%m-%d %H:%M:%S")
    description = details.get('actions', [])[0].get('simple_preview', {}).get('description', None)

    transaction_type = None
    amount_ton = None
    amount_token = None

    if description and "for" in description:
        parts = description.split("for")
        if "TON" in parts[0]:
            transaction_type = "buy"
            amount_ton = float(re.findall(r"[\d.]+", parts[0])[0])
            amount_token = float(re.findall(r"[\d.]+", parts[1])[0])
        else:
            transaction_type = "sell"
            amount_token = float(re.findall(r"[\d.]+", parts[0])[0])
            amount_ton = float(re.findall(r"[\d.]+", parts[1])[0])

    return {
        "transaction_type": transaction_type,
        "amount_token": amount_token,
        "amount_ton": amount_ton,
        "formatted_time": formatted_time
    }

def fetch_and_insert_transactions(address, start_date, jetton_address_raw):
    logger.info(f"Обработка транзакций для адреса: {address}")
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    raw_address = get_raw_address(address)
    if not raw_address:
        logger.warning(f"Не удалось получить raw-адрес для {address}")
        return

    logger.info(f"Raw-адрес для {address}: {raw_address}")
    cursor.execute('''SELECT MAX(timeoftransaction) FROM all_transactions WHERE address = ?''', (raw_address,))
    last_transaction_time = cursor.fetchone()[0]
    if last_transaction_time:
        last_transaction_time = datetime.strptime(last_transaction_time, "%Y-%m-%d %H:%M:%S")
        logger.info(f"Последняя транзакция для {raw_address}: {last_transaction_time}")
    else:
        logger.info(f"Транзакции для {raw_address} ранее не обрабатывались.")

    base_url = f"https://tonapi.io/v2/accounts/{raw_address}/jettons/{jetton_address_raw}/history?limit=100"
    headers = {
        "Authorization": API_KEY,
        "Accept": "application/json"
    }

    logger.info(f"Запрос транзакций с {base_url}")
    transactions = make_request(base_url, headers=headers)
    if not transactions:
        logger.warning(f"Не удалось получить данные транзакций для {raw_address}")
        return

    # Извлекаем все event_id из базы данных для текущего адреса
    cursor.execute('SELECT event_id FROM all_transactions WHERE address = ?', (raw_address,))
    existing_event_ids = {row[0] for row in cursor.fetchall()}

    # Фильтруем только новые события
    new_events = [event for event in transactions.get('events', []) if event.get('event_id') not in existing_event_ids]

    if not new_events:
        logger.info(f"Для {raw_address} нет новых событий для обработки.")
        conn.close()
        return

    for event in new_events:
        logger.info(f"Обработка события: {event.get('event_id')}")
        details = get_event_details(event.get('event_id'))
        if not details:
            logger.warning(f"Не удалось получить детали для события {event.get('event_id')}")
            continue

        parsed = parse_transaction(details, last_transaction_time, start_date)
        if not parsed:
            logger.info(f"Событие {event.get('event_id')} пропущено по временным ограничениям.")
            continue

        logger.info(f"Добавление транзакции в базу данных: {parsed}")
        cursor.execute('''
            INSERT OR IGNORE INTO all_transactions (event_id, address, type, amount_token, amount_ton, timeoftransaction)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (event.get('event_id'), raw_address, parsed["transaction_type"], parsed["amount_token"], parsed["amount_ton"], parsed["formatted_time"]))

    logger.info(f"Транзакции для {raw_address} успешно обработаны.")
    conn.commit()
    conn.close()

def delete_wallet(address):
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    try:
        # Получаем raw-адрес кошелька
        raw_address = get_raw_address(address)
        if not raw_address:
            logger.warning(f"Не удалось получить raw-адрес для {address}. Удаление прервано.")
            return

        # Удаляем связанные транзакции
        cursor.execute('DELETE FROM all_transactions WHERE address = ?', (raw_address,))
        deleted_transactions = cursor.rowcount
        logger.info(f"Удалено {deleted_transactions} транзакций, связанных с кошельком {raw_address}.")

        # Удаляем кошелек
        cursor.execute('DELETE FROM wallet WHERE raw_address = ?', (raw_address,))
        deleted_wallets = cursor.rowcount

        if deleted_wallets > 0:
            logger.info(f"Кошелек {raw_address} успешно удален.")
        else:
            logger.warning(f"Кошелек {raw_address} не найден в базе данных.")

        # Сохраняем изменения
        conn.commit()

    except Exception as e:
        logger.error(f"Ошибка при удалении кошелька {address}: {e}")
        conn.rollback()

    finally:
        conn.close()

# Расчет статистики
def calculate_wallet_statistics():
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    cursor.execute('SELECT raw_address FROM wallet')
    wallets = cursor.fetchall()

    for (wallet,) in wallets:
        cursor.execute('''
            SELECT type, SUM(amount_token) FROM all_transactions WHERE address = ? GROUP BY type
        ''', (wallet,))

        results = cursor.fetchall()
        buys = sum(amount for ttype, amount in results if ttype == 'buy')
        sells = sum(amount for ttype, amount in results if ttype == 'sell')

        saldo = buys - sells
        volume = buys + sells
        cursor.execute('''
            UPDATE wallet SET buys_vol = ?, sell_vol = ?, saldo_vol = ?, volume = ? WHERE raw_address = ?
        ''', (buys, sells, saldo, volume, wallet))

    conn.commit()
    conn.close()

def periodic_update(start_date, jetton_address_raw):
    try:
        logger.info("Запуск обновления...")
        conn = sqlite3.connect('database1.db')
        cursor = conn.cursor()

        cursor.execute('SELECT raw_address FROM wallet')
        wallets = cursor.fetchall()
        logger.info(f"Найдено {len(wallets)} кошельков для обновления.")

        conn.close()

        for (wallet,) in wallets:
            logger.info(f"Обновление транзакций для кошелька: {wallet}")
            fetch_and_insert_transactions(wallet, start_date, jetton_address_raw)

        logger.info("Пересчет статистики кошельков...")
        calculate_wallet_statistics()
        logger.info("Обновление завершено.")

    except Exception as e:
        logger.error(f"Ошибка в обновлении: {e}")

# Терминал для управления
def terminal_interface(jetton_address_raw):
    print("Доступные команды: add_wallet, fetch_wallets, wallet_transactions, fetch_all_transactions, update_google_sheets, update_tables, exit")

    while True:
        try:
            command = input("Введите команду: ").strip()

            if command == "add_wallet":
                address = input("Введите адрес кошелька: ")
                add_wallet(address)
                fetch_and_insert_transactions(address, start_date, jetton_address_raw)
                calculate_wallet_statistics()

            elif command == "fetch_wallets":
                fetch_all_wallets()

            elif command == "wallet_transactions":
                address = input("Введите адрес кошелька: ")
                fetch_transactions_for_wallet(address)

            elif command == "fetch_all_transactions":
                fetch_all_transactions()

            elif command == "update_google_sheets":
                write_wallet_data_to_google_sheets(jetton_address_raw)

            elif command == "update_tables":
                periodic_update(start_date, jetton_address_raw)

            elif command == "delete_wallet":
                address = input("Введите адрес кошелька для удаления: ")
                delete_wallet(address)

            elif command == "exit":
                break

            else:
                print("Неизвестная команда")
        except Exception as e:
            logger.error(f"Ошибка в терминале: {e}")


# Функции для работы с кошельками
def add_wallet(address):
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    raw_address = get_raw_address(address)
    if not raw_address:
        logger.warning(f"Не удалось добавить кошелек: {address}")
        return

    cursor.execute('''
        INSERT OR IGNORE INTO wallet (base_address,raw_address)
        VALUES (?, ?)
    ''', (address,raw_address,))

    conn.commit()
    conn.close()

    logger.info(f"Кошелек {address} добавлен")

def fetch_all_wallets():
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM wallet')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    conn.commit()
    conn.close()

async def periodic_task(interval, update_tables, update_google_sheets):

    while True:
        try:
            # Вызов функций обновления
            await asyncio.get_event_loop().run_in_executor(None, update_tables)
            await asyncio.get_event_loop().run_in_executor(None, update_google_sheets)
            # Ожидание перед следующим вызовом
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Ошибка в периодическом обновлении: {e}")


def fetch_transactions_for_wallet(wallet_address):
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    raw_address = get_raw_address(wallet_address)
    if not raw_address:
        print("Не удалось получить raw адрес для кошелька.")
        conn.close()
        return

    cursor.execute('''
        SELECT * FROM all_transactions WHERE address = ?
    ''', (raw_address,))

    for row in cursor.fetchall():
        print(f"Event ID: {row[0]}")
        print(f"Address: {row[1]}")
        print(f"Type: {row[2]}")
        print(f"Amount Token: {row[3]}")
        print(f"Amount TON: {row[4]}")
        print(f"Time of Transaction: {row[5]}")
        print("-" * 40)

    conn.close()

def fetch_all_transactions():
    conn = sqlite3.connect('database1.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM all_transactions')
    rows = cursor.fetchall()
    for row in rows:
        print(f"Event ID: {row[0]}")
        print(f"Address: {row[1]}")
        print(f"Type: {row[2]}")
        print(f"Amount Token: {row[3]}")
        print(f"Amount TON: {row[4]}")
        print(f"Time of Transaction: {row[5]}")
        print("-" * 40)

    conn.close()

def update_tables():
    try:
        periodic_update(start_date, jetton_address_raw)
    except Exception as e:
        logger.error(f"Ошибка при обновлении таблиц: {e}")


if __name__ == "__main__":
    start_date_str = input("Введите начальную дату (YYYY-MM-DD): ")
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    jetton_address = input("Введите адрес токена: ")
    jetton_address_raw = get_raw_address(jetton_address)
    if not jetton_address_raw:
        print("Не удалось преобразовать адрес токена в raw. Завершение работы.")
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Запуск терминала в отдельном executor
        loop.run_in_executor(None, terminal_interface, jetton_address_raw)

        # Запуск периодической задачи с вызовом функций
        asyncio.ensure_future(periodic_task(900, update_tables, lambda: write_wallet_data_to_google_sheets(jetton_address_raw)))

        # Запуск цикла событий
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            print("Программа завершена.")
