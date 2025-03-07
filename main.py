import time

import telebot
import json
from typing import Dict, List
import os
from db import FetchOperationData, write_log_file
from background_task import BackgroundTask
from copy import deepcopy
import unicodedata
import asyncio

# pyinstaller --onefile --name=EasyTradeWarehouseOperationTelegramBot --icon=logo.ico main.py
with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())

TOKEN = config['token']
bot = telebot.TeleBot(TOKEN)

# File to store user data
USERS_FILE = "users.json"

fetch_operation_data = FetchOperationData()


def split_string(text: str, max_length: int = 4096) -> list[str]:
    if len(text) <= max_length:
        return [text]

    chunks = []
    current_position = 0

    while current_position < len(text):
        if current_position + max_length >= len(text):
            chunks.append(text[current_position:])
            break

        chunk_end = current_position + max_length
        last_newline = text.rfind('\n', current_position, chunk_end)

        if last_newline != -1 and last_newline > current_position:
            chunks.append(text[current_position:last_newline + 1])
            current_position = last_newline + 1  # Start next chunk after the newline
        else:
            chunks.append(text[current_position:chunk_end])
            current_position = chunk_end

    return chunks


def load_users() -> Dict:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_users(users: Dict) -> None:
    """Save users to JSON file."""
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=4)

def normalize_font(text):
    if not text:
        return "Не указан"
    return ''.join(
        unicodedata.normalize('NFKD', char)[0]
        for char in text
    )

@bot.message_handler(commands=['start'])
def handle_start(message):
    """Handle /start command."""
    user_id = str(message.from_user.id)
    users = load_users()
    telegram_last_name = normalize_font(message.from_user.last_name)
    telegram_first_name = normalize_font(message.from_user.first_name)
    telegram_username = message.from_user.username if message.from_user.username else "Не указано"

    if user_id not in users:
        users[user_id] = {
            'status': 'inactive',
            'departments': ['department'],
            'firstname': telegram_first_name,
            'lastname': telegram_last_name,
            'username': telegram_username
        }
        save_users(users)
        send_notification(user_id, "Ваш аккаунт успешно зарегистрирован!")
    else:
        send_notification(user_id, "Вы уже зарегистрированы!")


def send_notification(user_id: str, message_text: str) -> bool:
    try:
        bot.send_message(int(user_id), message_text, parse_mode='HTML')

    except Exception as e:
        print(f"Error sending message to user {user_id}: {e}")
        write_log_file(f"Error sending message to user {user_id}: {e}")
        return False
    else:
        print(f"message was successfully sent!")
        return True




@bot.message_handler(commands=['status'])
def handle_status(message):
    """Handle /status command to check current status."""
    user_id = str(message.from_user.id)
    users = load_users()

    if user_id in users:
        status = users[user_id]['status']
        departments = ', '.join(users[user_id]['departments'])
        bot.reply_to(message,
                     f"Ваш текущий статус: {status}\n"
                     f"Ваши отделы: {departments}")
    else:
        bot.reply_to(message, "Вы не зарегистрированы. Пожалуйста, используйте /start для регистрации.")


def prepare_notifications():
    if fetch_operation_data.is_mysql_connected():
        last_operations_tuple = fetch_operation_data.check_operations_changes()
        last_documents_statuses = deepcopy(fetch_operation_data.last_sent_docs_status_dict)
        if last_operations_tuple:
            last_operations_each_department = last_operations_tuple[0]
            last_operations_all_department = last_operations_tuple[1]
            last_operation_time = last_operations_tuple[2]
            all_departments_notifications = fetch_operation_data.format_notification(
                last_warehouse_operations=last_operations_all_department,
                last_documents_statuses=last_documents_statuses,
            )
            if all_departments_notifications:
                for document_id, notification_string in all_departments_notifications['all'].items():
                    result = send_department_notification(
                        last_operation_time=last_operation_time,
                        message=notification_string,
                        department='all',
                    )
                    print(result)
                    write_log_file(f'Short string: {result}')

            each_departments_notifications = fetch_operation_data.format_notification(
                last_warehouse_operations=last_operations_each_department,
                last_documents_statuses=last_documents_statuses
            )
            if each_departments_notifications:
                for department, department_data in each_departments_notifications.items():
                    for document_id, notification_string in department_data.items():
                        result = send_department_notification(
                            last_operation_time=last_operation_time,
                            message=notification_string,
                            department=department,
                        )
                        print(result)
                        write_log_file(f'Long string: {result}')


def send_department_notification(last_operation_time, message: str, department: str = None) -> Dict:
    users = load_users()
    results = {'success': 0, 'failed': 0}

    for user_id, user_data in users.items():
        if user_data['status'] == 'active':
            if department in user_data['departments']:
                if len(message) <= 4096:
                    if send_notification(user_id, message):
                        results['success'] += 1
                        fetch_operation_data.last_changes_time = last_operation_time
                    else:
                        results['failed'] += 1
                else:
                    split_message = split_string(message, 4096)
                    for mes in split_message:
                        if send_notification(user_id, mes):
                            results['success'] += 1
                            fetch_operation_data.last_changes_time = last_operation_time
                        else:
                            results['failed'] += 1

    return results


def check_if_already_running():
    pid = str(os.getpid())
    pid_file = "bot.pid"

    if os.path.isfile(pid_file):
        with open(pid_file, 'r') as f:
            old_pid = f.read().strip()
        try:
            # Check if process with old PID is still running
            os.kill(int(old_pid), 0)
            print(f"Bot is already running with PID {old_pid}. Exiting.")
            exit(1)
        except OSError:
            # Process not found, safe to continue
            pass
        except SystemError:
            pass

    # Write current PID to file
    with open(pid_file, 'w') as f:
        f.write(pid)


def main():
    task = BackgroundTask(background_task=prepare_notifications)
    task.start()


if __name__ == "__main__":
    check_if_already_running()
    main()
    print("Bot started...")
    write_log_file('Bot started...')
    bot.infinity_polling(timeout=60, long_polling_timeout=30)

