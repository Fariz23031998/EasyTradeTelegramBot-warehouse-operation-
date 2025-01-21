import time

import telebot
import json
from typing import Dict, List
import os
from db import FetchOperationData
from background_task import BackgroundTask

with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())
# Initialize bot with your token
TOKEN = config['token']
bot = telebot.TeleBot(TOKEN)

# File to store user data
USERS_FILE = "users.json"

fetch_operation_data = FetchOperationData()

def split_string(text: str, max_length: int = 4096) -> list[str]:
    """
    Split a string into chunks, each no longer than max_length characters.
    Splits are made at line boundaries where possible.

    Args:
        text (str): The input string to split
        max_length (int): Maximum length of each chunk (default 4096)

    Returns:
        list[str]: List of string chunks
    """
    # If string is shorter than max_length, return it as a single chunk
    if len(text) <= max_length:
        return [text]

    chunks = []
    current_position = 0

    while current_position < len(text):
        # If remaining text is shorter than max_length, add it as the last chunk
        if current_position + max_length >= len(text):
            chunks.append(text[current_position:])
            break

        # Find the last newline within the max_length limit
        chunk_end = current_position + max_length
        last_newline = text.rfind('\n', current_position, chunk_end)

        if last_newline != -1 and last_newline > current_position:
            # If we found a newline, split there and include the newline
            chunks.append(text[current_position:last_newline + 1])
            current_position = last_newline + 1  # Start next chunk after the newline
        else:
            # If no newline found, split at max_length
            chunks.append(text[current_position:chunk_end])
            current_position = chunk_end

    return chunks


def load_users() -> Dict:
    """Load users from JSON file."""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_users(users: Dict) -> None:
    """Save users to JSON file."""
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=4)


@bot.message_handler(commands=['start'])
def handle_start(message):
    """Handle /start command."""
    user_id = str(message.from_user.id)
    users = load_users()

    if user_id not in users:
        users[user_id] = {
            'status': 'inactive',
            'departments': ['department A']
        }
        save_users(users)
        bot.reply_to(message, "Welcome! You've been registered. Your status is currently inactive.")
    else:
        bot.reply_to(message, "Welcome back! You're already registered.")


def send_notification(user_id: str, message_text: str) -> bool:
    """
    Send notification to a specific user.
    Returns True if message was sent successfully, False otherwise.
    """
    try:
        bot.send_message(int(user_id), message_text)
        return True
    except Exception as e:
        print(f"Error sending message to user {user_id}: {e}")
        return False


@bot.message_handler(commands=['status'])
def handle_status(message):
    """Handle /status command to check current status."""
    user_id = str(message.from_user.id)
    users = load_users()

    if user_id in users:
        status = users[user_id]['status']
        departments = ', '.join(users[user_id]['departments'])
        bot.reply_to(message,
                     f"Your current status: {status}\n"
                     f"Your departments: {departments}")
    else:
        bot.reply_to(message, "You're not registered. Please use /start to register.")


def prepare_notifications():
    if fetch_operation_data.is_mysql_connected():
        last_operations_tuple = fetch_operation_data.check_operations_changes()

        if last_operations_tuple:
            last_operations_each_department = last_operations_tuple[0]
            last_operations_all_department = last_operations_tuple[1]
            last_operation_time = last_operations_tuple[2]
            all_departments_notifications = fetch_operation_data.format_notification(
                last_warehouse_operations=last_operations_all_department
            )
            if all_departments_notifications:
                for document_id, notification_string in all_departments_notifications['all'].items():
                    # print(notification_string)
                    result = send_department_notification(
                        last_operation_time=last_operation_time,
                        message=notification_string,
                        department='all',
                    )
                    print(result)

            each_departments_notifications = fetch_operation_data.format_notification(
                last_warehouse_operations=last_operations_each_department,
            )
            if each_departments_notifications:
                for department, department_data in each_departments_notifications.items():
                    for document_id, notification_string in department_data.items():
                        # print(document_type_data)
                        result = send_department_notification(
                            last_operation_time=last_operation_time,
                            message=notification_string,
                            department=department,
                        )
                        print(result)


def send_department_notification(last_operation_time, message: str, department: str = None) -> Dict:
    """
    Broadcast notification to all active users or users in specific department.
    Returns dictionary with success and failure counts.
    """
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

def main():
    task = BackgroundTask(background_task=prepare_notifications)
    task.start()

if __name__ == "__main__":
    main()
    print("Bot started...")
    bot.infinity_polling()
