import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import os
import win32print
import win32ui
from win32con import *
import win32con


def write_log_file(text):
    with open("log.txt", "a", encoding='utf-8') as file:
        file.write(text + '\n')


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())


host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]

print_return = config['print_return']
font_name = config['font_name']
font_size = config['font_size']
font_spacing = config['font_spacing']
word_max_length = config['word_max_length']
margin = config['margin']
timeout = config['timeout']


def format_number(number: float) -> str:
    str_num = str(number)
    integer_part, *decimal_part = str_num.split('.')

    length = len(integer_part)
    groups = []
    for i in range(length, 0, -3):
        start = max(0, i - 3)
        groups.append(integer_part[start:i])
    formatted_integer = ' '.join(reversed(groups))

    if not decimal_part or decimal_part[0] == '0':
        return formatted_integer
    return f"{formatted_integer}.{decimal_part[0]}"


def delete_receipt_files():
    # Iterate through files in the folder
    for filename in os.listdir('receipts'):
        if filename.endswith('.txt'):
            file_path = os.path.join('receipts', filename)
            os.remove(file_path)


def format_phone(number: str) -> str:
    # Handle both string and integer inputs
    num_str = str(number)

    # Extract different parts of the number
    country_code = num_str[:3]
    city_code = num_str[3:5]
    first_part = num_str[5:8]
    second_part = num_str[8:10]
    last_part = num_str[10:]

    # Format the number according to the pattern: +998 (97) 392-33-03
    formatted_number = f"+{country_code} ({city_code}) {first_part}-{second_part}-{last_part}"

    return formatted_number


def format_date(date):
    # Parse the input string
    date_string = str(date)
    dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    # Format to desired output
    return dt.strftime('%d.%m.%Y %H:%M')


class PrintForDepartment:
    def __init__(self):
        self.mysql_conn = None
        self.last_changes_time = datetime(2000, 1, 1, 00, 00, 00)
        self.last_sale_id = 0
        self.last_sale_info_list = []
        self.connect_mysql()

        with open("log.txt", 'w', encoding='utf-8') as file:
            file.write(f"File created at {self.get_date()}\n")

    def get_date(self):
        now = datetime.now()
        return now.strftime("%m/%d/%Y %H:%M:%S")

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e} {self.get_date()}")
            return False
        else:
            return True

    def is_mysql_connected(self):
        if self.mysql_conn:
            if self.mysql_conn.is_connected():
                return True
            else:
                write_log_file("Failed to connect to MySQL")
                self.connect_mysql()
                return False
        else:
            write_log_file("Failed to connect to MySQL")
            self.connect_mysql()
            return False

    def check_purchase_changes(self):
        try:
            my_cursor = self.mysql_conn.cursor()
            my_cursor.execute("RESET QUERY CACHE")

            query_check_last_purchase = """
            SELECT 
                pur_id,
                pur_vendor,
                pur_performed,
                
            """

        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e} {self.get_date()}")
            return False

    def write_tuple_to_file(self, filename, dict_data):
        with open(filename, "w", encoding='utf-8') as file:
            file.write(str(dict_data))





