import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import os
from collections import defaultdict


def write_log_file(text):
    with open("log.txt", "a", encoding='utf-8') as file:
        file.write(text + '\n')


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())


host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]

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


class FetchOperationData:
    def __init__(self):
        self.mysql_conn = None
        self.last_changes_time = datetime(2000, 1, 1, 00, 00, 00)
        self.last_operation_id = 0
        self.last_operations_dict = {}
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

    def check_operations_changes(self):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute("RESET QUERY CACHE")

        query_get_last_operations = """
        SELECT
            G1.gd_id,
            CASE 
                WHEN G1.gd_sc_parent > 0 THEN G2.gd_code
                ELSE G1.gd_code
            END AS gd_code,
            CASE 
                WHEN G1.gd_sc_parent > 0 THEN G2.gd_name 
                ELSE G1.gd_name 
            END AS gd_name,
            O.opr_quantity,
            OA.oap_cost,
            OA.oap_price1,
            CASE 
                WHEN G1.gd_sc_parent > 0 THEN U2.unt_name
                ELSE U1.unt_name
            END AS unit_name,
            S.sct_name,
            O.opr_positive,
            O.opr_type,
            O.opr_document,
            O.opr_id,
            O.opr_last_update
        FROM operations O
        LEFT JOIN operations_additional_prop OA ON OA.oap_id = O.opr_id
        LEFT JOIN dir_goods G1 ON O.opr_good = G1.gd_id
        LEFT JOIN dir_goods G2 ON G1.gd_sc_parent = G2.gd_id
        LEFT JOIN dir_units U1 ON U1.unt_id = G1.gd_unit
        LEFT JOIN dir_units U2 ON U2.unt_id = G2.gd_unit
        LEFT JOIN dir_goods_additional_prop GA ON G1.gd_id = GA.gdap_good
        LEFT JOIN dir_sizechart S ON GA.gdap_size = S.sct_id
        WHERE O.opr_last_update > %s
            AND O.opr_type IN (1, 3, 4, 5, 7)
            AND O.opr_performed = 1
        ORDER BY O.opr_last_update ASC
        """

        my_cursor.execute(query_get_last_operations, (self.last_changes_time, ))
        last_operations = my_cursor.fetchall()
        if last_operations:
            last_operation_time = last_operations[-1][-1]
            self.last_operation_id = last_operations[-1][11]
            if self.last_changes_time == datetime(2000, 1, 1, 00, 00, 00):
                self.last_changes_time = last_operation_time
            elif self.last_changes_time < last_operation_time:
                last_operations_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
                all_department = defaultdict(lambda: defaultdict(list))
                for operation in last_operations:
                    operation_type = operation[9]
                    is_operation_positive = operation[8]
                    operation_document = operation[10]
                    department = operation[7] if operation[7] else 'all'
                    if operation_type == 1:
                        last_operations_dict[department]['purchase'][operation_document].append(operation)
                        all_department['purchase'][operation_document].append(operation)
                    elif operation_type == 3:
                        last_operations_dict[department]['write_off'][operation_document].append(operation)
                        all_department['write_off'][operation_document].append(operation)
                    elif operation_type == 4:
                        last_operations_dict[department]['entry'][operation_document].append(operation)
                        all_department['entry'][operation_document].append(operation)
                    elif operation_type == 5:
                        last_operations_dict[department]['return'][operation_document].append(operation)
                        all_department['return'][operation_document].append(operation)
                    elif operation_type == 7 and is_operation_positive == 1:
                        last_operations_dict[department]['moving_positive'][operation_document].append(operation)
                        all_department['moving_positive'][operation_document].append(operation)
                    elif operation_type == 7 and is_operation_positive == 0:
                        last_operations_dict[department]['moving_negative'][operation_document].append(operation)
                        all_department['moving_negative'][operation_document].append(operation)

                self.last_changes_time = last_operation_time
                last_operations_dict_all_department = {'all': all_department}
                return last_operations_dict, last_operations_dict_all_department

    def get_document_info(self, document_type, document_id):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute("RESET QUERY CACHE")
        if document_type == 'purchase':
            query_get_purchase_info = """
            SELECT
                pur_id, 
                pur_note,
                pur_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name,
                ven_company_name
            FROM doc_purchases
            LEFT JOIN dir_vendors ON pur_vendor = ven_id
            LEFT JOIN dir_users ON pur_autor0 = us_id
            LEFT JOIN dir_objects ON pur_object = obj_id    
            WHERE pur_id = %s
            """
            my_cursor.execute(query_get_purchase_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'write_off' or document_type == 'entry':
            query_get_inout_info = """
            SELECT
                int_id, 
                int_note,
                int_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name
            FROM doc_inout
            LEFT JOIN dir_users ON int_autor0 = us_id
            LEFT JOIN dir_objects ON int_object = obj_id  
            WHERE int_id = %s
            """
            my_cursor.execute(query_get_inout_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'return':
            query_get_return_info = """
            SELECT
                rtn_id, 
                rtn_note,
                rtn_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name,
                ven_company_name
            FROM doc_returns
            LEFT JOIN dir_vendors ON rtn_vendor = ven_id
            LEFT JOIN dir_users ON int_autor0 = us_id
            LEFT JOIN dir_objects ON rtn_object = obj_id 
            WHERE rtn_id = %s
            """
            my_cursor.execute(query_get_return_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'moving_positive' or document_type == 'moving_negative':
            query_get_moving_info = """
            SELECT 
                M.mvt_id,
                M.mvt_note,
                M.mvt_date0,
                U.us_name1,
                U.us_name2,
                U.us_phone,
                OSE.obj_name AS obj_sender,
                ORE.obj_name AS obj_receiver
            FROM doc_movements
            LEFT JOIN dir_users ON mvt_autor0 = us_id
            LEFT JOIN dir_objects OSE ON mvt_object1 = obj_id  
            LEFT JOIN dir_objects ORE ON mvt_object2 = obj_id
            WHERE mvt_id = %s
            """
            my_cursor.execute(query_get_moving_info, (document_id,))
            return my_cursor.fetchone()

    def write_tuple_to_file(self, filename, dict_data):
        with open(filename, "w", encoding='utf-8') as file:
            file.write(str(dict_data))

    def document_type_name(self, document_type):
        if document_type == 'purchase':
            return 'ЗАКУПКИ'
        elif document_type == 'write_off':
            return 'СПИСАНИЕ'
        elif document_type == 'entry':
            return 'ЗАНЕСЕНИЕ'
        elif document_type == 'moving_positive' or document_type == 'moving_negative':
            return 'ПЕРЕМЕЩЕНИЕ'
        elif document_type == 'return':
            return 'ВОЗВРАТ ОТ ПОСТАВЩИКА'

    def format_lastname_firstname_phone(self, last_name, first_name, phone):
        formatted_text = last_name
        formatted_text += f' {first_name}' if first_name else ''
        formatted_text += f', Тел: {phone}' if phone else ''
        return formatted_text

    def test_tuple(self):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute('RESET QUERY CACHE')
        my_cursor.execute('SELECT sct_name FROM dir_sizechart WHERE sct_deleted = 0')
        size_list = my_cursor.fetchall()
        for size in size_list:
            print(size)

    def test_class_function(self):
        if self.is_mysql_connected():
            my_cursor = self.mysql_conn.cursor()
            my_cursor.execute('RESET QUERY CACHE')
            my_cursor.execute('SELECT sct_name FROM dir_sizechart WHERE sct_deleted = 0')
            departments_list = [department_info[0] for department_info in my_cursor.fetchall()]
            last_operations_tuple = self.check_operations_changes()
            # each_departments_notifications = self.format_notification(
            #     last_warehouse_operations=last_operations_dict,
            #     department_config='each'
            # )

            if last_operations_tuple:
                last_operations_each_department = last_operations_tuple[0]
                last_operations_all_department = last_operations_tuple[1]
                all_departments_notifications = self.format_notification(
                    last_warehouse_operations=last_operations_all_department
                )
                for document_id, notification_content in all_departments_notifications['all'].items():
                    print(notification_content)
            # if each_departments_notifications:
            #     for notification_department, notification_contents in each_departments_notifications.items():
            #         for document_id, notification_content in notification_contents.items():
            #             print(notification_content)

    def format_notification(self, last_warehouse_operations):
        document_notification_dict = defaultdict(lambda: defaultdict(str))
        if not last_warehouse_operations:
            return False

        for department, department_data in last_warehouse_operations.items():
            for document_type, document_type_data in department_data.items():
                for document_id, document_data in document_type_data.items():
                    document_info = self.get_document_info(document_type, document_id)
                    formatted_user_name = self.format_lastname_firstname_phone(
                        last_name=document_info[3],
                        first_name=document_info[4],
                        phone=document_info[5]
                    )

                    if document_type == 'purchase' or document_type == 'return':
                        formatted_vendor_name = f"\nПоставщик: {document_info[7]}"
                    else:
                        formatted_vendor_name = ""

                    if document_type == 'moving_positive' or document_type == 'moving_negative':
                        warehouse_name = f"Отправитель: {document_info[6]}, Получатель: {document_info[7]}"
                    else:
                        warehouse_name = document_info[6]

                    document_description = f"""
ДОКУМЕНТ {self.document_type_name(document_type)}
Отдел: {department if department != 'all' else 'все отделы'}
Исполнитель: {formatted_user_name}
Склад: {warehouse_name}{formatted_vendor_name}
Дата и время: {format_date(document_info[2])}
Дата и время (последнее изменение): {format_date(document_data[-1][-1])}
"""
                    operation_order = 1
                    total_cost_document = 0
                    total_sum_document = 0
                    document_operations_text = ''
                    for operation in document_data:
                        operation_department = 'all' if department == 'all' else operation[7]
                        if department == operation_department:
                            department_name = f' ({operation[7]})' if operation[7] else ''
                            operation_total_cost = operation[3] * operation[4]
                            operation_total_price = operation[3] * operation[5]
                            document_operations_text += f"""{30 * '-'}
{operation_order}. K:{operation[1]}, {operation[2]}{department_name}
{format_number(operation[3])} ({operation[6]}) x {format_number(operation[4])} = {format_number(operation_total_cost)}, x {format_number(operation[5])} = {format_number(operation_total_price)}
"""
                            operation_order += 1
                            total_cost_document += operation_total_cost
                            total_sum_document += operation_total_price

                    if operation_order > 1:
                        document_notification_dict[department][document_id] += document_description + f"""{30 * '-'}
Сумма стоимости: {format_number(total_cost_document)}
Сумма: {format_number(total_sum_document)}
Количество операции: {operation_order}
"""
                        document_notification_dict[department][document_id] += document_operations_text

        return document_notification_dict

test = FetchOperationData()
while True:
    test.test_class_function()
    time.sleep(1)

