import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import os
from collections import defaultdict
from copy import deepcopy
import inspect


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())


host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]
limit_operation = config['limit_operation']


def get_date():
    now = datetime.now()
    return now.strftime("%m/%d/%Y %H:%M:%S")


def get_line_number():
    return inspect.currentframe().f_back.f_lineno


def write_log_file(text):
    with open("log.txt", "a", encoding='utf-8') as file:
        file.write(f"{text} (Line: {get_line_number()}, {get_date()})\n")


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

    return f"{formatted_integer}.{decimal_part[0][:2]}"


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
        self.last_sent_docs_status_dict = {
            'purchases': {},
            'returns': {},
            'inouts': {},
            'movements': {},
        }
        self.last_operations_dict_all_department = {}
        self.connect_mysql()
        if self.is_mysql_connected():
            self.get_documents_status()
            # self.check_and_create_indexes()

        with open("log.txt", 'w', encoding='utf-8') as file:
            file.write(f"File created at {get_date()}\n")

    def check_and_create_indexes(self):
        # Check existing indexes
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute("""
           SELECT INDEX_NAME 
           FROM information_schema.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE()
           AND TABLE_NAME IN ('operations', 'operations_additional_prop', 'dir_goods')
       """)
        existing_indexes = [row[0] for row in my_cursor.fetchall()]

        # Define indexes
        indexes = {
            'idx_opr_last_update': "CREATE INDEX idx_opr_last_update ON operations(opr_last_update, opr_type)",
            'idx_opr_good': "CREATE INDEX idx_opr_good ON operations(opr_good)",
            'idx_oap_id': "CREATE INDEX idx_oap_id ON operations_additional_prop(oap_id)",
            'idx_gd_sc_parent': "CREATE INDEX idx_gd_sc_parent ON dir_goods(gd_sc_parent)"
        }

        # Create missing indexes
        for index_name, create_query in indexes.items():
            if index_name not in existing_indexes:
                try:
                    my_cursor.execute(create_query)
                    print(f"Created index: {index_name}")
                except mysql.connector.Error as err:
                    print(f"Error creating {index_name}: {err}")

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
        except Error as e:
            print(f"Line: {get_line_number()}, Can't connect to the MySQL. {e} {get_date()}")
            write_log_file(f"Can't connect to the MySQL. {e}")
            return False
        else:
            return True

    def is_mysql_connected(self):
        if self.mysql_conn:
            if self.mysql_conn.is_connected():
                return True
            else:
                print(f"Line: {get_line_number()}, Failed to connect to MySQL")
                write_log_file(f"Failed to connect to MySQL")
                self.connect_mysql()
                return False
        else:
            print(f"Line: {get_line_number()}, Failed to connect to MySQL")
            write_log_file(f"Failed to connect to MySQL")
            self.connect_mysql()
            return False

    def check_operations_changes(self):
        try:
            my_cursor = self.mysql_conn.cursor()
            my_cursor.execute("RESET QUERY CACHE")

            query_get_last_operations = """
            SELECT
                G1.gd_id,
                IF(G1.gd_sc_parent > 0, G2.gd_code, G1.gd_code) AS gd_code,
                IF(G1.gd_sc_parent > 0, G2.gd_name, G1.gd_name) AS gd_name,
                O.opr_quantity,
                OA.oap_cost,
                OA.oap_price1,
                IF(G1.gd_sc_parent > 0, U2.unt_name, U1.unt_name) AS unit_name,
                S.sct_name,
                O.opr_positive,
                O.opr_type,
                O.opr_document,
                O.opr_id,
                O.opr_last_update
            FROM operations O
                LEFT JOIN operations_additional_prop OA ON O.opr_id = OA.oap_operation 
                LEFT JOIN dir_goods G1 ON O.opr_good = G1.gd_id
                LEFT JOIN dir_goods G2 ON G1.gd_sc_parent = G2.gd_id
                LEFT JOIN dir_units U1 ON U1.unt_id = G1.gd_unit
                LEFT JOIN dir_units U2 ON U2.unt_id = G2.gd_unit
                LEFT JOIN dir_goods_additional_prop GA ON G1.gd_id = GA.gdap_good
                LEFT JOIN dir_sizechart S ON GA.gdap_size = S.sct_id
            WHERE O.opr_last_update > %s
                AND O.opr_type IN (1, 3, 4, 5, 7)
            ORDER BY O.opr_last_update DESC
            LIMIT %s
            """

            my_cursor.execute(query_get_last_operations, (self.last_changes_time, limit_operation))
            last_operations = my_cursor.fetchall()
            last_operations.reverse()
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
                        department = operation[7] if operation[7] else 'without_department'
                        if operation_type == 1:
                            last_operations_dict[department]['purchases'][operation_document].append(operation)
                            all_department['purchases'][operation_document].append(operation)
                        elif operation_type == 5:
                            last_operations_dict[department]['returns'][operation_document].append(operation)
                            all_department['returns'][operation_document].append(operation)
                        elif operation_type in (3, 4):
                            last_operations_dict[department]['inouts'][operation_document].append(operation)
                            all_department['inouts'][operation_document].append(operation)
                        elif operation_type == 7:
                            last_operations_dict[department]['movements'][operation_document].append(operation)
                            all_department['movements'][operation_document].append(operation)

                    last_operations_dict_all_department = {'all': all_department}
                    return last_operations_dict, last_operations_dict_all_department, last_operation_time
        except Error as e:
            print(f"Line: {get_line_number()}, Can't connect to the MySQL. {e} {get_date()}")
            write_log_file(f"Can't connect to the MySQL. {e}")

    def get_document_info(self, document_type, document_id):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute("RESET QUERY CACHE")
        if document_type == 'purchases':
            query_get_purchase_info = """
            SELECT
                pur_id, 
                pur_note,
                pur_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name,
                pur_performed,
                ven_company_name
            FROM doc_purchases
            LEFT JOIN dir_vendors ON pur_vendor = ven_id
            LEFT JOIN dir_users ON pur_autor0 = us_id
            LEFT JOIN dir_objects ON pur_object = obj_id    
            WHERE pur_id = %s
            """
            my_cursor.execute(query_get_purchase_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'inouts':
            query_get_inout_info = """
            SELECT
                int_id, 
                int_note,
                int_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name,
                int_performed,
                int_type
            FROM doc_inout
            LEFT JOIN dir_users ON int_autor0 = us_id
            LEFT JOIN dir_objects ON int_object = obj_id  
            WHERE int_id = %s
            """
            my_cursor.execute(query_get_inout_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'returns':
            query_get_return_info = """
            SELECT
                rtn_id, 
                rtn_note,
                rtn_date0,
                us_name1,
                us_name2,
                us_phone,
                obj_name,
                rtn_performed,
                ven_company_name
            FROM doc_returns
            LEFT JOIN dir_vendors ON rtn_vendor = ven_id
            LEFT JOIN dir_users ON rtn_autor0 = us_id
            LEFT JOIN dir_objects ON rtn_object = obj_id 
            WHERE rtn_id = %s
            """
            my_cursor.execute(query_get_return_info, (document_id,))
            return my_cursor.fetchone()
        elif document_type == 'movements':
            query_get_moving_info = """
            SELECT 
                M.mvt_id,
                M.mvt_note,
                M.mvt_date0,
                U.us_name1,
                U.us_name2,
                U.us_phone,
                OSE.obj_name AS obj_sender,
                ORE.obj_name AS obj_receiver,
                M.mvt_performed
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

    def document_type_name(self, document_type, document_id, is_doc_performed, inout_type):
        formatted_doc_name = 'ДОКУМЕНТ ' if is_doc_performed == 1 else 'ОТМЕНЕННЫЙ ДОКУМЕНТ '
        if document_type == 'purchases':
            formatted_doc_name += f'ЗАКУПКИ №{document_id}'
        elif document_type == 'inouts':
            if inout_type == 3:
                formatted_doc_name += f'СПИСАНИЕ №{document_id}'
            elif inout_type == 4:
                formatted_doc_name += f'ЗАНЕСЕНИЕ №{document_id}'
        elif document_type == 'movements':
            formatted_doc_name += f'ПЕРЕМЕЩЕНИЕ №{document_id}'
        elif document_type == 'returns':
            formatted_doc_name += f'ВОЗВРАТ ОТ ПОСТАВЩИКА №{document_id}'
        return formatted_doc_name

    def format_lastname_firstname_phone(self, last_name, first_name, phone):
        formatted_text = last_name
        formatted_text += f' {first_name}' if first_name else ''
        formatted_text += f', Тел: {phone}' if phone else ''
        return formatted_text

    def get_documents_status(self):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute('RESET QUERY CACHE')
        my_cursor.execute('SELECT pur_id, pur_performed FROM doc_purchases')
        purchases_status = my_cursor.fetchall()
        if purchases_status:
            for purchase_info in purchases_status:
                self.last_sent_docs_status_dict['purchases'][purchase_info[0]] = purchase_info[1]

        my_cursor.execute('SELECT rtn_id, rtn_performed FROM doc_returns')
        returns_status = my_cursor.fetchall()
        if returns_status:
            for return_info in returns_status:
                self.last_sent_docs_status_dict['returns'][return_info[0]] = return_info[1]

        my_cursor.execute('SELECT int_id, int_performed FROM doc_inout')
        inouts_status = my_cursor.fetchall()
        if inouts_status:
            for inout_info in inouts_status:
                self.last_sent_docs_status_dict['inouts'][inout_info[0]] = inout_info[1]

        my_cursor.execute('SELECT mvt_id, mvt_performed FROM doc_movements')
        movements_status = my_cursor.fetchall()
        if movements_status:
            for movement_info in movements_status:
                self.last_sent_docs_status_dict['movements'][movement_info[0]] = movement_info[1]


    def test_tuple(self):
        my_cursor = self.mysql_conn.cursor()
        my_cursor.execute('RESET QUERY CACHE')
        my_cursor.execute('SELECT sct_name FROM dir_sizechart WHERE sct_deleted = 0')
        size_list = my_cursor.fetchall()
        for size in size_list:
            print(size)

    def test_class_function(self):
        if self.is_mysql_connected():
            last_operations_tuple = self.check_operations_changes()
            last_documents_statuses = deepcopy(self.last_sent_docs_status_dict)

            if last_operations_tuple:
                last_operations_each_department = last_operations_tuple[0]
                last_operations_all_department = last_operations_tuple[1]
                last_operation_time = last_operations_tuple[2]
                all_departments_notifications = self.format_notification(
                    last_warehouse_operations=last_operations_all_department,
                    last_documents_statuses=last_documents_statuses
                )
                if all_departments_notifications:
                    for document_id, notification_content in all_departments_notifications['all'].items():
                        print(notification_content)

                each_departments_notifications = self.format_notification(
                    last_warehouse_operations=last_operations_each_department,
                    last_documents_statuses=last_documents_statuses
                )
                if each_departments_notifications:
                    for department, department_data in each_departments_notifications.items():
                        for document_id, notification_string in department_data.items():
                            print(notification_string)

                self.last_changes_time = last_operation_time

    def format_notification(self, last_warehouse_operations, last_documents_statuses):
        document_notification_dict = defaultdict(lambda: defaultdict(str))
        if not last_warehouse_operations:
            return False

        for department, department_data in last_warehouse_operations.items():
            for document_type, document_type_data in department_data.items():
                for document_id, document_data in document_type_data.items():
                    document_info = self.get_document_info(document_type, document_id)
                    is_document_performed = document_info[8] if 'movements' == document_type else document_info[7]
                    if is_document_performed == 0 and document_id not in last_documents_statuses[document_type]:
                        return False
                    elif document_id in last_documents_statuses[document_type]:
                        if is_document_performed == last_documents_statuses[document_type][document_id]:
                            return False

                    formatted_user_name = self.format_lastname_firstname_phone(
                        last_name=document_info[3],
                        first_name=document_info[4],
                        phone=document_info[5]
                    )

                    if document_type == 'purchases' or document_type == 'returns':
                        formatted_vendor_name = f"\nПоставщик: {document_info[8]}"
                    else:
                        formatted_vendor_name = ""

                    if document_type == 'movements':
                        warehouse_name = f"Отправитель: {document_info[6]}, Получатель: {document_info[7]}"
                    else:
                        warehouse_name = document_info[6]

                    inout_type = document_info[8] if document_type == 'inouts' else 0

                    if department == 'all':
                        department_name = ''
                    elif department == 'without_department':
                        department_name = '\nОтдел: без отдела'
                    else:
                        department_name = f'\nОтдел: {department}'

                    if not document_info[1]:
                        doc_description = ''
                    else:
                        doc_description = f'\nПримечание: {document_info[1]}'

                    document_description = f"""
<b>{self.document_type_name(document_type, document_id, is_document_performed, inout_type)}{department_name}</b>
Исполнитель: {formatted_user_name}
Склад: {warehouse_name}{formatted_vendor_name}
Дата и время: {format_date(document_info[2])}
Дата и время (последнее изменение): {format_date(document_data[-1][-1])}{doc_description}
"""
                    operation_order = 1
                    total_cost_document = 0
                    total_sum_document = 0
                    document_operations_text = ''
                    for operation in document_data:
                        operation_department = operation[7] if department == operation[7] else department
                        if department == operation_department:
                            department_name = f' ({operation[7]})' if operation[7] else ''
                            operation_total_cost = operation[3] * operation[4]
                            operation_total_price = operation[3] * operation[5]
                            document_operations_text += f"""{50 * '-'}
{operation_order}. K:{operation[1]}, {operation[2]}{department_name}
<b>{format_number(operation[3])} ({operation[6]})</b> x {format_number(operation[4])} = {format_number(operation_total_cost)}, x {format_number(operation[5])} = {format_number(operation_total_price)}
"""
                            operation_order += 1
                            total_cost_document += operation_total_cost
                            total_sum_document += operation_total_price

                    if operation_order > 1:
                        document_notification_dict[department][document_id] += document_description + f"""{50 * '-'}
<b>Сумма стоимости: {format_number(total_cost_document)}</b>
<b>Сумма: {format_number(total_sum_document)}</b>
<b>Количество операции: {operation_order-1}</b>
"""
                        document_notification_dict[department][document_id] += document_operations_text

                    self.last_sent_docs_status_dict[document_type][document_id] = is_document_performed

        return document_notification_dict

# test = FetchOperationData()
# while True:
#     test.test_class_function()
#     time.sleep(1)

