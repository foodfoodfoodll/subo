import csv
import json
import uuid
from datetime import datetime
import os

path = "D:/Work/Скрипты/1416 дозагрузка/тест/"
path_to_result = "D:/Work/Скрипты/1416 дозагрузка/"
filename = ''
all_data = []
data_headers = ['id', 'document_id', 'created_at', 'updated_at', 'creation_channel', 'client_iteraction', 'creation_source_type', 'creation_source_id', 'client_mdm_id', 'client_unc_id', 'client_tb_id', 'creation_system_id', 'provider_id', 'provider_service_id', 'client_product_id', 'client_product_number', 'client_product_type', 'payment_amount', 'commission_amount', 'limit_transaction_id', 'rsa_transaction_id', 'confirm_transaction_id', 'abs_transaction_id', 'status', 'sub_status', 'fields', 'description', 'call_back_notify', 'abs_transaction_system_id', 'processing_fields', 'history_operation', 'service_provider', 'payment_detail', 'guid']
headers = []
fields_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'value', 'history', 'processing', 'keyProcessing']
processing_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'value']
history_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'index', 'value', 'caption']
payment_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'id', 'main', 'systemId']
fields_index = data_headers.index('fields')
payment_detail_index = data_headers.index('payment_detail')
processing_index = data_headers.index('processing_fields')
history_operation_index = data_headers.index('history_operation')
now = datetime.now()
commit_ts = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
processed_dt = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
op_scn = ""
op_seq = ""
op_type = ""
dte = now.strftime("%Y%m%d")


def write_to_file(filename, data, mode):
    file = open(path_to_result + filename, mode, newline ='', encoding='utf8')
    with file:
        writer = csv.writer(file, delimiter='')
        writer.writerow(data)

def write_log(data):
    file = open(path_to_result + 'log.txt', 'a', encoding='utf8')
    file.write(data)
    file.close()

### Для fields нужны: 'commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', key, value, history, processing, keyProcessing
### Словари в питоне неупорядоченны, шаблонный лист из комментария выше
def do_fields(fields, parent_guid):
    data = json.loads(fields)
    values = data.get('data')
    ### Бежим по словарю вложенных полей, внутри точно есть key, value, остального может не быть (а может быть)
    for item in values:
        row = [commit_ts, op_scn, op_seq, op_type, processed_dt, dte, parent_guid, parent_guid, parent_guid, item.get('key'), item.get('value')]
        if item.get('history') or item.get('history') == False:
            row.append(item.get('history'))
        else:
            row.append('')
        if item.get('processing') or item.get('processing') == False:
            row.append(item.get('processing'))
        else:
            row.append('')
        if item.get('keyProcessing') or item.get('keyProcessing') == False:
            row.append(item.get('keyProcessing'))
        else:
            row.append('')
        write_to_file('txt_fields/txt_fields_' + filename, row, 'a')

def do_processing(processing, parent_guid):
    data = json.loads(processing)
    values = data.get('data')
    ### Бежим по словарю вложенных полей, внутри точно есть key, value, остального нет
    for item in values:
        row = [commit_ts, op_scn, op_seq, op_type, processed_dt, dte, parent_guid, parent_guid, parent_guid, item.get('key'), item.get('value')]
        write_to_file('txt_processing_fields/txt_processing_fields_' + filename, row, 'a')

def do_history(history, parent_guid):
    data = json.loads(history)
    values = data.get('fields')
    ### Бежим по словарю вложенных полей, внутри точно есть key, остального может не быть (а может быть)
    for item in values:
        row = [commit_ts, op_scn, op_seq, op_type, processed_dt, dte, parent_guid, parent_guid, parent_guid, item.get('key')]
        if item.get('value') or item.get('value') == False:
            row.append(item.get('value'))
        else:
            row.append('')
        if item.get('index') or item.get('index') == False:
            row.append(item.get('index'))
        else:
            row.append('')
        if item.get('caption') or item.get('caption') == False:
            row.append(item.get('caption'))
        else:
            row.append('')
        write_to_file('txt_history_operation/txt_history_operation_' + filename, row, 'a')


def do_payment(payment, parent_guid):
    data = json.loads(payment)
    values = data.get('transactions')
    ### Бежим по словарю вложенных полей, внутри точно есть key, остального может не быть (а может быть)
    for item in values:
        row = [commit_ts, op_scn, op_seq, op_type, processed_dt, dte, parent_guid, parent_guid, parent_guid, item.get('id')]
        if item.get('main') or item.get('main') == False:
            row.append(item.get('main'))
        else:
            row.append('')
        if item.get('systemId') or item.get('systemId') == False:
            row.append(item.get('systemId'))
        else:
            row.append('')
        write_to_file('txt_payment_detail/txt_payment_detail_' + filename, row, 'a')

### Тут нужно обработать основную сущность (прокинуть ей ид), дальше обработать остальные
### Поля id, document_id, created_at, updated_at, creation_channel, client_iteraction, creation_source_type, creation_source_id, client_mdm_id, client_unc_id, client_tb_id,
### creation_system_id, provider_id, provider_service_id, client_product_id, client_product_number, client_product_type, payment_amount, commission_amount, limit_transaction_id,
### rsa_transaction_id, confirm_transaction_id, abs_transaction_id, status, sub_status, fields, description, call_back_notify, abs_transaction_system_id, processing_fields, history_operation, service_provider, payment_detail, guid
def do_smth(data):
    ### Добавили uuid
    guid = str(uuid.uuid4())

    row = []
    for item in data_headers:
        if item in headers:
            row.append(data[headers.index(item)])
        elif item == 'guid':
            row.append(guid)
        else:
            row.append('')
    
    if row[fields_index] and row[fields_index] != '':
        do_fields(row[fields_index], guid)
        row[fields_index] = ""
    if row[processing_index] and row[processing_index] != '':
        do_processing(row[processing_index], guid)
        row[processing_index] = ""
    if row[payment_detail_index] and row[payment_detail_index] != '' and row[payment_detail_index] != '{}':
        do_payment(row[payment_detail_index], guid)
        row[payment_detail_index] = ""
    elif row[payment_detail_index] == '{}':
        row[payment_detail_index] = ""
    if row[history_operation_index] and row[history_operation_index] != '':
        do_history(row[history_operation_index], guid)
        row[history_operation_index] = ""


    write_to_file('txt_payments/txt_payments_' + filename, row, 'a')


files = os.listdir(path)
os.makedirs(path_to_result + 'txt_payments', exist_ok=True)
os.makedirs(path_to_result + 'txt_payment_detail', exist_ok=True)
os.makedirs(path_to_result + 'txt_history_operation', exist_ok=True)
os.makedirs(path_to_result + 'txt_processing_fields', exist_ok=True)
os.makedirs(path_to_result + 'txt_fields', exist_ok=True)
os.makedirs(path_to_result + 'broken_items', exist_ok=True)

for item in files:
    filename = item
    write_log('Начало обработки файла ' + item + '\n')
    with open(path + item, newline='', encoding='utf8') as File:  
        df = csv.reader(File, delimiter='')
        headers = next(df)
        ###индексы подтаблиц (потому что могут отличаться от эталонного - не достает полей, например)
        write_to_file('txt_payments/txt_payments_' + filename, data_headers, 'w')
        if payment_detail_index:
            write_to_file('txt_payment_detail/txt_payment_detail_' + filename, payment_headers, 'w')
        if history_operation_index:
            write_to_file('txt_history_operation/txt_history_operation_' + filename, history_headers, 'w')
        if processing_index:
            write_to_file('txt_processing_fields/txt_processing_fields_' + filename, processing_headers, 'w')
        if fields_index:
            write_to_file('txt_fields/txt_fields_' + filename, fields_headers, 'w')
        write_to_file('broken_items/broken_items_' + filename, headers, 'w')
        for row in df:
            row = [cell.replace('\r\n', '') for cell in row]
            if len(row) == len(headers):
                try:
                    do_smth(row)
                    write_log('Обработка сущности с идентифкатором  ' + row[0] + '\n')
                except ValueError as e:
                    print('Ошибка парсинга джейсона в файле ', item)
                    print('Неправильная сущность ', row)
                    write_log('Ошибка обработки сущности с идентифкатором  ' + row[0] + '\n')
                    write_to_file('broken_items/broken_items_' + filename, row, 'a')
                except IndexError as e:
                    print('Ошибка парсинга джейсона в файле ', item)
                    print('Выход за границы массива в сущности ', row)
                    write_log('Ошибка обработки сущности с идентифкатором  ' + row[0] + '\n')
                    write_to_file('broken_items/broken_items_' + filename, row, 'a')
                except:
                    print('Ошибка парсинга джейсона в файле ', item)
                    print('Неизвестная ошибка ', row)
                    write_to_file('broken_items/broken_items_' + filename, row, 'a')
            else:
                print('Неверная длина сущности в файле ', item)
                print('Неправильная сущность ', row)
                print('Ожидалась длина ', len(headers), ' Получили сущность длиной ', len(row))
                write_to_file('broken_items/broken_items_' + filename, row, 'a')
    write_log('Конец обработки файла ' + item + '\n')
    write_log('============================================================================' + '\n')
