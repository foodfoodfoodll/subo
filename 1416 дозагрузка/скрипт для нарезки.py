import csv
import json
import uuid
from datetime import datetime

all_data = []
data_headers = ['id', 'document_id', 'created_at', 'updated_at', 'creation_channel', 'client_iteraction', 'creation_source_type', 'creation_source_id', 'client_mdm_id', 'client_unc_id', 'client_tb_id', 'creation_system_id', 'provider_id', 'provider_service_id', 'client_product_id', 'client_product_number', 'client_product_type', 'payment_amount', 'commission_amount', 'limit_transaction_id', 'rsa_transaction_id', 'confirm_transaction_id', 'abs_transaction_id', 'status', 'sub_status', 'fields', 'description', 'call_back_notify', 'abs_transaction_system_id', 'processing_fields', 'history_operation', 'service_provider', 'payment_detail', 'guid']
fields_data = []
fields_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'value', 'history', 'processing', 'keyProcessing']
processing_data = []
processing_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'value']
history_data = []
history_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'key', 'index', 'value', 'caption']
payment_data = []
payment_headers = ['commit_ts', 'op_scn', 'op_seq', 'op_type', 'processed_dt', 'dte', 'guid', 'parent_guid', 'object_guid', 'id', 'main', 'systemId']
now = datetime.now()
commit_ts = now.strftime("%Y-%m-%d %H:%M:%S")
processed_dt = now.strftime("%Y-%m-%d %H:%M:%S")
op_scn = ""
op_seq = ""
op_type = ""
dte = ""

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
        fields_data.append(row)

def do_processing(processing, parent_guid):
    #print(processing)
    data = json.loads(processing)
    values = data.get('data')
    ### Бежим по словарю вложенных полей, внутри точно есть key, value, остального нет
    for item in values:
        #print(item.get('key'))
        #print(item.get('value'))
        row = [commit_ts, op_scn, op_seq, op_type, processed_dt, dte, parent_guid, parent_guid, parent_guid, item.get('key'), item.get('value')]
        processing_data.append(row)

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
        history_data.append(row)

def do_payment(payment, parent_guid):
    if json.loads(payment).get('transactions'):
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
            payment_data.append(row)

### Тут нужно обработать основную сущность (прокинуть ей ид), дальше обработать остальные
### Поля id, document_id, created_at, updated_at, creation_channel, client_iteraction, creation_source_type, creation_source_id, client_mdm_id, client_unc_id, client_tb_id,
### creation_system_id, provider_id, provider_service_id, client_product_id, client_product_number, client_product_type, payment_amount, commission_amount, limit_transaction_id,
### rsa_transaction_id, confirm_transaction_id, abs_transaction_id, status, sub_status, fields, description, call_back_notify, abs_transaction_system_id, processing_fields, history_operation, service_provider, payment_detail, guid
def do_smth(data):
    ### Добавили uuid
    data.append(str(uuid.uuid4()))
    ###Обработка вложенных структур (номер ячейки посмотреть на данных)
    ###Для настоящих данных
    if (data[-9] != '' ):
        do_fields(data[-9], data[-1])
        data[-9] = ""
    if (data[-5] != ''):
        do_processing(data[-5], data[-1])
        data[-5] = ""
    if (data[-4] != ''):
        do_history(data[-4], data[-1])
        data[-4] = ""
    if (data[-2] != '{}' and data[-2] != ''):
        do_payment(data[-2], data[-1])
    data[-2] = ""
    data[-3] = ""

    ''' ###Для тестов
    do_fields(data[-5], data[-1])
    data[-5] = ""
    do_processing(data[-4], data[-1])
    data[-4] = ""
    do_payment(data[-2], data[-1])
    data[-2] = ""
    data[-3] = ""
    '''

    all_data.append(data)


with open('test1.csv', newline='', encoding='utf8') as File:
    df = csv.reader(File, delimiter=';')
    headers = next(df)
    for row in df:
        do_smth(row)
###Запись
all_data_file = open('txt_payments.csv', 'w', newline ='', encoding='UTF8')
with all_data_file:
	writer = csv.writer(all_data_file)
	writer.writerow(data_headers)
	writer.writerows(all_data)

fields_file = open('txt_fields.csv', 'w', newline ='', encoding='UTF8')
with fields_file:
	writer = csv.writer(fields_file)
	writer.writerow(fields_headers)
	writer.writerows(fields_data)

payment_file = open('txt_payment_detail.csv', 'w', newline ='', encoding='UTF8')
with payment_file:
	writer = csv.writer(payment_file)
	writer.writerow(payment_headers)
	writer.writerows(payment_data)

processing_file = open('txt_processing_fields.csv', 'w', newline ='', encoding='UTF8')
with processing_file:
	writer = csv.writer(processing_file)
	writer.writerow(processing_headers)
	writer.writerows(processing_data)

history_file = open('txt_history_operation.csv', 'w', newline ='', encoding='UTF8')
with history_file:
	writer = csv.writer(history_file)
	writer.writerow(history_headers)
	writer.writerows(history_data)
