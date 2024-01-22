import json
import pandas as pd
import glob
import os

from tkinter import *
from tkinter import messagebox

def print_message(widget, path, message, output_to_file=False):
    widget.insert(END, str(message) + '\n')
    if output_to_file:
        with open(path + '/result.txt', 'a', encoding='utf-8') as file:
            file.write(message + '\n') 


def add_attr(source, result, parent_key, table):
    """
    source:         payload из json
    result:         словарь, куда записывается результат
    parent_key:     участвует в формировании названия атрибута, собирается из путя
    table:          название таблицы, к которой относится атрибут

    преобразование json в список словарей с данными.
    Названия атрибутов собираются из пути в json
    """
    if table not in result.keys():
        result[table] = {}
    for key, value in source.items():
        if type(value) not in [dict, list]:
            k = parent_key + '_' + key
            k = k.lower()
            if k[0] == '_':
                k = k[1:]
            if k not in result[table].keys():
                result[table][k] = []
            result[table][k].append(value)
        elif type(value) is dict:
            add_attr(value, result, parent_key + '_' + key, table)
        elif type(value) is list:
            for i in range(len(value)):
                if key == 'Records':
                    add_attr(value[i], result, parent_key, table + '_' + key)
                else: 
                    add_attr(value[i], result, parent_key + '_' + key, table + '_' + key)

def get_schema_from_s2t(s2t_path):
    """
    s2t_path:       path s2t-файла
    Получение структура таблиц из s2t.
    Из структура исключаются поля, указанные в списке exclude_columns и хэши
    """
    df = pd.read_excel(s2t_path, sheet_name='Mapping', skiprows=1)
    column_names = list(df.columns.values)

    if 'Таблица.1' in column_names:
        table_col = 'Таблица.1'
    else:
        table_col = 'Таблица'

    if 'Код атрибута.1' in column_names:
        code_col = 'Код атрибута.1'
    else:
        code_col = 'Код атрибута'   

    df = df[[table_col, code_col]]
    
    grouped = df.groupby([table_col])[code_col].apply(list).reset_index()
    del df
    schema = {}
    for index, row in grouped.iterrows():
        schema[row[table_col]] = []
        for item in row[code_col]:
            item = item.lower().strip()
            schema[row[table_col]].append(item)
    return schema

def exclude_tech_columns(schema, excluded_columns):
    res = {}
    for table, columns in schema.items():
        if table not in res.keys():
            res[table] = []
        for item in columns:
            if item not in excluded_columns and item[-5:] != '_hash':
                res[table].append(item)
    return res
    
def get_json_dict_list(json_path):
    """
    json_path:  path файла, в котором сохранены сообщения из кафки в формате json.

    Помещает сообщения кафки в список.
    Каждый элемент списка - словарь, полученный из json
    """
    with open(json_path, 'r', encoding="utf-8") as file:
        lines = file.readlines()
        json_list = []
        for item in lines:
            j = json.loads(item)
            json_list.append(j)
    return json_list

def compare_structures(path, json_schema, output_only_failures, output_in_file, files_directory, result_widget):
    res_equals_flag = True
    for file in glob.glob(path + '\*.csv'):
        df_csv = pd.read_csv(file)
        csv_schema = set(df_csv)
        table_name = file.split('\\')[-1].split('.')[0]
        json_schema_set = set(json_schema[table_name])
        if json_schema_set != csv_schema:
            if len(json_schema_set) > len(csv_schema):
                message = table_name + '. В csv нет атрибутов, которые есть в S2T: ' + str(json_schema_set - csv_schema)
            elif len(json_schema_set) < len(csv_schema):
                message = table_name + '. В S2T нет атрибутов, которые есть в csv: ' + str(csv_schema - json_schema_set)
            elif len(json_schema_set) == len(csv_schema):
                message = table_name + '. Не совпадают названия атрибутов. ' + str(json_schema_set.symmetric_difference(csv_schema))
            res_equals_flag *= False
        else:
            if not output_only_failures:
                message = table_name + '. Структуры в S2T и csv совпадают.'
        print_message(result_widget, files_directory, message, output_in_file)
    return res_equals_flag

def get_csv_df_list(csv_directory_path, json_df_list):
    """
    csv_directory_path:     директория, в которой хранятся csv-файлы
    json_df_list:           список датафреймов, полученный из json

    Получение списка датафреймов из csv. 
    Название таблицы вытаскивается из названия файла. 
    Порядок колонок приводится к порядку из маппинга.
    """
    list_df_csv = {}
    for file in glob.glob(csv_directory_path + '\*.csv'):
        df_csv = pd.read_csv(file)
        table_name = file.split('\\')[-1].split('.')[0]
        df_csv = df_csv[list(json_df_list[table_name])]
        list_df_csv[table_name] = pd.DataFrame(data=df_csv)
    return list_df_csv

def from_dict_to_df(json_list, json_df_list, json_schema):
    """
    json_list:      список с сообщениями в формате json
    json_df_list:   список датафреймов
    json_schema:    структура таблиц, полученная из s2t

    Создание датафреймов из словарей json.
    Происходит сравнение со схемой. Если атрибута нет в датафрейме, то добавляется колонка с пустыми значениями
    """
    for table_name, table_data in json_list.items():
        tmp_df = pd.DataFrame(data=table_data)
        num = 0
        for col in json_schema[table_name]:
            if col not in list(tmp_df):
                tmp_df.insert(num, col, [None] * len(tmp_df)) #numpy.full(len(list_df[n]),numpy.nan)
            num+=1
        if table_name not in json_df_list.keys():
            json_df_list[table_name] = pd.DataFrame(data=tmp_df)
        else:
            json_df_list[table_name] = pd.concat([json_df_list[table_name], tmp_df]) 

def compare_json_with_csv(json_df_list, csv_df_list, output_only_failures, output_in_file, output_examples, result_widget, files_directory):
    """
    json_df_list:       список датафреймов, полученных из json
    csv_df_list:        список датафреймов, полученных из csv

    Датафреймы с данными одной таблицы склеиваются в один, после чего удаляются дубли.
    Если в результате получен пустой датафрейм, то данные совпадают.
    """
    for table_name in json_df_list.keys():
        if table_name in csv_df_list.keys():
            df = pd.concat([json_df_list[table_name], csv_df_list[table_name]]) 
            df = df.drop_duplicates(keep=False)
            if len(df) == 0:
                if not output_only_failures:
                    print_message(result_widget, files_directory, table_name + '. Данные совпадают.', output_in_file)
            else:
                print_message(result_widget, files_directory, table_name + '. Данные не совпадают.', output_in_file)
                if output_examples:
                    print_message(result_widget, files_directory, df.to_string() + '\n', output_in_file)
        else:
            print_message(result_widget, files_directory, table_name + '. Не найден csv для этой таблицы.', output_in_file)

def main(config, result_widget):
    database = config['database_name']
    excluded_columns = config['exclude_columns']
    files_directory = config['files_directory']

    output_in_file = config['output_in_file']  #true = file
    output_only_failures = config['output_only_failures']
    output_examples = config['output_examples']
    continue_if_the_structure_does_not_match = config['continue_if_the_structure_does_not_match']

    list_dir = os.listdir(files_directory)

    for name in list_dir:
        if '.' not in name:
            csv_path = files_directory + '\\' + name
        elif name[-5:] == '.xlsx' and name[:2] != '~$':
            s2t_path = files_directory + '\\' + name
        elif name[-5:] == '.json':
            json_path = files_directory + '\\' + name

    try:
        full_json_schema = get_schema_from_s2t(s2t_path)
    except Exception as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return

    if not continue_if_the_structure_does_not_match \
        and not compare_structures(csv_path, full_json_schema, output_only_failures, output_in_file, files_directory, result_widget):
        return

    json_schema_without_tech = exclude_tech_columns(full_json_schema, excluded_columns)
    
    try:
        raw_json_list = get_json_dict_list(json_path)
    except NameError as ex:
        print_message(result_widget, files_directory, 'Не найден файл с json', output_in_file)
        return
    except PermissionError as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return
    except FileNotFoundError as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return
    except json.decoder.JSONDecodeError as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return
    except Exception as ex:
        print_message(result_widget, files_directory, 'Ошибка при чтении файла с json.', output_in_file)
        return

    json_df_list = {}

    for json_item in raw_json_list:
        json_data_dict = {}
        root_name = database + '_' + json_item['meta']['BaseClass']
        payload = json_item['payload']
        records = payload[0]['Records'][0]

        add_attr(payload[0], json_data_dict, '', root_name)
        del json_data_dict[root_name]
        from_dict_to_df(json_data_dict, json_df_list, json_schema_without_tech)

    del json_data_dict

    try:
        csv_df_list = get_csv_df_list(csv_path, json_df_list)
    except NameError as ex:
        print_message(result_widget, files_directory, 'Не найда директория с csv-файлами', output_in_file)
        return
    except PermissionError as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return
    except FileNotFoundError as ex:
        print_message(result_widget, files_directory, ex, output_in_file)
        return
    except:
        print_message(result_widget, files_directory, 'Ошибка при чтении csv-файлов.', output_in_file)

    compare_json_with_csv(json_df_list, csv_df_list, output_only_failures, output_in_file, output_examples, result_widget, files_directory)
    print_message(result_widget, files_directory, '\n\n', output_in_file)
