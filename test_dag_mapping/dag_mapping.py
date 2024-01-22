import re
import json
import pandas as pd
import os

from tkinter import *
from tkinter import messagebox, filedialog
from tkinter.ttk import Checkbutton
from tkinter.scrolledtext import ScrolledText

start_json = '"flows":'
end_json = 'def spark_operator'

dags_path = '1642_19_datalake_subo_gsz_1567_parse_load.py'
mappings_directory = '.'


def print_message(text, widget, output_to_file=False, path=''):
    widget.insert(END, str(text) + '\n')
    if output_to_file:
        with open(path+'/result.txt', 'a', encoding='utf-8') as file:
            file.write(text + '\n') 

def get_attrs_from_dag(dags_path):
    try:
        with open(dags_path, 'r', encoding="utf-8") as file:
            dag = file.read()
    except  Exception as error:
        messagebox.showerror("Ошибка!", "Ошибка чтения файла с дагом. \n" + str(error))
        return 0
    dag = dag.replace('\'', '"')
    dag = dag.split(start_json)[1].split(end_json)

    dag = re.sub('(#[\S| ]*\n)|\t|\n| ', '', dag[0])    #убираем в даге комментарии, табуляцию, переносы строк и пробелы
    dag = dag.replace('},}', '}}').replace('},]','}]')  #убираем лишние запятые, которые могут сломать чтение джейсона
    dag = re.split('loadType', dag)

    dag_dict = {}

    for i in range(1, len(dag)):
        tmp = re.split('parsedColumns":', dag[i])
        tmp = re.split(']', tmp[1])
        attrs = tmp[0] + ']'
        attrs = attrs.lower()
        for s in tmp:
            if s.find('"target":{') > 0:
                table = re.split('"target":{', s)
                break
        # table = re.split('}', table[1])
        table = re.split(',', table[1])
        table = re.split('}', table[0])
        table = table[0].replace('"', '').replace('table:','').lower()

        json_str = '{"' + table + '":' + attrs + '}'

        dag_dict.update(json.loads(json_str))
        dag_dict[table].append({'name': 'current_ts', 'type': 'timestamp', 'alias': 'hdp_processed_dttm'})
    return dag_dict

def get_attrs_from_mapping(mappings_directory):
    try:
        list_dir = os.listdir(mappings_directory)
        mappings = []
        for item in list_dir:
            if item[-5:] == '.xlsx' and item[0:2] != '~$':
                mappings.append(mappings_directory + '/' + item)
    except  Exception as error:
        messagebox.showerror("Ошибка!", "Ошибка чтения маппингов. \n" + str(error))
        return 0
    
    if len(mappings) == 0:
        messagebox.showerror("Ошибка!", "В указанной директории нет xlsx-файлов")
        return 0

    excel_df = None
    for path in mappings:
        tmp_df = pd.read_excel(path, sheet_name='Mapping', skiprows=1)
        column_names = list(tmp_df.columns.values)

        if 'Таблица.1' in column_names:
            table_col = 'Таблица.1'
        else:
            table_col = 'Таблица'

        if 'Код атрибута.1' in column_names:
            code_col = 'Код атрибута.1'
        else:
            code_col = 'Код атрибута'

        if 'Тип данных.1' in column_names:
            type_col = 'Тип данных.1'
        else:
            type_col = 'Тип данных'
        tmp_df = tmp_df[[table_col, code_col, type_col]]

        if excel_df is None:
            excel_df = tmp_df
        else:
            excel_df = pd.concat([excel_df, tmp_df])

    mapping_dict = {}
    for index, row in excel_df.iterrows():
        try:
            table_name = row[table_col].lower()
        except:
            table_name = row[table_col]
        if table_name not in mapping_dict.keys():
            mapping_dict[table_name]=[]
        m = mapping_dict[table_name]
        try:
            m.append({'name': row[code_col].lower().strip(), 'type':  row[type_col].lower()})
        except:
            m.append({'name': row[code_col], 'type':  row[type_col]})
        mapping_dict[table_name] = m
    return mapping_dict

def compare_attrs(dag_dict, mapping_dict, output_diff, output_to_file, mappings_directory):
    for key, value in mapping_dict.items():
        mapping_set = set()
        dag_set = set()
        for item in value:
            mapping_set.add(item['name'])
        try:
            for item in dag_dict[key]:
                if 'alias' not in item.keys():
                    dag_set.add(item['name'])
                else:
                    dag_set.add(item['alias'])
            if mapping_set != dag_set:
                print_message("В таблице " + key + " есть расхождения по атрибутам", res_txt, output_to_file, mappings_directory)
                if output_diff:
                    print_message('Атрибуты, которые есть только в маппинге', res_txt, output_to_file, mappings_directory)
                    print_message(str(mapping_set - dag_set), res_txt, output_to_file, mappings_directory)
                    print_message('Атрибуты, которые есть только в даге', res_txt, output_to_file, mappings_directory)
                    print_message(str(dag_set - mapping_set)+'\n', res_txt, output_to_file, mappings_directory)    
        except:
            print_message('В даге нет таблицы ' + key, res_txt, output_to_file, mappings_directory)
        

def main(dags_path, mappings_directory, output_diff, output_to_file):
    dag_dict = get_attrs_from_dag(dags_path)
    if not dag_dict:
        return
    mapping_dict = get_attrs_from_mapping(mappings_directory)
    if not mapping_dict:
        return
    compare_attrs(dag_dict, mapping_dict, output_diff, output_to_file, mappings_directory)

window = Tk()
window.title("Сравнение дага и маппинга")
window.geometry('600x380')

dags_path = StringVar()
mappings_directory = StringVar()
output_diff = BooleanVar()  
output_to_file = BooleanVar()  
main_dag = BooleanVar()  

dags_path_lbl = Label(window, text="Выберите файл дага:")
dags_path_txt = Entry(window, width=70, textvariable=dags_path, bg='white')
dags_path_btn = Button(window, text="...", command=lambda: dags_path.set(filedialog.askopenfilename()), bg='white', width=2)

dags_path_lbl.place(x=10, y=10) 
dags_path_txt.place(x=130, y=13)
dags_path_btn.place(x=555, y=8)


mappings_directory_lbl = Label(window, text="Выберите директирию с маппингами:")
mappings_directory_txt = Entry(window, width=54, textvariable=mappings_directory, bg='white')
mappings_directory_btn = Button(window, text="...", command=lambda: mappings_directory.set(filedialog.askdirectory()), bg='white', width=2)

mappings_directory_lbl.place(x=10, y=35) 
mappings_directory_txt.place(x=225, y=38)
mappings_directory_btn.place(x=555, y=33)


output_diff_chk = Checkbutton(window, var=output_diff, padding=0, text = 'Вывести атрибуты, которые не совпадают')
output_diff_chk.place(x=10, y=60) 

output_to_file_chk = Checkbutton(window, var=output_to_file, padding=0, text = 'Сохранить результаты в файл')
output_to_file_chk.place(x=10, y=85) 


result_lbl = Label(window, text='Результат:')
result_lbl.place(x=10, y=110) 

result_btn = Button(window, text="Старт", width=15, height=2,bg='white',
                    command=lambda: main(dags_path.get(),
                                         mappings_directory.get(), 
                                         output_diff.get(), 
                                         output_to_file.get()))
result_btn.place(x=463, y=83) 

res_txt = ScrolledText(window, width=70,  height=14)
res_txt.place(x=10, y=135) 

window.mainloop()
