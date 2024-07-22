import os
import re

 
# Указываем путь к директории
directory = 'C:/Users/okachan/Documents/втб/прод_наши_даги'
 
# Получаем список файлов
files = os.listdir(directory)

for dag_path in files:
    if dag_path[-3:] == '.py':
        with open(directory + '/' + dag_path, 'r', encoding="utf-8") as file:
            reference = file.read()

        with open(directory + '/formatted/' + dag_path, 'r', encoding="utf-8") as file:
            edited = file.read()

        reference = re.sub('[\s]*', '', reference)
        reference = re.sub('"incrementField":"hdp_processed_dttm",', '"incrementField":"hdp_processed_dttm"', reference)
        edited = re.sub('[\s]*', '', edited)
        if reference != edited:
            print(dag_path)
            print()