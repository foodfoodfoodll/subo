import json
import os
from mappings_schema_registry.excel_mapping import create_excel_mapping

def create_mapping(path):
    database = ""
    table = ""
    topic = ""
    fields = []
    output = []
    index = 1
    files = os.listdir(path)
    for item in files:
        if item.endswith(".json"):
            with open(path + "/" + item, encoding="utf-8-sig") as file:
                data: dict = json.load(file)
                schema = json.loads(data['schema'].replace("\\", ""))
                topic = data['subject'].split("-")[0]
                database = schema["namespace"]
                table = schema["name"]
                fields = schema["fields"]
                for field in fields:
                    name = field["name"]
                    if isinstance(field["type"], list):
                        type = field["type"][1]
                        required = False
                    else:
                        type = field["type"]
                        required = True
                    maxLength = field["maxLength"] if "maxLength" in field else 0

                    if type == "string" and maxLength > 0:
                        type = "varchar2(" + str(maxLength) + " char)"
                    elif type == "string":
                        type = "varchar2(1000 char)"
                    elif type == "long" or type == "double":
                        type = "number"
                    elif type == "boolean":
                        type = "char(1 byte)"

                    row_data: list = [
                        index,                                      # "#"
                        "",                                         # "Тип объекта"
                        "",                                         # "Система"
                        topic,                                      # "Топик Kafka 610_4"
                        "",                                         # "База данных"
                        table,                                      # "Таблица"
                        "",                                         # "Описание таблицы"
                        name.upper(),                               # "Имя поля"
                        "",                                         # "Комментарий поля"
                        type,                                       # "Тип данных"
                        "",                                         # "PK"
                        "",                                         # "Not Null"
                        "1642_19 Озеро данных (детальный слой)",    # "База/Система"
                        "prod_repl_subo_" + database,               # "Схема"
                        table.lower(),                              # "Таблица"
                        "",                                         # "Название родительской таблицы"
                        "",                                         # "Описание таблицы"
                        name.lower(),                               # "Код атрибута"
                        "",                                         # "Описание атрибута"
                        "",                                         # "Комментарий"
                        "string",                                   # "Тип данных"
                        "",                                         # "Length"
                        "",                                         # "PK"
                        "",                                         # "FK"
                        "+" if required else "",                    # "Not Null",
                        "",                                         # "Rejectable"
                        "",                                         # "Trace New Values"
                    ]
                    output.append(row_data)
                    index += 1
                output.append([index, "", "", topic, "", table, "", "", "", "", "", "", "1642_19 Озеро данных (детальный слой)", "prod_repl_subo_" + database, table, "", "", "hdp_processed_dttm", "", "", "timestamp", "", "", "", "+", "", "",])
                index += 1
    create_excel_mapping(output, path + "/S2T_mapping.xlsx")