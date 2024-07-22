import re
import os
 
# Указываем путь к директории
directory = 'C:/Users/okachan/Documents/втб/файлы/dag/1901 - rcvr - Взыскание/202402'
 
# Получаем список файлов
files = os.listdir(directory)

tab = '    '
new_template = 1
for dag_path in files:
    if dag_path[-3:] == '.py':
        with open(directory + '/' + dag_path, 'r', encoding="utf-8") as file:
            dag = file.read()

        if new_template == 0:    # общая часть для старой версии дагов
            dag = re.sub('\}\,[\s]*\]', '}]', dag)
            dag = re.sub('\}\,[\s]*\}', '}}', dag)
            dag = re.sub('\n[\s]*config = json', f'\n{tab}config = json', dag)
            dag = re.sub('\n[\s]*target_db =', f'\n{tab}target_db =', dag)
            dag = re.sub('\n[\s]*etl_schema =', f'\n{tab}etl_schema =', dag)
            dag = re.sub('\n[\s]*assert False, ', f'\n{tab}assert False, ', dag)
            dag = re.sub('\n[\s]*"', f'\n{tab}"', dag)
        else:    # общая часть для новой версии дагов
            dag = re.sub('\n[\s]*STAGE_SUBFOLDER', f'\n{tab}STAGE_SUBFOLDER', dag)
            dag = re.sub('\n[\s]*\'owner\'', f'\n{tab}"owner"', dag)
            dag = re.sub('\n[\s]*\'email\'', f'\n{tab}"email"', dag)
            dag = re.sub('\n[\s]*\'depends_on_past\'', f'\n{tab}"depends_on_past"', dag)
            dag = re.sub('\n[\s]*\'start_date\'', f'\n{tab}"start_date"', dag)
            dag = re.sub('\n[\s]*\'retries\'', f'\n{tab}"retries"', dag)
            dag = re.sub('\n[\s]*\'connType\'', f'\n{tab}"connType"', dag)
            dag = re.sub('\n[\s]*\'keyPassword\'', f'\n{tab}"keyPassword"', dag)
            dag = re.sub('\n[\s]*\'keystoreLocation\'', f'\n{tab}"keystoreLocation"', dag)
            dag = re.sub('\n[\s]*\'keystorePassword\'', f'\n{tab}"keystorePassword"', dag)
            dag = re.sub('\n[\s]*\'protocol\'', f'\n{tab}"protocol"', dag)
            dag = re.sub('\n[\s]*\'servers\'', f'\n{tab}"servers"', dag)
            dag = re.sub('\n[\s]*\'truststoreLocation\'', f'\n{tab}"truststoreLocation"', dag)
            dag = re.sub('\n[\s]*\'truststorePassword\'', f'\n{tab}"truststorePassword"', dag)
            dag = re.sub('\n[\s]*\'url\'', f'\n{tab}"url"', dag)
            dag = re.sub('\n[\s]*\'driver\'', f'\n{tab}"driver"', dag)
            dag = re.sub('\n[\s]*\'user\'', f'\n{tab}"user"', dag)
            dag = re.sub('\n[\s]*\'password\'', f'\n{tab}"password"', dag)
            dag = re.sub('\n[\s]*\'targetSchema\'', f'\n{tab}"targetSchema"', dag)
            dag = re.sub('\n[\s]*\'etlSchema\'', f'\n{tab}"etlSchema"', dag)
            dag = re.sub('\n[\s]*\'logsTable\'', f'\n{tab}"logsTable"', dag)
            dag = re.sub('\n[\s]*\'flagName\'', f'\n{tab}"flagName"', dag)

        dag = re.sub('\n[\s]*\'connection\':', f'\n{tab}\'connection\':', dag)
        dag = re.sub('\n[\s]*\'commonInfo\':', f'\n{tab}\'commonInfo\':', dag)
        dag = re.sub('\n[\s]*\'flows\':\[', f'\n{tab}\'flows\': [\n', dag)
        dag = re.sub('\n[\s]*\{\'loadType\'', '\n'+ tab*2 +'{\n' + tab*3 + '\'loadType\'', dag)
        dag = re.sub('\n[\s]*\'source\':[\s]*\{', '\n' + tab*3 + '\'source\': {\n', dag)
        dag = re.sub('\n[\s]*\'schema\':', f'\n{tab*4}\'schema\':', dag)
        dag = re.sub('\n[\s]*\'table\':[\s]*', f'\n{tab*4}\'table\': ', dag)
        dag = re.sub('\n[\s]*\'columnsWithJson\'', f'\n{tab*4}\'columnsWithJson\'', dag)
        dag = re.sub('\n[\s]*\'explodedColumns\'', f'\n{tab*4}\'explodedColumns\'', dag)
        dag = re.sub('\n[\s]*\'parsedColumns\':[\s]*\[', f'\n{tab*4}\'parsedColumns\': [\n', dag)
        dag = re.sub('\n[\s]*\{\'name\'', '\n' + tab*5 + '{\'name\'', dag)
        dag = re.sub('\n[\s]*\'columnCasts\'', f'\n{tab*4}\'columnCasts\'', dag)
        dag = re.sub('}],', '}\n'+tab*4+'],', dag)
        dag = re.sub('\n[\s]*\'preFilterCondition\':[\s]*\'value', f'\n{tab*4}\'preFilterCondition\': \'value', dag)
        dag = re.sub('\n[\s]*\'preFilterCondition\':[\s]*"value', f'\n{tab*4}\'preFilterCondition\': "value', dag)
        dag = re.sub('\n[\s]*\'postFilterCondition\'', f'\n{tab*4}\'postFilterCondition\'', dag)
        dag = re.sub('\n[\s]*\'incrementField\':[\s]*\'hdp_processed_dttm\'[,|\s]*\},', f'\n{tab*4}\'incrementField\': \'hdp_processed_dttm\'\n{tab*3}'+'},', dag)
        dag = re.sub('\n[\s]*\'target\':[\s]*\{[\s]*\'table\'', f'\n{tab*3}\'target\': ' +'{\n' + tab*4 + '\'table\'', dag)
        dag = re.sub('\n[\s]*\'aggregationField\':[\s]*\'dte\'', f'\n{tab*4}\'aggregationField\': \'dte\'', dag)
        dag = re.sub('\n[\s]*\'partitionFields\':[\s]*\[\'dte\'\]', f'\n{tab*4}\'partitionFields\': [\'dte\']', dag)
        dag = re.sub('\n[\s]*\'customPartitioning\':[\s]*\'Day\'', f'\n{tab*4}\'customPartitioning\': \'Day\'', dag)
        dag = re.sub('\n[\s]*\'updateAllowed\':[\s]*True', f'\n{tab*4}\'updateAllowed\': True', dag)

        dag = re.sub('[\s]*\}\,[\s]*\'addInfo\':[\s]*\{[\s]*\'orderField\'', '\n'+tab*3+'},\n' + tab*3 + '\'addInfo\': {\n' + tab*4 + '\'orderField\'', dag)
        dag = re.sub('[\s]*\}[\s]*\}\,', '\n' +tab*3+ '}\n' + tab*2 + '},', dag)

        if new_template == 0:    # общая часть для старой версии дагов
            dag = re.sub('\}\,[\s]*\]', '}]', dag)
            dag = re.sub('\}\,[\s]*\}', '}}', dag)
            dag = re.sub('\n[\s]*config = json', f'\n{tab}config = json', dag)
            dag = re.sub('\n[\s]*target_db =', f'\n{tab}target_db =', dag)
            dag = re.sub('\n[\s]*etl_schema =', f'\n{tab}etl_schema =', dag)
            dag = re.sub('\n[\s]*assert False, ', f'\n{tab}assert False, ', dag)
            dag = re.sub('\n[\s]*"', f'\n{tab}"', dag)
        else:    # общая часть для ноыой версии дагов
            dag = re.sub('\n[\s]*STAGE_SUBFOLDER', f'\n{tab}STAGE_SUBFOLDER', dag)
            dag = re.sub('\n[\s]*"owner"', f'\n{tab}"owner"', dag)
            dag = re.sub('\n[\s]*"email"', f'\n{tab}"email"', dag)
            dag = re.sub('\n[\s]*"depends_on_past"', f'\n{tab}"depends_on_past"', dag)
            dag = re.sub('\n[\s]*"start_date"', f'\n{tab}"start_date"', dag)
            dag = re.sub('\n[\s]*"retries"', f'\n{tab}"retries"', dag)
            dag = re.sub('\n[\s]*"connType"', f'\n{tab}"connType"', dag)
            dag = re.sub('\n[\s]*"keyPassword"', f'\n{tab}"keyPassword"', dag)
            dag = re.sub('\n[\s]*"keystoreLocation"', f'\n{tab}"keystoreLocation"', dag)
            dag = re.sub('\n[\s]*"keystorePassword"', f'\n{tab}"keystorePassword"', dag)
            dag = re.sub('\n[\s]*"protocol"', f'\n{tab}"protocol"', dag)
            dag = re.sub('\n[\s]*"servers"', f'\n{tab}"servers"', dag)
            dag = re.sub('\n[\s]*"truststoreLocation"', f'\n{tab}"truststoreLocation"', dag)
            dag = re.sub('\n[\s]*"truststorePassword"', f'\n{tab}"truststorePassword"', dag)
            dag = re.sub('\n[\s]*"url"', f'\n{tab}"url"', dag)
            dag = re.sub('\n[\s]*"driver"', f'\n{tab}"driver"', dag)
            dag = re.sub('\n[\s]*"user"', f'\n{tab}"user"', dag)
            dag = re.sub('\n[\s]*"password"', f'\n{tab}"password"', dag)
            dag = re.sub('\n[\s]*"targetSchema"', f'\n{tab}"targetSchema"', dag)
            dag = re.sub('\n[\s]*"etlSchema"', f'\n{tab}"etlSchema"', dag)
            dag = re.sub('\n[\s]*"logsTable"', f'\n{tab}"logsTable"', dag)
            dag = re.sub('\n[\s]*"flagName"', f'\n{tab}"flagName"', dag)

        dag = re.sub('\n[\s]*"connection":', f'\n{tab}"connection":', dag)
        dag = re.sub('\n[\s]*"commonInfo":', f'\n{tab}"commonInfo":', dag)
        dag = re.sub('\n[\s]*"flows":\[', f'\n{tab}"flows": [\n', dag)
        dag = re.sub('\n[\s]*\{"loadType"', '\n'+ tab*2 +'{\n' + tab*3 + '"loadType"', dag)
        dag = re.sub('\n[\s]*"source":[\s]*\{', '\n' + tab*3 + '"source": {\n', dag)
        dag = re.sub('\n[\s]*"schema":', f'\n{tab*4}"schema":', dag)
        dag = re.sub('\n[\s]*"table":[\s]*', f'\n{tab*4}"table": ', dag)
        dag = re.sub('\n[\s]*"columnsWithJson"', f'\n{tab*4}"columnsWithJson"', dag)
        dag = re.sub('\n[\s]*"explodedColumns"', f'\n{tab*4}"explodedColumns"', dag)
        dag = re.sub('\n[\s]*"parsedColumns":[\s]*\[', f'\n{tab*4}"parsedColumns": [\n', dag)
        dag = re.sub('\n[\s]*\{"name"', '\n' + tab*5 + '{"name"', dag)
        dag = re.sub('\n[\s]*"columnCasts"', f'\n{tab*4}"columnCasts"', dag)
        dag = re.sub('}],', '}\n'+tab*4+'],', dag)
        dag = re.sub('\n[\s]*"preFilterCondition":[\s]*"value', f'\n{tab*4}"preFilterCondition": "value', dag)
        # dag = re.sub('"%', '\'%', dag)
        # dag = re.sub('%"', '%\'', dag)
        # dag = re.sub('""\,', '\'",', dag)
        dag = re.sub('\n[\s]*"postFilterCondition"', f'\n{tab*4}"postFilterCondition"', dag)
        dag = re.sub('\n[\s]*"incrementField":[\s]*"hdp_processed_dttm"[,|\s]*\},', f'\n{tab*4}"incrementField": "hdp_processed_dttm"\n{tab*3}'+'},', dag)
        dag = re.sub('\n[\s]*"target":[\s]*\{[\s]*"table"', f'\n{tab*3}"target": ' +'{\n' + tab*4 + '"table"', dag)
        dag = re.sub('\n[\s]*"aggregationField":[\s]*"dte"', f'\n{tab*4}"aggregationField": "dte"', dag)
        dag = re.sub('\n[\s]*"partitionFields":[\s]*\["dte"\]', f'\n{tab*4}"partitionFields": ["dte"]', dag)
        dag = re.sub('\n[\s]*"customPartitioning":[\s]*"Day"', f'\n{tab*4}"customPartitioning": "Day"', dag)
        dag = re.sub('\n[\s]*"updateAllowed":[\s]*True', f'\n{tab*4}"updateAllowed": True', dag)

        dag = re.sub('[\s]*\}\,[\s]*"addInfo":[\s]*\{[\s]*"orderField"', '\n'+tab*3+'},\n' + tab*3 + '"addInfo": {\n' + tab*4 + '"orderField"', dag)
        dag = re.sub('[\s]*\}[\s]*\}\,', '\n' +tab*3+ '}\n' + tab*2 + '},', dag)


# dag = re.sub('\n[\s]*', f'\n{tab}', dag)

        with open(directory + '/formatted/' + dag_path, 'w', encoding='utf-8') as file:
            file.write(dag) 
