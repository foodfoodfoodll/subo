import pprint
import re

def formatter(flow):
    tab='    '
    flow = re.sub('\'', '`', flow)
    flow = re.sub('"','\'', flow)
    flow = re.sub('`', '"', flow)
    flow = re.sub('\n[\s]*"flows":\[[\s]*\{[\s]*"loadType"', '\n'+ tab*1 + '"flows": \[' + '\n{\n' + tab*3 + '"loadType"', flow)
    flow = re.sub('\n[\s]*\{[\s]*"loadType"', '\n'+ tab*2 +'{\n' + tab*3 + '"loadType"', flow)
    flow = re.sub('\n[\s]*"source":[\s]*\{', '\n' + tab*3 + '"source": {\n', flow)
    flow = re.sub('\n[\s]*"schema":', f'\n{tab*4}"schema":', flow)
    flow = re.sub('\n[\s]*"table":[\s]*', f'\n{tab*4}"table": ', flow)
    flow = re.sub('\n[\s]*"columnsWithJson"', f'\n{tab*4}"columnsWithJson"', flow)
    flow = re.sub('\n[\s]*"explodedColumns"', f'\n{tab*4}"explodedColumns"', flow)
    flow = re.sub('\n[\s]*"parsedColumns":[\s]*\[', f'\n{tab*4}"parsedColumns": [\n', flow)
    flow = re.sub('\n[\s]*\{"name"', '\n' + tab*5 + '{"name"', flow)
    flow = re.sub('\n[\s]*"columnCasts"', f'\n{tab*4}"columnCasts"', flow)
    flow = re.sub('}],', '}\n'+tab*4+'],', flow)
    flow = re.sub('\n[\s]*"preFilterCondition"', f'\n{tab*4}"preFilterCondition"', flow)
    flow = re.sub('\n[\s]*"postFilterCondition"', f'\n{tab*4}"postFilterCondition"', flow)
    flow = re.sub('\n[\s]*"incrementField":[\s]*"hdp_processed_dttm"[,|\s]*\},', f'\n{tab*4}"incrementField": "hdp_processed_dttm"\n{tab*3}'+'},', flow)
    flow = re.sub('\n[\s]*"target":[\s]*\{[\s]*"table"', f'\n{tab*3}"target": ' +'{\n' + tab*4 + '"table"', flow)
    flow = re.sub('[\s]*"aggregationField"', f'\n{tab*4}"aggregationField"', flow)
    flow = re.sub('[\s]*"partitionFields"', f'\n{tab*4}"partitionFields"', flow)
    flow = re.sub('[\s]*"customPartitioning"', f'\n{tab*4}"customPartitioning"', flow)
    flow = re.sub('[\s]*"updateAllowed":[\s]*True', f'\n{tab*4}"updateAllowed": True', flow)
    flow = re.sub('[\s]*\}\,[\s]*"addInfo":[\s]*\{[\s]*"orderField"', '\n'+tab*3+'},\n' + tab*3 + '"addInfo": {\n' + tab*4 + '"orderField"', flow)
    flow = re.sub('[\s]*\}[\s]*\}\,', '\n' +tab*3+ '}\n' + tab*2 + '},', flow)
    flow = re.sub('[\s]*\}[\s]*\}[\s]*\]', '\n' +tab*3+ '}\n' + tab*2 + '}\n' + tab*1 + ']', flow)
    return flow

def tmp_dag(docs,
            developer,
            database,
            id_is,
            topic,
            flows):
    res = f"""
#### Version 0.01
#### Last_modification_date: 25.02.2025
#### Docs: {docs}
#### Stream: Ozero dannykh
#### Team_id: LOADSUBO
#### Team: Загрузка данных СУБО
#### Dev: {developer}
#### Email: @vtb.ru
#### Sourcedb: 1642_19: prod_etl_subo
#### Targetdb: 1642_19: prod_repl_subo_{database}

import os
import sys

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

DAG_ID = "1642_19_datalake_subo_{database}_{id_is}_parse_load\"""" + """

DAG_OWNER = "1642_19"
ALERT_EMAIL_ADDRESSES = ""
PRIORITY_WEIGHT = 1  # Приоритет выполнения в рамках пула AirFlow
DAG_TAGS = ["1642_19_LOADSUBO"]
UNIT = "subo_parse"  # subo_load, subo_parse, etc
PATH_TO_DAG_SCRIPTS = os.environ["AIRFLOW_VAR_SCRIPTS"]

if os.environ["AIRFLOW_VAR_DEPLOYMENT"].lower() in ("p0", "prod"):
    STAGE_SUBFOLDER = "1642_19/1642_19_setl_dag_load_subo" if os.environ.get("TEST_BUILD") is None else ""
else:
    STAGE_SUBFOLDER = "1642_19/1642_19_setl_dag_load_subo_prelive" if os.environ.get("TEST_BUILD") is None else ""

CONFIG_SUBFOLDER = "configuration"

PATH_TO_DAG_CONF = os.path.join(PATH_TO_DAG_SCRIPTS, STAGE_SUBFOLDER, CONFIG_SUBFOLDER, f"{DAG_ID}.yaml")

sys.path = [os.path.join(PATH_TO_DAG_SCRIPTS, STAGE_SUBFOLDER)] + sys.path

from datalake_libs.config_loader import DagConfig, CommonConfig, get_connection_extra as get_connection, get_connection_password, form_json

common_config = CommonConfig(UNIT)
dag_config = DagConfig(PATH_TO_DAG_CONF)

POOL_NAME = common_config["dag_pool"]  # Пул AirFlow

core_path = common_config["spark_etl_jar"]
spark_jars = common_config["datalake_spark_jars"]
keytab_dir = common_config["datalake_keytab_dir"]
keytab_name = common_config["keytab"]
principal = common_config["principal"]

truststore_path = common_config["truststore_path"]
keystore_path = common_config["keystore_path"]
subo_keystore_con = os.path.basename(keystore_path)
subo_truststore_con = os.path.basename(truststore_path)
spark_conf = dag_config["spark_conf"]

keytab = os.path.join(keytab_dir, keytab_name)

etl_schema = common_config["etl_schema"]
target_db = dag_config["target_db"]

default_args = {
    "owner": DAG_OWNER,
    "email": ALERT_EMAIL_ADDRESSES,
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
}

connection_log = dag_config["connection_log"]
hive_connection_name = dag_config["connection_hive"]

connection = get_connection(hive_connection_name)
logs_table = get_connection(connection_log)

spark_conf.update(
    {
        "spark.connection.password": get_connection_password(hive_connection_name),
        "spark.logs.password": get_connection_password(connection_log)
    }
)

hive_common_info = {
    "targetSchema": target_db,
    "etlSchema": etl_schema,
    "logsTable": logs_table,
    "flagName": """ + f"""{id_is}""" + """
}

scd_build_json = {
    "connection": connection,
    "commonInfo": hive_common_info, 
    "flows":"""+ f"""{formatter(pprint.pformat(flows, indent=4, width=250, sort_dicts=False, compact=False))}""" + """
}

def spark_operator(task_id: str, name: str, json_load: dict):
    return SparkSubmitOperator(
        task_id=task_id,
        name=name,
        files=f\"{keystore_path},{truststore_path}",
        application_args=[form_json(json_load)],
        application=core_path,
        java_class="sparketl.Main",
        keytab=keytab,
        principal=principal,
        jars=spark_jars,
        conf=spark_conf,
        verbose=False,
        pool=POOL_NAME,
        priority_weight=PRIORITY_WEIGHT,
        dag=dag
    )


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=DAG_TAGS,
    schedule_interval="30 */12 * * *",
    catchup=False,
    max_active_runs=1
) as dag:
    scd_build = spark_operator("scd_build", f\"{DAG_ID}.scd_build", scd_build_json)
"""
    return res