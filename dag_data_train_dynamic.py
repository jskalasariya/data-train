# We'll start by importing the DAG object
from datetime import timedelta
from airflow.models import DAG
# We need to import the operators used in our tasks
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.subdag_operator import SubDagOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago
import data_train_bucket_sensor as dts
import pathlib
import os
import json


def read_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
    else:
        print(f"{file_path} does not exists.")


def check_output_file_as_input(fileName, system_list):
    list_systems = list()
    for system in system_list:
        for inputFile in system["inputFiles"]:
            if inputFile["feedName"] == fileName:
                list_systems.append(system["systemName"])

    return list_systems


def get_input_files(dependent_system, system_list):
    for in_system in system_list:
        if in_system["systemName"] == dependent_system:
            return in_system["inputFiles"]


def get_output_files(dependent_system, system_list):
    for out_system in system_list:
        if out_system["systemName"] == dependent_system:
            return out_system["outputFiles"]

# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5),
    'email': ['data-train@hsbc.co.in'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

current_path = pathlib.Path(__file__).parent.absolute()
json_file_path = current_path.joinpath("data_train_dag_config.json")
json_data = read_json(json_file_path)
system_list = json_data["systems"]

dag_id = 'data-train'
dag = DAG(dag_id, default_args=default_args, schedule_interval='0 1 * * *', catchup=False)

operator_dict = dict()

with dag:
    for system in system_list:
        system_name = system["systemName"]
        parent_system_task_id = system_name
        parent_system_task = operator_dict.get(parent_system_task_id)
        if parent_system_task is None:
            parent_system_task = dts.data_train_bucket_sensor(task_id=parent_system_task_id,
                                                              bucket='data-train-' + system_name.lower(),
                                                              input_files=system["inputFiles"],
                                                              output_files=system["outputFiles"],
                                                              mode="reschedule",
                                                              poke_interval=20,
                                                              dag=dag)
            operator_dict.update({parent_system_task_id: parent_system_task})

        for outputFile in system["outputFiles"]:
            dependent_system_list = check_output_file_as_input(outputFile["feedName"], system_list)
            for dependent_system in dependent_system_list:
                dependent_system_task_id = dependent_system
                dependent_system_task = operator_dict.get(dependent_system_task_id)
                dependent_system_input_files = get_input_files(dependent_system, system_list)
                dependent_system_output_files = get_output_files(dependent_system, system_list)
                if dependent_system_task is None:
                    dependent_system_task = dts.data_train_bucket_sensor(task_id=dependent_system_task_id,
                                                                         bucket='data-train-' + dependent_system.lower(),
                                                                         input_files=dependent_system_input_files,
                                                                         output_files=dependent_system_output_files,
                                                                         mode="reschedule",
                                                                         poke_interval=20,
                                                                         dag=dag)
                    operator_dict.update({dependent_system_task_id: dependent_system_task})

                if dependent_system_task_id != parent_system_task_id:
                    parent_system_task >> dependent_system_task
