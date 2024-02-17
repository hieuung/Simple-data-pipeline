import logging
logger = logging.getLogger(__name__)

from typing import Sequence

from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator, get_current_context

def get_config_param(**kwargs):
    context = get_current_context()
    logging.info(context)

default_args = {
        'owner' : 'hieuung',
        'start_date' : datetime(2022, 11, 12),
}

class MyConfigOperator(BaseOperator):
    template_fields: Sequence[str] = ("configuration",)
    template_fields_renderers = {
        "configuration": "json",
    }

    def __init__(self, configuration: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration

    def execute(self, context):
        logging.info(self.configuration)
        message = f"Hello {self.configuration['world']} it's {self.configuration['name']}!"
        print(message)
        return message

# Creating DAG Object
with DAG(dag_id= 'base_task',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False,
        params={"my_int_param": Param(5, type="integer", minimum=3)},
    ):

    # Creating task
    start = EmptyOperator(task_id = 'start')

    # config_task = MyConfigOperator(
    #     task_id= "config-task",
    #     configuration= {"world": "Airflow", "name": "Hieu"},
    # )

    config_params= PythonOperator(task_id= 'config_param', python_callable= get_config_param)


    # Creating second task 
    end = EmptyOperator(task_id = 'end')

    # Step 5: Setting up dependencies 
    start >> config_params >> end
