import os
import time
import logging
logger = logging.getLogger(__name__)

import pathlib
FILE_PATH = pathlib.Path(__file__).parent.resolve()
PREVIOUS_FILE_PATH = pathlib.Path(__file__).parent.parent.resolve()

from typing import Sequence
from airflow import DAG
from airflow.models import DagRun
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.models.baseoperator import BaseOperator
from hieuung.common_utils.exception import debug
from airflow.providers.postgres.operators.postgres import PostgresOperator
from hieuung.common_utils.database import transaction

default_args = {
        'owner':'hieuung',
        'schedule_interval':'@daily', 
        'start_date' : datetime(2024, 2, 1),
    }

def get_most_recent_dag_run(dag_id) -> datetime:
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None

class ET(BaseOperator):
    template_fields: Sequence[str] = ("conn_id", "full_load")
    
    def __init__(self,
                 conn_id:str,
                 full_load:bool, 
                 **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.full_load = full_load
    
    @staticmethod
    def load_last_run():
        try:
            with open(os.path.join(PREVIOUS_FILE_PATH, 'temp', 'last_created_time_etl_user_signed_up.txt'), 'r') as f:
                return int(f.readline().strip())
        except:
            raise Exception('File not exists')

    @staticmethod
    @debug
    def save_last_run(last_run):
        with open(os.path.join(PREVIOUS_FILE_PATH, 'temp', 'last_created_time_etl_user_signed_up.txt'), 'w') as f:
            f.write(str(last_run))
            
    @debug
    def execute(self, context):
        with transaction(self.conn_id, database= None) as conn:
            logger.info(f"Session opened")
            cur = conn.cursor()
            created_time = round(time.time() * 1000)

            last_date_milisec = self.load_last_run()

            if self.full_load:
                last_date = '1970-01-01'
            else:
                last_date = datetime.fromtimestamp(last_date_milisec/1000.0).strftime('%Y-%m-%d')

            today = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"last_date: {last_date}, today: {today}")

            cur.execute(f"""SELECT 
                                user_id, 
                                extract(day from to_timestamp((created_time + 25200000) / 1000.0)) "date",
                                extract(month from to_timestamp((created_time + 25200000) / 1000.0)) "month",
                                extract(year from to_timestamp((created_time + 25200000) / 1000.0)) "year",
                                created_time
                            FROM 
                                webapp.app_users 
                            WHERE 
                                TO_CHAR(
                                    to_timestamp((created_time + 25200000) / 1000.0), 
                                    'YYYY-MM-DD'
                                ) BETWEEN '{last_date}' AND '{today}'""")
                
            data = cur.fetchall()

            logger.info(f"Length Data: {len(data)}")
            
            max_created_time = last_date_milisec

            sql_statement = ""
            for user_id, date, month, year, created_time in data:
                max_created_time = created_time if created_time > max_created_time else max_created_time
                statement = """
                    INSERT INTO datalake.user_signed_up_at (user_id, date, month, year, created_time)
                    VALUES """
                values_part = """
                    ('{user_id}', {date}, {month}, {year}, {created_time})
                    """.format(
                                    user_id= user_id, 
                                    date= date,
                                    month= month,
                                    year= year,
                                    created_time= created_time
                                )
                statement += values_part + ';'
                sql_statement += statement
            
            self.save_last_run(max_created_time)

            with open(os.path.join(PREVIOUS_FILE_PATH, 'sql', 'user_history' ,'temp_load.sql'), 'w') as f:
                f.write(sql_statement)

            result = cur.rowcount
            logger.info(f"Data: {(result)}")
        return 1

# Creating DAG Object
with DAG(dag_id= 'etl_user_signed_up_history',
        default_args=default_args,
        catchup=False,
        template_searchpath= [PREVIOUS_FILE_PATH],
        params={
            "full_load" : Param(False, type="boolean"),
            "conn_id_source": Param('my_prod_database', type="string"),
            "conn_id_sink": Param('my_data_lake', type="string"),
            },
    ):

    # Creating task
    start = EmptyOperator(task_id = 'start')

    create_table_sink = PostgresOperator(
    task_id= "created_table",
    postgres_conn_id= "{{ params.conn_id_sink }}",
    sql= os.path.join("sql", "user_history", "user_history.sql"),
    )

    extract_transform = ET(task_id= "extract_transform"
                            ,conn_id= "{{ params.conn_id_source }}", 
                           full_load="{{ params.full_load }}")


    load = PostgresOperator(
    task_id= "load_to_sink",
    postgres_conn_id= "{{ params.conn_id_sink }}",
    sql= os.path.join("sql", "user_history", "temp_load.sql"),
    )

    # Creating second task 
    end = EmptyOperator(task_id = 'end')

    # Step 5: Setting up dependencies 
    start >> create_table_sink >> extract_transform >> load >> end
