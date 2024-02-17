import random
import os
import time
import logging
logger = logging.getLogger(__name__)

import pathlib
FILE_PATH = pathlib.Path(__file__).parent.resolve()

from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from hieuung.common_utils.exception import debug
from hieuung.common_utils.database import transaction

# Step 2: Initiating the default_args
default_args = {
        'owner' : 'hieuung',
        'start_date' : datetime(2022, 11, 12),
    }

class AddData(BaseOperator):
    def __init__(self, 
                 conn_id: str, 
                 database: str, 
                 action: str ,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.action = action 

    @staticmethod
    def _get_list_words(n_user: int) -> str:
        path = os.path.join(FILE_PATH, 'wordlist_10000.txt')
        logging.info(path)
        with open(path, 'r') as f:
            res = [i.strip() for i in f.readlines()]
        return random.sample(res, k = n_user)
    
    @staticmethod
    def _get_list_passwords(n_user: int) -> int:
        return [random.randint(1, 1000000) for _ in range(n_user)]
    
    def random_signed_up_users(self, n_user):
        with transaction(self.conn_id, database= self.database) as conn:
            logger.info(f"Session opened")
            cur = conn.cursor()

            created_time = round(time.time() * 1000)
            user_names = self._get_list_words(n_user)
            user_passwrod = self._get_list_passwords(n_user)
            sql_statement = ""

            for _name, _password in zip(user_names, user_passwrod):
                statement = """
                    INSERT INTO webapp.app_users (user_name, user_email, user_password, created_time, updated_time)
                    VALUES """
                values_part = """
                    ('{p_name}', '{p_email}', '{p_password}', {p_created_time}, {p_updated_time})
                    """.format(
                                    p_name= _name, 
                                    p_email= _name + '@mail.com',
                                    p_password= _password,
                                    p_created_time= created_time,
                                    p_updated_time= created_time
                                )
                statement += values_part + ';'
                sql_statement += statement
            
            logger.info(f"Statement: {sql_statement}")

            cur.execute(sql_statement)

            result = cur.rowcount
            logger.info(f"Data: {(result)}")
        return 1

    def random_signed_in_users(self, n_user):
        with transaction(self.conn_id, database= self.database) as conn:
            logger.info(f"Session opened")
            cur = conn.cursor()
            created_time = round(time.time() * 1000)

            cur.execute("SELECT user_id FROM webapp.app_users")
            data = cur.fetchall()

            logger.info(f"Length Data: {len(data)}")

            try:
                ids = ','.join([str(i[0]) for i in random.sample(data, k = n_user)])
            except:
                logger.info(f"N_users: {n_user - len(data)}")
                ids = ','.join([str(i[0]) for i in random.sample(data, k = n_user - len(data))])
                
            cur.execute("UPDATE webapp.app_users \
                                                SET latest_signed_in_time = {p_latest_signed_in_time}, updated_time = {p_latest_signed_in_time} \
                                                WHERE user_id in ({ids});".format(
                                                ids= ids, 
                                                p_latest_signed_in_time= created_time,
                                                )
                                            )
            result = cur.rowcount
            logger.info(f"Data: {(result)}")
        return 1

    @debug
    def execute(self, context):
        n_user = random.randint(1, 9)
        logger.info(f"N_users: {n_user}")
        if self.action == 'signin':
            self.random_signed_in_users(n_user= n_user)
        else:
            self.random_signed_up_users(n_user= n_user)
        return 1

@task.branch(task_id="branching")
def signup_or_signin():
    return random.choice(['add_signin', 'add_signup'])

# Step 3: Creating DAG Object
with DAG(dag_id='Add-data',
        default_args=default_args,
        schedule_interval= '*/10 * * * *', 
        catchup=False
    ):

    # Step 4: Creating task
    # Creating first task

    start = EmptyOperator(task_id = 'start')

    branching = signup_or_signin()

    add_data_signin = AddData(task_id="add_signin", conn_id= 'my_prod_database', database= 'bookstore', action= 'signin')
    add_data_signup = AddData(task_id="add_signup", conn_id= 'my_prod_database', database= 'bookstore', action= 'signup')

    # Creating second task 
    end = EmptyOperator(task_id = 'end')

    # Step 5: Setting up dependencies 
    start >> branching 
    branching >> add_data_signin >> end
    branching >> add_data_signup >> end