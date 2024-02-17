export AIRFLOW_HOME=~/airflow
export PYTHONPATH=/home/hieuung/learning/simple_data_pipeline/
export AIRFLOW_CONN_MY_PROD_DATABASE='postgres://hieuut:hieuut@localhost:5432/bookstore'
export AIRFLOW_CONN_MY_DATA_LAKE='postgres://hieuut:hieuut@localhost:5432/datalake'
airflow standalone