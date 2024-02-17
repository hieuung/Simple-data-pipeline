FROM apache/airflow

ENV AIRFLOW_HOME '/opt/airflow'

COPY . /${AIRFLOW_HOME}/dags/hieuung

WORKDIR /${AIRFLOW_HOME}/dags/hieuung

RUN pip install --upgrade pip && \ 
    pip install -r requirements.txt