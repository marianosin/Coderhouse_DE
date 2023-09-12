FROM apache/airflow:2.7.0

COPY requirements.txt /opt/airflow
RUN pip install -r /opt/airflow/requirements.txt


COPY --chown=airflow:root dag_after.py /opt/airflow/dags

RUN airflow db init


ENTRYPOINT airflow scheduler & airflow webserver