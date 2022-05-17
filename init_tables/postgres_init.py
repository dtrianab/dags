import os
import datetime
from airflow import DAG
from airflow import settings
from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Create connection using airflow env variable created by docker-compose
c = Connection(
    uri=os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"],
    conn_id="airflow_db"
)

# Add the connection to airflow settings
session = settings.Session() # get the session
print(session)
conn_name = session\
.query(Connection)\
.filter(Connection.conn_id == c.conn_id)\
.first()

if str(conn_name) != str(c.conn_id):
    session.add(c)  
    session.commit()


with DAG(
    # instantiating the Postgres Operator
    dag_id="postgres_init",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=True,
) as dag:
    # create_newsapi_table in pg DB
    create_newsapi_table = PostgresOperator(
        task_id="create_news_tables",
        postgres_conn_id="airflow_db",
        sql="news_tables.sql",
    )