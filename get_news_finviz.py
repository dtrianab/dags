from hashlib import new
import json, requests
from re import U
import os
import pandas as pd
from pandas.core.frame import DataFrame
from datetime import datetime, date, timedelta

from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

import time

from airflow import DAG
from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Diego T',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# define the DAG
dag = DAG(
    'get_finviz_news',
    default_args=default_args,
    description='Retrieve news from finviz_url on ticker list and store them in postgree',
    schedule_interval='@once',
)

def _get_news(**kwargs):
    news_tables = {}
    for ticker in kwargs['tickers']:
        # for extracting data from finviz
        finviz_url = 'https://finviz.com/quote.ashx?t='
        url = finviz_url + ticker
        req = Request(url=url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}) 
        response = urlopen(req)    
        # Read the contents of the file into 'html'
        html = BeautifulSoup(response)
        # Find 'news-table' in the Soup and load it into 'news_table'
        news_table = html.find(id='news-table')

        #store in dict as string (String For push to xcom {bs4/tag cant be sent})
        news_tables[ticker] = str(news_table)  
        time.sleep(0.5)  
    return news_tables

def _process(**kwargs):
    #news_table = BeautifulSoup(kwargs['ti'].xcom_pull(task_ids='retrieve_news', key='return_value'), "html.parser")
    news_tables = kwargs['ti'].xcom_pull(task_ids='retrieve_news', key='return_value')
    
    parsed_news = []

    for file_name, news_table in news_tables.items():        
        for x in BeautifulSoup(news_table, "html.parser").findAll('tr'):
            # read the text from each tr tag into text
            # get text from a only
            text = x.a.get_text()
            # split text in the td tag into a list
            date_scrape = x.td.text.split()
            # if the length of 'date_scrape' is 1, load 'time' as the only element

            if len(date_scrape) == 1:
                time_news = date_scrape[0]

                # else load 'date' as the 1st element and 'time' as the second
            else:
                date_news = date_scrape[0]
                time_news = date_scrape[1]
            #url
            url = x.find('a').attrs['href']

            ticker = file_name.split('_')[0]

            # Append ticker, date, time and headline as a list to the 'parsed_news' list
            parsed_news.append([ticker, date_news, time_news, text, url])
    #print(parsed_news)    
    #print('Processing')
    return parsed_news

def _verify(**kwargs):
    parsed_news = kwargs['ti'].xcom_pull(task_ids='proccess_response', key='return_value')
    columns = ['ticker','date_news', 'time_news', 'headline', 'url_news']
    return pd.DataFrame(parsed_news, columns=columns).to_json()    

def _transform_sql(**kwargs):
    news = pd.read_json( kwargs['ti'].xcom_pull(task_ids='verify_data', key='return_value') )
    # keep all new values, drop existing records from news
    print(news.head())
    print(news.tail())
    n=0
    k=str(n)
       
    col = ', '.join("'" + str(x).replace("'", '') + "'" for x in news[:0])
    col = col.replace("'","")

    values = ', '.join("'" + str(x).replace("'", '') + "'" for x in news.iloc[0])
    sql = "INSERT INTO %s (%s) VALUES (%s)" % ('raw_news.tmp_FinViz', col, values)
    print(col)
    print(values)
    print(sql)
    for i in range(1, news.shape[0]):
        values2 = ', '.join("'" + str(x).replace("'", '') + "'" for x in news.iloc[i])
        sql_i = ",(%s)" % (values2)
        sql+=sql_i
    sql+=';'
    print(sql)
    return sql


t1 = PythonOperator(
    task_id='retrieve_news',
    python_callable= _get_news,
    op_kwargs={'tickers': ['RCL', 'NIO', 'FROG', 'MANU', 'SPCE', 'NU', 'IQV']},
    dag=dag,
    )
t1

t2 = PythonOperator(
    task_id='proccess_response',
    python_callable= _process,
    dag=dag,
    )
t2

t3 = PythonOperator(
    task_id='verify_data',
    python_callable= _verify,
    dag=dag,
    )
t3

t4 = PythonOperator(
    task_id='transform_sql',
    python_callable= _transform_sql,
    dag=dag,
    )
t4

t5 = PostgresOperator(
    task_id="delete_tmp_table",
    postgres_conn_id='airflow_db',
    sql= "DELETE FROM raw_news.tmp_FinViz;",
    dag=dag,
    )
t5   

t6 = PostgresOperator(
    task_id="populate_news_tmp_table",
    postgres_conn_id='airflow_db',
    sql= '''{{ ti.xcom_pull(task_ids='transform_sql', key='return_value') }}'''
    ,
    dag=dag,
    )
t6  

t7 = PostgresOperator(
    task_id="delete_existing_news",
    postgres_conn_id='airflow_db',
    #sql= "DELETE FROM tmp_FinViz WHERE url_news IN (SELECT url_news FROM FinViz);"
    sql= "DELETE FROM raw_news.tmp_FinViz USING raw_news.FinViz WHERE raw_news.tmp_FinViz.url_news = raw_news.FinViz.url_news AND raw_news.tmp_FinViz.ticker = raw_news.FinViz.ticker;"
    ,
    dag=dag,
    )
t7  

t8 = PostgresOperator(
    task_id="populate_new_news",
    postgres_conn_id='airflow_db',
    sql= "INSERT INTO raw_news.FinViz (SELECT * FROM raw_news.tmp_FinViz);"
    ,
    dag=dag,
    )
t8  

t1 >> t2 >> t3 >> [t4, t5] >> t6 >> t7 >> t8