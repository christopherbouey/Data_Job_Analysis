import psycopg2
from psycopg2 import extras
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from datetime import datetime

def insert_values(conn, df, table):

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return
    print("Dataframe inserted successfully")
    cursor.close()

def load_jobs():

  today_date = datetime.today().strftime('%Y_%m_%d')
  df = pd.read_csv(f'/opt/airflow/dags/data/LIJobs_working.csv')

  username = 'airflow'
  password = 'airflow'
  host = 'postgres'
  database = 'airflow'

  table_name = 'job_listings_test_pipe'

  connection = psycopg2.connect(
    user=username,
    password=password,
    host=host,
    database=database
  )

  cursor = connection.cursor()

  create_table_query = '''
    CREATE TABLE IF NOT EXISTS %s (
    CREATE_DATE DATE NOT NULL,
    ID BIGINT NOT NULL,
    POST_DATE DATE,
    COMPANY TEXT NOT NULL,
    TITLE TEXT NOT NULL,
    LOCATION TEXT,
    DESCRIPTION TEXT,
    LEVEL TEXT,
    TYPE TEXT,
    INDUSTRY TEXT,
    LINK TEXT,
    PRIMARY KEY (CREATE_DATE, ID)
    ) PARTITION BY LIST (CREATE_DATE);
  ''' % table_name

  create_partition_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name}_{today_date} PARTITION OF {table_name}
      FOR VALUES IN ('{today_date}');
  '''
  cursor.execute(create_table_query)
  cursor.execute(create_partition_query)
  connection.commit()

  insert_values(connection, df, table_name)

if __name__=='__main__':
  load_jobs()
