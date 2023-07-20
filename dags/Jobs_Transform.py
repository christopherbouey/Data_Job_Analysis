import pandas as pd
import numpy as np
import re

def salary_finder(df):
  "Function takes a predefined pandas dataframe and returns same object with appended column of average salary pulled from job description"
  # Regex identifier to pull salary values
  rg = "(?<=\$)(\d(?:,?\d)*(?:\.\d\d)?)"
  #lambda function to remove unrealistic values and return floating types
  l_func = lambda x: [float(num.replace(',','')) for num in x if float(num.replace(',','')) > 50000]

  # Pull salary text using regex
  df['Salary_Range'] = df.Description.apply(lambda x: set(re.findall(rg, x)))
  # Apply lambda function
  df['Salary_Range'] = df['Salary_Range'].apply(l_func)
  # Take avg salary from range(s)
  df['Salary_Avg'] = df['Salary_Range'].apply(lambda x: sum(x)/len(x) if len(x) > 0 else None)
  return df

def skill_picker(df):
  skills_list = {
    'Data_Architecture':['data architecture'],
    'ETL': ['ETL', 'Extract, Transport, Load'],
    'Big_Data': ['big data'],
    'Python': ['python'],
    'R': ['R'],
    'Java': ['java','javascript'],
    'SQL':['sql','nosql','mysql','postgresql'],
    'Hadoop':['hadoop'],
    'BigQuery': ['bigquery'],
    'Kafka': ['kafka'],
    'Spark': ['spark','pyspark'],
    'Hive': ['hive'],
    'AWS': ['aws','amazon web services'],
    'Lambda': ['lambda'],
    'Redshift': ['redshift'],
    'S3': ['S3'],
    'VPC': ['vpc'],
    'EC2': ['EC2'],
    'Kubernetes': ['kubernetes'],
    'GCP': ['gcp', 'google cloud platform'],
    'Tableau': ['tableau'],
    'Looker': ['looker']
  }

  for key, vals in skills_list.items():
    df[key] = df.Description.apply(lambda x: any(n.lower() in x.lower() for n in vals))
  return df

def job_transform():
  df = pd.read_csv('/opt/airflow/dags/data/LIJobs_working.csv')
  df = salary_finder(df)
  df = skill_picker(df)
  df.to_csv(f'/opt/airflow/dags/data/LIJobs_final.csv', index=False)


if __name__ == "__main__":
  job_transform()