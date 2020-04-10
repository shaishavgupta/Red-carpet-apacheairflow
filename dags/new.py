from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

dag_name = "the_dag"
pg_hook = PostgresHook(postgres_conn_id="postgres_data")
def load_data(**kwargs):
	global pg_hook
	global dag_name

	data = pd.read_excel("sample.xlsx")
	data.to_csv("sample.csv")

	pg_command = """select * from sample;"""
	city = []
	location = []
	latitude = []
	longitude = []
	pincode = []
	for i in pg_hook.get_records(pg_command):
		city.append(i[0])
		location.append(i[1])
		latitude.append(i[2])
		longitude.append(i[3])
		pincode.append(i[4])
	# A function for updating the updated_at column at each UPDATE
	old_table = pd.DataFrame(columns=["city","location","latitude","longitude","pincode"])
	old_table["city"] = city
	old_table["location"] = location
	old_table["latitude"] = latitude
	old_table["longitude"] = longitude
	old_table["pincode"] = pincode
	kwargs['old_list'].xcom_push(key='old_table', value=old_table) 


def setup_db(**kwargs):
	global pg_hook
	global dag_name	

	old_table = kwargs['ti'].xcom_pull(key = 'old_table')
	pg_command = """drop table sample;"""
	pg_hook.run(pg_command)
	
	pg_command = """copy sample from 'C:\Users\nidhi\Desktop\airflow_home\ubuntu_venv\pwd\dags\sample.csv' ;"""
	pg_hook.run(pg_command)
	# A trigger for calling the above function
	pg_command = """select * from sample"""
	city = []
	location = []
	latitude = []
	longitude = []
	pincode = []
	for i in pg_hook.get_records(pg_command):
		city.append(i[0])
		location.append(i[1])
		latitude.append(i[2])
		longitude.append(i[3])
		pincode.append(i[4])
	# A function for updating the updated_at column at each UPDATE
	new_table = pd.DataFrame(columns=["city","location","latitude","longitude","pincode"])
	new_table["city"] = city
	new_table["location"] = location
	new_table["latitude"] = latitude
	new_table["longitude"] = longitude
	new_table["pincode"] = pincode

	kwargs['new_list'].xcom_push(key='new_table', value=new_table)

	

def calc(**kwargs):
	old_table = kwargs['old_list'].xcom_pull(key='old_table')
	new_table = kwargs['new_list'].xcom_pull(key='new_table')

	tmp = []
	new_df = pd.DataFrame()

	np.where(old_table['pincode'] == new_table['pincode'], 0,tmp.append(new_table["pincode"]))  #create new column in df1 to check if prices match

	for i in range(len(tmp)):
		pg_command = """select * from sample where pincode = {{tmp[i]}};"""
		
		for j in pg_hook.get_records(pg_command):
			new_df.loc[i] = [j[0],j[1],j[2],j[3]]
	
	tmp.to_excel('output.xlsx')

# Setup the DAG

default_args = {
    'owner': 'shaishav',
    'depends_on_past': False,
    'email': ['gshaishavgupta@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(dag_name,
		  description="DAG_struct",
		  schedule_interval='@daily',
		  start_date=datetime.now(),
		  default_args=default_args)

# Setup the operators of the DAG
with dag:
	calculating = PythonOperator(
		task_id="task3",
		python_callable=calc,
		provide_context=True,
		dag=dag)
	generating_new_table = PythonOperator(
		task_id="task2",
		python_callable=setup_db,
		provide_context=True,
		dag=dag)
	loading_old_table = PythonOperator(
		task_id="task1",
		python_callable=load_data,
		provide_context=True,
		dag=dag)


	# Setup the flow of operators in the DAG
	loading_old_table >> generating_new_table >> calculating