from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import csv
import pandas as pd
import pandasql as psql

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

def sqlite_read():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("airflow-data/data/Northwind_small.sqlite")

    # Creating the cursor
    cur = con.cursor()

    # Executing que SQL Query
    cur.execute('SELECT * FROM "Order";')

    with open("output_orders.csv", 'w', newline='') as csv_file: 
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cur.description]) 
        csv_writer.writerows(cur)

    print('CSV file created: output_orders.csv')
    con.close()

def query_result():
    # Create a SQL connection to our SQLite database
    con = sqlite3.connect("airflow-data/data/Northwind_small.sqlite")

    # Creating Pandas DataFrame
    df_orderDetails = pd.read_sql_query("SELECT * FROM OrderDetail", con)
    print('Printando o df_orderDetails')

    # Reading the CSV file
    df_Order = pd.read_csv("output_orders.csv")
    print('Printando o df_Order')

    # Creating a query to join the DataFrames
    query = """
    SELECT 
        SUM(Quantity) 
    FROM 
        df_orderDetails
    JOIN 
        df_Order 
    ON 
        df_Order.Id = df_orderDetails.OrderId
    WHERE 
        ShipCity = 'Rio de Janeiro'
    GROUP BY 
        ShipCity;
    """

    # Saving the result in a Pandas DataFrame
    df_result = psql.sqldf(query)

    # Converting Pandas DataFrame to txt
    df_result.to_csv('count.txt', header=None, index=None, mode='w')

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    sqlite_to_CSV = PythonOperator(
        task_id='sqlite_to_CSV',
        python_callable=sqlite_read,
        provide_context=True
    )

    query_result_txt = PythonOperator(
    task_id='query_result_txt',
    python_callable=query_result,
    provide_context=True
    )
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

sqlite_to_CSV >> query_result_txt >> export_final_output