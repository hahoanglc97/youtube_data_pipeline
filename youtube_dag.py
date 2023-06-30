from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pull_youtube_data import pull_data
from run_query_in_Athena import run_query


# pip install apache-airflow
# pip install pandas
# pip install boto3
# pip install --upgrade google-api-python-client
# pip install --upgrade google-auth-oauthlib google-auth-httplib2


# create dag
with DAG(
    dag_id="pulling_youtube_data",
    schedule="@daily",
    start_date=datetime(year=2023, month=7, day=30),
    catchup=False,
    tags=["youtube"],
) as dag:
    # pull Vietnam video data
    pull_data_VN = PythonOperator(
        task_id="pull_video_data_VN",
        python_callable=pull_data,
        op_kwargs={"region_code": "VN"},
    )

    # pull Philippines video data
    pull_data_PH = PythonOperator(
        task_id="pull_video_data_PH",
        python_callable=pull_data,
        op_kwargs={"region_code": "PH"},
    )

    # pull Singapore video data
    pull_data_SG = PythonOperator(
        task_id="pull_video_data_SG",
        python_callable=pull_data,
        op_kwargs={"region_code": "SG"},
    )

    # pull Thailand video data
    pull_data_TH = PythonOperator(
        task_id="pull_video_data_TH",
        python_callable=pull_data,
        op_kwargs={"region_code": "TH"},
    )

    # pull Indonesia video data
    pull_data_ID = PythonOperator(
        task_id="pull_video_data_ID",
        python_callable=pull_data,
        op_kwargs={"region_code": "ID"},
    )

    # pull Malaysia video data
    pull_data_MY = PythonOperator(
        task_id="pull_video_data_MY",
        python_callable=pull_data,
        op_kwargs={"region_code": "MY"},
    )

    # run query on all collected data
    run_query = PythonOperator(task_id="run_sql_query", python_callable=run_query)

    # pull all data before running the query
    [
        pull_data_VN,
        pull_data_SG,
        pull_data_TH,
        pull_data_ID,
        pull_data_MY,
        pull_data_PH,
    ] >> run_query
