import datetime

# from airflow import DAG
# from airflow.operators.python import PythonOperator

default_args = {"owner": "lateef",
                "start_date": datetime.datetime(2024, 1, 1, 5, 30)}


def stream_data():
    import requests
    import json

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]
    print(json.dumps(res, indent=3))


# with DAG(
#     "user_automation",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False,
# ) as dag:
#
#     streaming_task = PythonOperator(
#         task_id="stream_data_from_api", python_callable=stream_data
#     )
stream_data()
