import datetime

# from airflow import DAG
# from airflow.operators.python import PythonOperator

default_args = {"owner": "lateef",
                "start_date": datetime.datetime(2024, 1, 1, 5, 30)}


def get_data():
    import requests
    import json

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]
    return res


def format_data(res):
    data = {}
    data["gender"] = res["gender"]
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    location = res["location"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']["name"]}, {location["city"]}, {
            location['state']}, {location['country']}, {location['postcode']}"
    )
    data['coordinates'] = f"{location['coordinates']
                             ['latitude'], location['coordinates']['longitude']}"

    return data


def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))


stream_data()

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
