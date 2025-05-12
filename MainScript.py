import requests
import pandas as pd
from clickhouse_connect import get_client
from types import SimpleNamespace
import json
from utils import fetch

SUPERSET_URL = 'http://localhost:8088/api/v1'
SUPERSET_USER = 'admin'
SUPERSET_PASSWORD = 'admin'
CLICKHOUSE_HOST = 'localhost'

client = get_client(host=CLICKHOUSE_HOST, port=8123)

def get_superset_tokens():
    login_url = f"{SUPERSET_URL}/security/login"
    payload = {"username": SUPERSET_USER, "password": SUPERSET_PASSWORD, "provider": "db"}
    session = requests.Session()
    response = session.post(login_url, json=payload)
    access_token = response.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    csrf_url = f"{SUPERSET_URL}/security/csrf_token/"
    csrf_response = session.get(csrf_url)
    csrf_token = csrf_response.json()["result"]
    return access_token, csrf_token, session

def create_clickhouse_connection(session, csrf_token):
    url = f"{SUPERSET_URL}/database/"
    headers = {
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }

    payload = {
        "database_name": "PROJECT",
        "sqlalchemy_uri": f"clickhousedb+connect://default:@clickhouse:8123/PROJECT",
        "extra": json.dumps({
            "engine_params": {},
            "metadata_params": {},
            "schemas_allowed_for_csv_upload": [],
        }),
        "expose_in_sqllab": True,
        "allow_run_async": True,
    }

    response = session.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print("ClickHouse подключён к Superset.")
    elif response.status_code == 422:
        print("Подключение к базе данных уже существует.")
    else:
        print("Ошибка подключения:", response.text)

def create_dataset(session, csrf_token, database_name, schema_name, table_name):
    existing_id = get_datasource_id(table_name, schema_name, session)
    if existing_id:
        return existing_id

    db_response = session.get(f"{SUPERSET_URL}/database/")
    dbs = db_response.json()["result"]
    db_id = next((db["id"] for db in dbs if db["database_name"] == database_name), None)
    if db_id is None:
        raise Exception(f"База данных '{database_name}' не найдена в Superset")

    headers = {
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }
    payload = {
        "database": db_id,
        "schema": schema_name,
        "table_name": table_name
    }
    resp = session.post(f"{SUPERSET_URL}/dataset/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()["id"]

def get_datasource_id(table_name, schema, session):
    url = f"{SUPERSET_URL}/dataset/"
    response = session.get(url)
    results = response.json().get('result', [])
    for ds in results:
        if ds['table_name'] == table_name and ds['schema'] == schema:
            return ds['id']

def select_publication():
    publications = fetch("/publications")
    print("Доступные публикации:")
    for publication in publications:
        print(f"id:{publication.id} title:{publication.category_name} {'- NoActive' if publication.NoActive else ''}")
    while True:
        publication_id = int(input("Введите id публикации: "))
        selected_publication = next((e for e in publications if e.id == publication_id and not e.NoActive), None)
        if selected_publication:
            return selected_publication
        print("Неверный id или раздел NoActive.")

def select_dataset(publication_id):
    datasets = fetch(f"/datasets?publicationId={publication_id}")
    print("Доступные показатели:")
    for dataset in datasets:
        print(f"id:{dataset.id} title:{dataset.name}")
    while True:
        selected_dataset_id = int(input("Введите id показателя: "))
        selected_dataset = next((e for e in datasets if e.id == selected_dataset_id), None)
        if selected_dataset:
            return selected_dataset
        print("Неверный id показателя.")

def select_measure(dataset_id, dataset_type):
    if dataset_type != 1:
        print("У этого показателя нет разрезов.")
        return SimpleNamespace(id=-1, name="Без разреза") 
    measures = fetch(f"/measures?datasetId={dataset_id}").measure 
    print("Доступные разрезы:")
    for measure in measures:
        print(f"id:{measure.id} title:{measure.name}")
    while True:
        select_measure_id = int(input("Введите id разреза: "))
        selected_measure = next((e for e in measures if e.id == select_measure_id), None)
        if selected_measure:
            return selected_measure
        print("Неверный id разреза.")

def select_years(dataset_id, measure_id):
    yearsParams = {'measureId': measure_id, 'datasetId': dataset_id}
    years = fetch("/years", params=yearsParams)[0]
    print(f"Доступные года: с {years.FromYear} по {years.ToYear}")
    while True:
        from_year = int(input("Введите год начала: "))
        to_year = int(input("Введите год окончания: "))
        if years.FromYear <= from_year <= to_year <= years.ToYear:
            return from_year, to_year
        print(f"Ошибка, введите год начала и окончания повторно. Данные доступны с {years.FromYear} по {years.ToYear} год.")

def select_chart_type():
    print("Доступные типы графиков:")
    print("1 — линейный график")
    print("2 — столбчатая диаграмма")
    print("3 — площадная диаграмма")
    while True:
        choice = input("Введите номер графика: ")
        if choice == "1":
            return "echarts_timeseries_line"
        elif choice == "2":
            return "echarts_timeseries_bar"
        elif choice == "3":
            return "echarts_area"
        else:
            print("Неверный выбор, попробуйте снова.")

def select_payload_type(dataset):

    print("Выберите сценарий расчёта:")
    print("1 — Среднее значение по месяцам")
    print("2 — Минимум и максимум по месяцам")
    while True:
        choice = input("Введите номер сценария: ")
        if choice == "1":
            return [
                {"expressionType": "SQL", "sqlExpression": "AVG(obs_val)", "label": f"Среднемесячное изменение для значения показателя {dataset.name}", "optionName": "metric_1"}
            ]
        elif choice == "2":
            return [
                {"expressionType": "SQL", "sqlExpression": "MAX(obs_val)", "label": f"Помесячное изменение для максимума значения показателя {dataset.name}", "optionName": "metric_1"},
                {"expressionType": "SQL", "sqlExpression": "MIN(obs_val)", "label": f"Помесячное изменение для минимума значения показателя {dataset.name}", "optionName": "metric_2"}
            ]
        else:
            print("Неверный выбор, попробуйте снова.")

def run(publication, dataset, measure, from_year, to_year, viz_type, metrics):
    _, csrf_token, session = get_superset_tokens()
    create_clickhouse_connection(session, csrf_token)  
    datasource_id = create_dataset(session, csrf_token, "PROJECT", "PROJECT", "data")
    headers = {
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }
    payload = {
        "slice_name": f"{viz_type}_{publication.category_name}_{dataset.full_name}_{measure.name if measure.id != -1 else '—'}",
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "params": json.dumps({
            "metrics": metrics,
            "adhoc_filters": [
                {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "dataset_id", "operator": "==", "comparator": dataset.id},
                {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "measure_id", "operator": "==", "comparator": measure.id},
                {"clause": "WHERE", "expressionType": "SIMPLE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": f"{from_year}-01-01 : {to_year}-12-31"}
            ],
            "time_grain_sqla": "P1M",
            "time_range": f"{from_year}-01-01 : {to_year}-12-31",
            "x_axis": "date"
        })
    }
    response = session.post(f"{SUPERSET_URL}/chart/", json=payload, headers=headers)
    chart_id = response.json()["id"]
    chart_url = f"{SUPERSET_URL.replace('/api/v1', '')}/explore/?slice_id={chart_id}"
    print(f"Chart создан. Ссылка: {chart_url}")
    
if __name__ == '__main__':
    publication = select_publication()
    dataset = select_dataset(publication.id)
    measure = select_measure(dataset.id, dataset.type)
    fromYear, toYear = select_years(dataset.id, measure.id)
    vizType = select_chart_type()
    metrics = select_payload_type(dataset)
    run(publication, dataset, measure, fromYear, toYear, vizType, metrics)






