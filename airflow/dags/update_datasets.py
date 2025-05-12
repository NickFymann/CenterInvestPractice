from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta
from clickhouse_connect import get_client
import requests
from types import SimpleNamespace
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryCallState

BASE_URL = 'http://www.cbr.ru/dataservice'

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def sql_value(val):
    if val is None:
        return 'NULL'
    if isinstance(val, str):
        return f"'{val}'"
    return val

def log_each_retry(retry_state: RetryCallState):
    attempt = retry_state.attempt_number
    if attempt > 1:
        endpoint = retry_state.args[0]
        params = retry_state.kwargs.get("params", {})
        print(f"[RETRY {attempt}/5] {endpoint} {params}")

@retry(
    stop=stop_after_attempt(5), 
    wait=wait_fixed(15),        
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before=log_each_retry
)
def fetch(endpoint, params=None):
    response = requests.get(f"{BASE_URL}{endpoint}", params=params)
    return response.json(object_hook=lambda d: SimpleNamespace(**d))

def put_data(client, publication_id, dataset_id, measure_id, from_date, to_date):
    log.info(f"Dataset {dataset_id}, Measure {measure_id}: Fetching data from {from_date.date()} to {to_date.date()}")

    dataParams = {
        'y1': from_date.year,
        'y2': to_date.year,
        'publicationId': publication_id,
        'datasetId': dataset_id,
        'measureId': measure_id
    }

    data = fetch("/data", params=dataParams).RawData
    log.info(f"Dataset {dataset_id}, Measure {measure_id}: Received {len(data)} rows")

    values = []
    for row in data:
        row_date = datetime.strptime(row.date[:10], '%Y-%m-%d')
        if from_date <= row_date < to_date:
            values.append(f"""(
                {dataset_id},
                {measure_id},
                {sql_value(row.unit_id)},
                {sql_value(row.obs_val)},
                {sql_value(row.rowId)},
                {sql_value(row.dt)},
                {sql_value(row.periodicity)},
                {sql_value(row.colId)},
                toDate('{row.date[:10]}'),
                {sql_value(row.digits)}
            )""")

    if values:
        log.info(f"Dataset {dataset_id}, Measure {measure_id}: Inserting {len(values)} rows")
        values_str = ",\n".join(values)
        insert_query = f"""
            INSERT INTO PROJECT.data (
                dataset_id, measure_id, unit_id, obs_val, row_id,
                dt, periodicity, col_id, date, digits
            ) VALUES {values_str};
        """
        client.command(insert_query)
    else:
        log.info(f"Dataset {dataset_id}, Measure {measure_id}: Nothing to insert")



def update_datasets():
    client = get_client(host='clickhouse', port=8123, username='', password='', database='')
    datasets = client.query("""
        SELECT id, publication_id, updated_time 
        FROM PROJECT.datasets
        WHERE updated_time <= subtractMonths(today(), 1)
    """).result_rows

    for dataset_id, publication_id, updated_time in datasets:
        from_date = updated_time
        to_date = updated_time + relativedelta(months=1)

        measures = client.query(f"SELECT id FROM PROJECT.measures WHERE dataset_id = {dataset_id}").result_rows
        if not measures:
            measures = [(-1,)]

        for (measure_id,) in measures:
            put_data(client, publication_id, dataset_id, measure_id, from_date, to_date)

        client.command(f"""
            ALTER TABLE PROJECT.datasets
            UPDATE updated_time = addMonths(updated_time, 1)
            WHERE id = {dataset_id}
        """)


with DAG(
    dag_id='update_datasets',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=["project"],

) as dag:
    
    update_task = PythonOperator(
        task_id='update_datasets_task',
        python_callable=update_datasets,
    )
