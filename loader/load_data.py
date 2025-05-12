import requests
from clickhouse_connect import get_client
from types import SimpleNamespace
import logging
import datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryCallState
from utils import fetch

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

logging.info("=" * 60)
logging.info(f"Запуск скрипта заполнения таблиц: {datetime.datetime.now()}")
logging.info("=" * 60)

client = get_client(host="clickhouse", port=8123, username="", password="", database="")
row_counter = 0

def is_already_loaded(publication_id: int, dataset_id: int, measure_id: int) -> bool:
    try:
        result = client.query(f"""
            SELECT count() FROM PROJECT.load_progress
            WHERE publication_id = {publication_id}
              AND dataset_id = {dataset_id}
              AND measure_id = {measure_id}
        """)
        return result.result_rows[0][0] > 0
    except Exception as e:
        logging.error(f"Ошибка при проверке прогресса: {e}")
        return False

def mark_as_loaded(publication_id: int, dataset_id: int, measure_id: int):
    try:
        client.command(f"""
            INSERT INTO PROJECT.load_progress (
                publication_id, dataset_id, measure_id
            ) VALUES (
                {publication_id}, {dataset_id}, {measure_id}
            )
        """)
    except Exception as e:
        logging.error(f"Ошибка при отметке загрузки: {e}")

def get_data_batch(publication_id, dataset_id, measure_id):
    global row_counter

    try:
        yearsParams = {'measureId': measure_id, 'datasetId': dataset_id}
        years = fetch("/years", params=yearsParams)[0]

        dataParams = {
            'y1': years.FromYear,
            'y2': years.ToYear,
            'publicationId': publication_id,
            'datasetId': dataset_id,
            'measureId': measure_id
        }
        return fetch("/data", params=dataParams).RawData
    except Exception as e:
        logging.error(f"Ошибка получения данных для pub={publication_id}, ds={dataset_id}, ms={measure_id}: {e}")
        return []
     
def insert_batch(insert_query_part, rows):
    global row_counter
    try:
        rows_str = ",\n".join(rows)
        insert_query = insert_query_part + " " + rows_str + ";"
        client.command(insert_query)
        logging.info(f"Вставлено {row_counter} строк.")
    except Exception as e:
        logging.error(f"Ошибка при вставке batch: {e}")
        raise  

def sql_value(val):
    if val is None:
        return 'NULL'
    if isinstance(val, str):
        return f"'{val}'"
    return val

def fill_publications():
    global row_counter

    insert_query_part = f"""
            INSERT INTO PROJECT.publications (
                id,
                parent_id,
                category_name,
                no_active
            ) VALUES
        """

    rows = []
    for publication in fetch("/publications"):
        publication_str = f"""(
            {publication.id},
            {sql_value(publication.parent_id)},
            {sql_value(publication.category_name)},
            {sql_value(publication.NoActive)}
        )"""
        rows.append(publication_str)
        row_counter += 1

        if row_counter % 500 == 0:
            try:
                insert_batch(insert_query_part, rows)
                rows = []
            except Exception as e:
                logging.error(f"Ошибка при вставке в таблицу publications")
                raise  

    if rows:
        try:
            insert_batch(insert_query_part, rows)
        except Exception as e:
            logging.error(f"Ошибка при вставке в таблицу publications")
            raise  
    logging.info("Таблица publications заполнена.")

def fill_datasets():
    global row_counter

    insert_query_part = f"""
        INSERT INTO PROJECT.datasets (
            id,
            publication_id,
            name,
            type,
            reporting,
            full_name,
            link,
            updated_time
        ) VALUES
    """

    rows = []
    for publication in fetch("/publications"):
        if publication.NoActive == 1: continue
        datasets = fetch(f"/datasets?publicationId={publication.id}")

        for dataset in datasets:
            dataset_str = f"""(
                {dataset.id},
                {publication.id},
                {sql_value(dataset.name)},
                {sql_value(dataset.type)},
                {sql_value(dataset.reporting)},
                {sql_value(dataset.full_name)},
                {sql_value(dataset.link)},
                toDate('{dataset.updated_time[:10]}')
            )"""
            rows.append(dataset_str)
            row_counter += 1
            
            if row_counter % 500 == 0:
                try:
                    insert_batch(insert_query_part, rows)
                    rows = []
                except Exception as e:
                    logging.error(f"Ошибка при вставке в таблицу datasets")
                    raise  

    if rows:
        try:
            insert_batch(insert_query_part, rows)
        except Exception as e:
            logging.error(f"Ошибка при вставке в таблицу datasets")
            raise      
    logging.info("Таблица datasets заполнена.")


def fill_measures():
    global row_counter

    insert_query_part = f"""
        INSERT INTO PROJECT.measures (
            id,
            dataset_id,
            name,
            parent_id,
            sort
        ) VALUES
    """

    rows = []
    for publication in fetch("/publications"):

        if publication.NoActive == 1: continue
        datasets = fetch(f"/datasets?publicationId={publication.id}")

        for dataset in datasets:

            if dataset.type == 1:
                measures = fetch(f"/measures?datasetId={dataset.id}").measure 

                for measure in measures:
                    measure_str = f"""(
                        {measure.id},
                        {dataset.id},
                        {sql_value(measure.name)},
                        {sql_value(measure.parent_id)},
                        {sql_value(measure.sort)}
                    )"""
                    rows.append(measure_str)
                    row_counter += 1
                    
                    if row_counter % 2000 == 0:
                        try:
                            insert_batch(insert_query_part, rows)
                            rows = []
                        except Exception as e:
                            logging.error(f"Ошибка при вставке в таблицу measures")
                            raise  

    if rows:
        try:
            insert_batch(insert_query_part, rows)
        except Exception as e:
            logging.error(f"Ошибка при вставке в таблицу measures")
            raise  
    logging.info("Таблица measures заполнена.")


def fill_year_ranges():
    global row_counter

    insert_query_part = f"""
        INSERT INTO PROJECT.year_ranges (
            dataset_id,
            measure_id,
            from_year,
            to_year
        ) VALUES
    """
    rows = []
    for publication in fetch("/publications"):

        if publication.NoActive == 1: continue
        datasets = fetch(f"/datasets?publicationId={publication.id}")

        for dataset in datasets:

            if dataset.type == 1:
                measures = fetch(f"/measures?datasetId={dataset.id}").measure 

                for measure in measures:
                    yearsParams = {'measureId': measure.id, 'datasetId': dataset.id}
                    years = fetch("/years", params=yearsParams)[0]
                    year_ranges_str = f"""(
                        {dataset.id},
                        {measure.id},
                        {sql_value(years.FromYear)},
                        {sql_value(years.ToYear)}
                    )"""
                    rows.append(year_ranges_str)
                    row_counter += 1

                    if row_counter % 500 == 0:
                        try:
                            insert_batch(insert_query_part, rows)
                            rows = []
                        except Exception as e:
                            logging.error(f"Ошибка при вставке в таблицу year_ranges")
                            raise  

            else:
                yearsParams = {'measureId': -1, 'datasetId': dataset.id}
                years = fetch("/years", params=yearsParams)[0]
                year_ranges_str = f"""(
                    {dataset.id},
                    {measure.id},
                    {sql_value(years.FromYear)},
                    {sql_value(years.ToYear)}
                )"""
                rows.append(year_ranges_str)
                row_counter += 1

                if row_counter % 500 == 0:
                    try:
                        insert_batch(insert_query_part, rows)
                        rows = []
                    except Exception as e:
                        logging.error(f"Ошибка при вставке в таблицу year_ranges")
                        raise  

    if rows:
        try:
            insert_batch(insert_query_part, rows)
        except Exception as e:
            logging.error(f"Ошибка при вставке в таблицу year_ranges")
            raise  
    logging.info("Таблица year_ranges заполнена.")



def fill_units():
    global row_counter

    insert_query_part = f"""
        INSERT INTO PROJECT.units (
            id,
            val
        ) VALUES
    """

    rows = []
    units = {1: '% годовых', 2: '%', 3: 'млрд руб.', 4: 'млн долларов США', 5: 'месяцев', 6: 'млн руб.', 7: 'единиц'}

    for key, value in units.items():
        units_str = f"""(
            {key},
            {sql_value(value)}
        )"""
        rows.append(units_str)
        row_counter += 1

    if rows:
        try:
            insert_batch(insert_query_part, rows)
        except Exception as e:
            logging.error(f"Ошибка при вставке в таблицу units")
            raise  
    logging.info("Таблица units заполнена.")

def fill_data():
    global row_counter
    insert_query_part = f"""
        INSERT INTO PROJECT.data (
            dataset_id, 
            measure_id, 
            unit_id, 
            obs_val, 
            row_id, 
            dt, 
            periodicity, 
            col_id, 
            date, 
            digits
        ) VALUES
    """
    rows = []
    for publication in fetch("/publications"):
        if publication.NoActive == 1: continue
        datasets = fetch(f"/datasets?publicationId={publication.id}")

        for dataset in datasets:
            if dataset.type == 1:
                measures = fetch(f"/measures?datasetId={dataset.id}").measure 

                for measure in measures:
                    if is_already_loaded(publication.id, dataset.id, measure.id):
                        continue
                    data = get_data_batch(publication.id, dataset.id, measure.id)
                    for row in data:
                        row_str = f"""(
                            {dataset.id},
                            {measure.id},
                            {sql_value(row.unit_id)},
                            {sql_value(row.obs_val)},
                            {sql_value(row.rowId)},
                            {sql_value(row.dt)},
                            {sql_value(row.periodicity)},
                            {sql_value(row.colId)},
                            toDate('{row.date[:10]}'),
                            {sql_value(row.digits)}
                        )"""
                        rows.append(row_str)
                        row_counter += 1

                        if row_counter % 5000 == 0: 
                            try:
                                insert_batch(insert_query_part, rows)
                                rows = []
                            except Exception as e:
                                logging.error(f"""Ошибка при обработке публикации {publication.category_name}, 
                                        показатель{dataset.full_name}, 
                                        measure={measure.id}: {e}""")
                    mark_as_loaded(publication.id, dataset.id, measure.id)
            else:
                if is_already_loaded(publication.id, dataset.id, -1):
                        continue
                data = get_data_batch(publication.id, dataset.id, -1)
                for row in data:
                    row_str = f"""(
                        {dataset.id},
                        {-1},
                        {sql_value(row.unit_id)},
                        {sql_value(row.obs_val)},
                        {sql_value(row.rowId)},
                        {sql_value(row.dt)},
                        {sql_value(row.periodicity)},
                        {sql_value(row.colId)},
                        toDate('{row.date[:10]}'),
                        {sql_value(row.digits)}
                    )"""
                    rows.append(row_str)
                    row_counter += 1
                    
                    if row_counter % 5000 == 0: 
                        try:
                            insert_batch(insert_query_part, rows)
                            rows = []
                        except Exception as e:
                            logging.error(f"Ошибка при обработке публикации {publication.category_name}, показатель{dataset.full_name}, measure={measure.id}: {e}")
                mark_as_loaded(publication.id, dataset.id, -1)

        logging.info(f"Публикация '{publication.category_name}' обработана...")    
    if rows:
        insert_batch(insert_query_part, rows)
    logging.info("Таблица data заполнена.")

def fill_tables():
    fill_publications()
    fill_datasets()
    fill_measures()
    fill_units()
    fill_year_ranges() 
    fill_data()
    
fill_tables()
logging.info("=" * 60)
logging.info(f"Cкрипт заполнения таблиц исполнен: {datetime.datetime.now()}")
logging.info("=" * 60)
