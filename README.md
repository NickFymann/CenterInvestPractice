# CenterInvestPractice
# Superset Chart Generator for ClickHouse (CBR Data)

Это приложение при помощи API автоматически загружает статистические данные с сайта [ЦБ РФ](https://www.cbr.ru/), сохраняет их в базе данных ClickHouse и позволяет создавать 
графики в Apache Superset, используя один из нескольких автоматизированных CLI-сценариев, через его REST API. Актуальность данных обеспечивается при помощи Apache Airflow, который позволяет
в автоматическом режиме их обновлять.  

## Возможности

- Интерактивный выбор публикации, показателя и разреза
- Анализ данных по выбранному периоду
- Автоматическое создание датасета и графика по выбранному сценарию

## Установка

1. Убедитесь, что у вас установлен Docker
2. Скачайте файлы в виде ZIP-архива и распакуйте в желаемой директории либо клонируйте репозиторий:

   ```bash
   git clone https://github.com/NickFymann/CenterInvestPractice.git
   cd CenterInvestPractice
3. Разверните окружение:
   ```bash
   docker-compose up --build
4. Дождитесь заполнения базы данных
5. Сервисы Apache Superset и Apache Airflow доступны по адресам http://localhost:8088/login/ и http://localhost:8080/login/
6. Для запуска скрипта MainScript.py установите дополнительные библиотеки
   ```bash
   pip install requests pandas clickhouse-connect
   ```
   После авторизации в Apache Superset (по умолчанию login: admin, password: admin), вы сможете перейти ссылке, полученной в ходе выполнения скрипта, на чарт, где будет построен желаемый график.
7. Для корректной работы Apache Airflow установите дополнительные библиотеки
   ```bash
   pip install apache-airflow tenacity python-dateutil
   ```
   После авторизации в Apache Airflow  (по умолчанию login: admin, password: admin), необходимо будет активировать DAG (update_datasets), переключив ползунок возле его имени, чтобы запустить периодическое выполнение обновления данных (по умолчанию ползунок находится в положении выкл.)![image](https://github.com/user-attachments/assets/bb66057b-61bd-43ed-80de-1547abf78642)
8. Приложение готов к использованию!




