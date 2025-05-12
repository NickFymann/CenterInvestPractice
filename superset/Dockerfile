FROM apache/superset:latest
RUN pip install Pillow
RUN pip install clickhouse-connect
COPY superset_config.py /app/pythonpath/superset_config.py
RUN superset db upgrade
RUN superset init
RUN superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@admin.com \
    --password admin

EXPOSE 8088
CMD ["superset", "run", "--host", "0.0.0.0", "--port", "8088"]


