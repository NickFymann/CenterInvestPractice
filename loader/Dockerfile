FROM python:3.11

WORKDIR /app
COPY create_schema.py .
COPY load_data.py .
COPY requirements.txt .
COPY entrypoint.sh .

RUN pip install -r requirements.txt
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
