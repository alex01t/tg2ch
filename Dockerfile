FROM python:3.12-slim

RUN apt-get update -qqq && apt-get install -qqq curl vim net-tools procps
WORKDIR /app
RUN pip install --no-cache-dir telethon clickhouse-connect python-dotenv qrcode

COPY app.py /app/app.py
CMD ["python", "-u", "/app/app.py"]
