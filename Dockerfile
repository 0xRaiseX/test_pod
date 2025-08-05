FROM python:3.11-slim

# Установка зависимостей
RUN pip install psycopg2-binary mysql-connector-python pymongo

# Копируем скрипт
COPY main.py /app/main.py
WORKDIR /app

# Точка входа
CMD ["python", "main.py"]
