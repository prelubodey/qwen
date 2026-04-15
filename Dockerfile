FROM python:3.10-slim

WORKDIR /app

# Установка зависимостей
RUN pip install --no-cache-dir requests python-dotenv beautifulsoup4

# Копируем скрипты
COPY ps.py monitor.py /app/

# Папка для данных будет примонтирована через volumes
RUN mkdir /app/data

# По умолчанию ничего не запускаем, команда передается в docker-compose.yml
