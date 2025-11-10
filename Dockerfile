# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /app
ENV PYTHONPATH=/app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt \
    && pip cache purge

COPY . /app/
EXPOSE 8000