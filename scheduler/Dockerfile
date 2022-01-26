FROM python:3.8-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY . /code

