FROM python:3.9.7-buster

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY . /code

CMD ["gunicorn", "main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]