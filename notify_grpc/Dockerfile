FROM python:3.9.7-buster

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY . /code
EXPOSE 50051
ENTRYPOINT ["python3", "registration/registration_event_listener.py"]