import os
from logging import config as logging_config

from core.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)

# Название проекта. Используется в Swagger-документации
PROJECT_NAME = os.getenv('PROJECT_NAME', 'movies-notification')


# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Auth host
AUTH_GRPC_HOST = os.getenv('AUTH_GRPC_HOST', '127.0.0.1')
AUTH_GRPC_PORT = os.getenv('AUTH_GRPC_PORT', '50051')

RABBITMQ_DEFAULT_USER = os.getenv('RABBITMQ_DEFAULT_USER')
RABBITMQ_DEFAULT_PASS = os.getenv('RABBITMQ_DEFAULT_PASS')
RABBIT_URL = f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:" \
             f"{os.getenv('RABBITMQ_DEFAULT_PASS')}@movies-rabbitmq:5672/"

SENTRY_DSN = os.getenv('SENTRY_DSN')