import os
from logging import config as logging_config

from core.logger import LOGGING

# применяем настройки логирования
logging_config.dictConfig(LOGGING)

# название проекта. Используется в свагер документации
PROJECT_NAME = os.getenv('PROJECT_NAME', 'Read-only API для онлайн-кинотеатра')

# настройки редиса
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# настройки эластика
ELASTIC_HOST = os.getenv('ELASTIC_HOST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ELASTIC_PORT', 9200))

# корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
