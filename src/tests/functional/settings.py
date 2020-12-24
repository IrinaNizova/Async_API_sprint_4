import os

API_HOST = os.getenv('API_HOST', '127.0.0.1')
API_PORT = int(os.getenv('API_PORT', 8000))

SERVICE_URL = f'http://{API_HOST}:{API_PORT}'

ELASTIC_HOST = os.getenv('ELASTICSEARCH_HOST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))

# настройки редиса
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
