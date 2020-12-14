import os

SERVICE_URL = 'http://127.0.0.1:8000'
ELASTICSEARCH_HOST = '127.0.0.1:9200'

# настройки редиса
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
