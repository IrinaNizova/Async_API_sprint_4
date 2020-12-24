import os
from datetime import datetime
from time import sleep

import elasticsearch

ELASTIC_HOST = os.getenv('ELASTIC_HOST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ELASTIC_PORT', 9200))


def main():
    es_connect = elasticsearch.Elasticsearch(hosts=[f'{ELASTIC_HOST}:{ELASTIC_PORT}'])
    start = datetime.now()
    while True:
        if es_connect.ping():
            print(datetime.now() - start)
            return
        sleep(0.1)


if __name__ == '__main__':
    main()
