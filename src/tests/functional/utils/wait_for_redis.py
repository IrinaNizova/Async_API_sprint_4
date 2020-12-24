import os
from time import sleep


import redis as redis

REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))


def main():
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    while True:
        if redis_client.ping():
            return
        sleep(0.1)


if __name__ == '__main__':
    main()
