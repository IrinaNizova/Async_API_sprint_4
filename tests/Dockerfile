FROM python:3.8-slim

WORKDIR /usr/src/app

# install dependencies
COPY requirements/ .
RUN pip install -U pip && pip install -r requirements.txt
COPY . .
CMD ["python", "pytest", "."]