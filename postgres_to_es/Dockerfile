FROM python:3.8-slim

WORKDIR /app

# get last pip version
RUN /usr/local/bin/python3 -m pip install --upgrade pip
## get last pip version

# install deps
COPY requirements.txt .
RUN /usr/local/bin/python3 -m pip install -I -r requirements.txt
## install deps

# Copy project files
COPY . .
