FROM python:3.8-slim

WORKDIR /usr/src/app

# install dependencies
COPY requirements/ .
RUN pip install -U pip && pip install -r requirements/prod.txt
COPY . .

ENV PYTHONPATH ${work_dir}
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
