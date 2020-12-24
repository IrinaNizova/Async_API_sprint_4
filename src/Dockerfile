FROM python:3.8-slim

WORKDIR /app

# install dependencies
COPY requirements/* requirements/
RUN pip install -U pip && pip install -r requirements/prod.txt
COPY . .

ENV PYTHONPATH ${work_dir}
#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
