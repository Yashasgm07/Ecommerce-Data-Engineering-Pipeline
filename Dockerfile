FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN mkdir -p /logs
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","etl/run_pipeline.py"]
