FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY data_pipeline/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_pipeline/ ./data_pipeline/

CMD ["python", "-m", "data_pipeline.run_pipeline"]