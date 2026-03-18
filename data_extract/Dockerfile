FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY data_extract/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_extract/ ./data_extract/

CMD ["python", "-m", "data_extract.run_extract"]