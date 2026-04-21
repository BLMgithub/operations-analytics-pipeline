# Measurement Methodology

This section details the methodology used to capture the memory metrics in the [`GCP Stress-Test Metrics (Scaling Efficiency)`](/README.md#gcp-stress-test-metrics-scaling-efficiency)

The telemetry logger below was added to the orchestrator for a specific benchmarking run. 

```python
import psutil
import threading
import time

def memory_logger(stop_event: threading.Event):
    """Temporary: Logs RAM usage to stdout every 1s for benchmarking."""
    while not stop_event.is_set():
        mem_mb = psutil.virtual_memory().used / (1024 * 1024)
        print(f"METRIC_MEM: {mem_mb:.2f} MB")
        time.sleep(1)

# Orchestrator Lifecycle
stop_event = threading.Event()
logger_thread = threading.Thread(target=memory_logger, args=(stop_event,))
logger_thread.start()

try:
    # Execute Pipeline Stages...
    ...
finally:
    stop_event.set()
    logger_thread.join()
```
Since `psutil` requires C-extensions to compile, the **Dockerfile** was modified to include the necessary build tools and the package itself. This allowed for benchmarking without altering the project's permanent [`requirements.txt`](/data_pipeline/requirements.txt).

```docker
FROM python:3.11-slim
ENV # Environments...

WORKDIR /app

COPY data_pipeline/requirements.txt .

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev && \
    pip install --no-cache-dir -r requirements.txt psutil && \
    apt-get purge -y --auto-remove gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY data_pipeline/ ./data_pipeline/
ENV PYTHONPATH=/app

# the rest of docker code...

```

### Data Collection
*   **Source:** Real-time stdout logs from the Cloud Run job execution.
*   **Extraction:** Log entries with the `METRIC_MEM` prefix were filtered and exported as a CSV.
*   **Status:** This methodology ensures that the reported peak loads and "V-shaped" memory reclamation drops are reproducible and based on actual hardware performance.