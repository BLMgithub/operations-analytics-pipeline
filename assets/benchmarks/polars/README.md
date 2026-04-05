# Measurement Methodology

This section provides proof that the memory metrics in the root README were captured from a real Cloud Run execution of the 18M row dataset.

The telemetry logger below was added **temporarily** to the orchestrator for a specific benchmarking run. This code was pushed directly to the Cloud Artifact Registry as an experimental image tag (`mem-record`) and is not part of the permanent git repository history.

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

### Data Collection
*   **Source:** Real-time stdout logs from the Cloud Run job execution.
*   **Extraction:** Log entries with the `METRIC_MEM` prefix were filtered and exported as a CSV.
*   **Status:** This methodology ensures that the reported peak loads and "V-shaped" memory reclamation drops are reproducible and based on actual hardware performance.