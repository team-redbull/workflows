# The lab's fake KV clusters (tests/kv_fleet.py) as a pod: each fake cluster polls its own
# vm-lifecycle queue on the lab's Temporal and runs the REAL worker code over an in-memory
# cluster. Built from the repo root like the other images; never shipped anywhere.
FROM python:3.12-slim
WORKDIR /app
COPY activities/vm_lifecycle/requirements.txt /app/activities/vm_lifecycle/requirements.txt
COPY workflow_domains/requirements.txt /app/workflow_domains/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/vm_lifecycle/requirements.txt -r /app/workflow_domains/requirements.txt
COPY shared/ /app/shared/
COPY activities/ /app/activities/
COPY workflow_domains/ /app/workflow_domains/
COPY tests/kv_fleet.py /app/tests/kv_fleet.py
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1001
ENTRYPOINT ["python", "-m", "tests.kv_fleet"]
