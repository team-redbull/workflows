# Workflow "brain" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f workflow_domains/Dockerfile -t workflows:dev .
FROM python:3.12-slim

WORKDIR /app

# Lightweight deps only: temporalio + pydantic for the worker, plus fastapi +
# uvicorn so the same image can also serve the trigger API
# (workflow_domains/api.py, started as a separate Deployment via
# `uvicorn workflow_domains.api:app`; see helm-charts-workflows-orchestrator).
# Same image as the brain — it shares the workflow definitions + Temporal client
# deps and holds no activity secrets. Still no activity-execution libraries.
COPY workflow_domains/requirements.txt /app/workflow_domains/requirements.txt
RUN pip install --no-cache-dir -r /app/workflow_domains/requirements.txt

COPY shared/ /app/shared/
COPY workflow_domains/ /app/workflow_domains/

ENV PYTHONPATH=/app
CMD ["python", "-m", "workflow_domains.main_worker_init"]
