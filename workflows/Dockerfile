# Workflow "brain" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f workflows/Dockerfile -t segment-connectivity-workflow:dev .
FROM python:3.12-slim

WORKDIR /app

# Lightweight deps only: temporalio + pydantic for the worker, plus fastapi +
# uvicorn so the same image can also serve the trigger API (workflows/api.py,
# started as a separate Deployment via `uvicorn workflows.api:app`; see
# helm/workflows). Same image as the brain — it shares the workflow
# definitions + Temporal client deps and holds no activity secrets. Still no
# activity-execution libraries.
COPY workflows/requirements.txt /app/workflows/requirements.txt
RUN pip install --no-cache-dir -r /app/workflows/requirements.txt

COPY shared/ /app/shared/
COPY workflows/ /app/workflows/

ENV PYTHONPATH=/app
CMD ["python", "-m", "workflows.main_worker_init"]
