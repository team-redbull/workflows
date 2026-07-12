# Workflow "brain" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f workflows/Dockerfile -t connectivity-workflow:dev .
FROM python:3.12-slim

WORKDIR /app

# Lightweight deps only (temporalio + pydantic) — no heavy execution libraries.
COPY workflows/requirements.txt /app/workflows/requirements.txt
RUN pip install --no-cache-dir -r /app/workflows/requirements.txt

COPY shared/ /app/shared/
COPY workflows/ /app/workflows/
# The connectivity-workflow trigger front-door (started as a separate
# Deployment via `uvicorn api:app`; see helm/workflow-worker). Same image as
# the brain — it shares the workflow definitions + Temporal client deps and
# holds no activity secrets.
COPY api.py /app/api.py

ENV PYTHONPATH=/app
CMD ["python", "-m", "workflows.main_worker_init"]
