# Workflow "brain" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f workflows/Dockerfile -t segment-allocation-workflow:dev .
FROM python:3.12-slim

WORKDIR /app

# Lightweight deps only (temporalio + pydantic) — no heavy execution libraries.
COPY workflows/requirements.txt /app/workflows/requirements.txt
RUN pip install --no-cache-dir -r /app/workflows/requirements.txt

COPY shared/ /app/shared/
COPY workflows/ /app/workflows/

ENV PYTHONPATH=/app
CMD ["python", "-m", "workflows.main_worker"]
