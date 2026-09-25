# Server-lifecycle activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/server_lifecycle/Dockerfile -t server-lifecycle-worker:dev .
FROM python:3.12-slim

# No git here, unlike the segment-lifecycle limb: this worker's dependencies
# are HTTP (server-scan) and the Kubernetes API, both pure-Python.
WORKDIR /app

COPY activities/server_lifecycle/requirements.txt /app/activities/server_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/server_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.server_lifecycle.worker_init"]
