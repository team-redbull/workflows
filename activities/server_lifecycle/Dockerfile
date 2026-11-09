# Server-lifecycle activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/server_lifecycle/Dockerfile -t server-lifecycle-worker:dev .
FROM python:3.12-slim

# git for update-nodepool's day1 read and replicas commit (activities/git_repo.py
# runs the binary; no Python git dependency), as in the segment-lifecycle limb.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY activities/server_lifecycle/requirements.txt /app/activities/server_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/server_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.server_lifecycle.worker_init"]
