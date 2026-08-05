# Segment-lifecycle activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/segment_lifecycle/Dockerfile -t segment-lifecycle-worker:dev .
FROM python:3.12-slim

WORKDIR /app

COPY activities/segment_lifecycle/requirements.txt /app/activities/segment_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/segment_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.segment_lifecycle.worker_init"]
