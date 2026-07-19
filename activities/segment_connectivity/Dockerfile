# Segment-connectivity activity "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/segment_connectivity/Dockerfile -t segment-connectivity:dev .
FROM python:3.12-slim

WORKDIR /app

COPY activities/segment_connectivity/requirements.txt /app/activities/segment_connectivity/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/segment_connectivity/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.segment_connectivity.worker_init"]
