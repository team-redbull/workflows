# Connectivity activity "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/connectivity/Dockerfile -t connectivity-activity:dev .
FROM python:3.12-slim

WORKDIR /app

COPY activities/connectivity/requirements.txt /app/activities/connectivity/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/connectivity/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.connectivity.worker_init"]
