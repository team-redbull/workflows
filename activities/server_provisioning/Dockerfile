# Server-provisioning activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/server_provisioning/Dockerfile -t server-provisioning-worker:dev .
FROM python:3.12-slim

# Pure-Python HTTP only: iDRAC Redfish, OME, the naming service, server-scan.
WORKDIR /app

COPY activities/server_provisioning/requirements.txt /app/activities/server_provisioning/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/server_provisioning/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.server_provisioning.worker_init"]
