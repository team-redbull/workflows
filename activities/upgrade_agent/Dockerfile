# Upgrade-agent activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/upgrade_agent/Dockerfile -t upgrade-agent-worker:dev .
FROM python:3.12-slim

# HTTP only: the upgrade agent service in the same MCE. No Kubernetes client.
WORKDIR /app

COPY activities/upgrade_agent/requirements.txt /app/activities/upgrade_agent/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/upgrade_agent/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Numeric, so runAsNonRoot holds on plain Kubernetes too; OpenShift assigns its own UID.
USER 1001
CMD ["python", "-m", "activities.upgrade_agent.worker_init"]
