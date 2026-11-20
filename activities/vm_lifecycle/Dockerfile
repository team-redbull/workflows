# VM-lifecycle activity worker "limb" image: it runs INSIDE each KV cluster.
# Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/vm_lifecycle/Dockerfile -t vm-lifecycle-worker:dev .
FROM python:3.12-slim

# The Kubernetes client only: this worker talks to its own cluster's API as its
# ServiceAccount. No git (the zone's cluster list is the cluster-lifecycle
# limb's read) and no HTTP client.
WORKDIR /app

COPY activities/vm_lifecycle/requirements.txt /app/activities/vm_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/vm_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Numeric, so runAsNonRoot holds on plain Kubernetes too; OpenShift assigns its own UID.
USER 1001
CMD ["python", "-m", "activities.vm_lifecycle.worker_init"]
