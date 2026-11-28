# UPI-provisioning activity worker "limb" image (the upi-lifecycle HUB limb).
# Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/upi_provisioning/Dockerfile -t upi-provisioning-worker:dev .
#
# No installer binaries here: openshift-install, terraform and oc run in the
# bootstrap-installer image (INSTALLER_IMAGE), as Jobs this worker creates. This
# worker only talks to the hub's API and to the installed cluster's API.
FROM python:3.12-slim

WORKDIR /app

COPY activities/upi_provisioning/requirements.txt /app/activities/upi_provisioning/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/upi_provisioning/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

# No .pyc writes: the chart runs the root filesystem read-only.
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Numeric, so runAsNonRoot holds on plain Kubernetes too; OpenShift assigns its own UID.
USER 1001
CMD ["python", "-m", "activities.upi_provisioning.worker_init"]
