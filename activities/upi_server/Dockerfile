# UPI-server activity worker "limb" image: it runs INSIDE each UPI cluster.
# Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/upi_server/Dockerfile -t upi-server-worker:dev .

# oc: `oc adm node-image create` builds each node's ISO (activities/upi_server/node_image.py).
# Taken from the OKD client image of the platform's minor, 4.20; node-image needs an
# oc of the cluster's minor or newer. Bump the tag here to move it.
FROM quay.io/openshift/origin-cli:4.20 AS oc

FROM python:3.12-slim

# nmstatectl is not needed here: node-image validates the network config inside
# the cluster, in the pod it starts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=oc /usr/bin/oc /usr/local/bin/oc

WORKDIR /app

COPY activities/upi_server/requirements.txt /app/activities/upi_server/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/upi_server/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Any non-root UID works (OpenShift assigns its own): oc's HOME is a fresh
# temporary folder per build, and the ISOs go to the mounted ISO_DIR.
USER 1001
CMD ["python", "-m", "activities.upi_server.worker_init"]
