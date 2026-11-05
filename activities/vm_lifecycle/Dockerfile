# VM-lifecycle activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/vm_lifecycle/Dockerfile -t vm-lifecycle-worker:dev .
FROM python:3.12-slim

# git: list_kv_clusters clones the day1 values repo via subprocess
# (activities/vm_lifecycle/day1_repo.py on activities/day1_repo.py and git_repo.py).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# HTTP only otherwise: the KubeVirt clusters' agents. No Kubernetes client.
WORKDIR /app

COPY activities/vm_lifecycle/requirements.txt /app/activities/vm_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/vm_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Numeric, so runAsNonRoot holds on plain Kubernetes too; OpenShift assigns its own UID.
USER 1001
CMD ["python", "-m", "activities.vm_lifecycle.worker_init"]
