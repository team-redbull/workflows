# Cluster-lifecycle activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/cluster_lifecycle/Dockerfile -t cluster-lifecycle-worker:dev .
FROM python:3.12-slim

# git: provision-cluster clones day1 main, pushes the cluster file's branch and
# pushes each day2 sig repo's default branch via subprocess
# (activities/cluster_lifecycle/day1_writer.py, day2_sigs.py on activities/git_repo.py).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# HTTP only otherwise: the day1 repo's git provider API (GitLab or GitHub) for
# the merge request. No Kubernetes client.
WORKDIR /app

COPY activities/cluster_lifecycle/requirements.txt /app/activities/cluster_lifecycle/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/cluster_lifecycle/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
# Numeric, so runAsNonRoot holds on plain Kubernetes too; OpenShift assigns its own UID.
USER 1001
CMD ["python", "-m", "activities.cluster_lifecycle.worker_init"]
