# Cluster-upgrade per-MCE activity worker "limb" image. Build from the REPO ROOT so shared/ is in context:
#   docker build -f activities/cluster_upgrade/Dockerfile -t cluster-upgrade-worker:dev .
FROM python:3.12-slim

# No git and no oc here: this limb's only dependency is the Kubernetes API —
# its own MCE's and, through each HostedCluster's admin kubeconfig, the hosted
# clusters' — and the client is pure Python. The day1 git work is the hub
# limb's (upgrade-gitops-worker).
WORKDIR /app

COPY activities/cluster_upgrade/requirements.txt /app/activities/cluster_upgrade/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/cluster_upgrade/requirements.txt

# The whole activities/ tree: this limb reuses server_lifecycle's cluster_api.py
# and hosted_cluster_api.py (one place for kubeconfigs and classification).
COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.cluster_upgrade.worker_init"]
