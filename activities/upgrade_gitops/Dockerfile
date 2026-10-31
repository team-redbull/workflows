# Upgrade-gitops activity worker "limb" image (the cluster-upgrade HUB limb).
# Build from the REPO ROOT so shared/ is in context:
#   podman build -f activities/upgrade_gitops/Dockerfile -t upgrade-gitops-worker:dev .
# An OpenShift binary build works the same way (Docker strategy, this file as
# dockerfilePath, the repo root uploaded as the context).

# oc: pin_release_digests runs `oc adm release info` on every release it pins
# (activities/upgrade_gitops/oc_release.py). Taken from the OKD client image of
# the sandbox's minor, 4.20; `oc adm release info` reads releases of any minor.
# Bump the tag here to move it.
FROM quay.io/openshift/origin-cli:4.20 AS oc

FROM python:3.12-slim

# git: the day1, day2 and Brain repos are cloned and pushed by subprocess
# (activities/git_repo.py), never a Python git library.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=oc /usr/bin/oc /usr/local/bin/oc

WORKDIR /app

COPY activities/upgrade_gitops/requirements.txt /app/activities/upgrade_gitops/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/upgrade_gitops/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
# Any non-root UID works (OpenShift assigns its own): every clone, and oc's
# HOME, is a fresh temporary directory.
USER 1001
CMD ["python", "-m", "activities.upgrade_gitops.worker_init"]
