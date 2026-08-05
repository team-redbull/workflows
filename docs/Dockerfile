# The documentation site — a 4th image out of this repo, alongside the brain,
# the segment-lifecycle-worker limb and the NEXT mock. Static pages only: it holds
# no credentials, talks to nothing, and is deployed by Argo CD from
# helm-charts-workflows-docs into the redbull-workflows namespace.
#
# Build from the REPO ROOT, like the other three images (the shared CI workflow
# always passes the repo root as the build context):
#   docker build -f docs/Dockerfile -t workflows-docs:dev .
#
# nginx-unprivileged, not nginx: OpenShift's restricted SCC runs the container
# as an arbitrary UID with GID 0, which the stock image cannot do (it wants to
# write /var/run/nginx.pid and bind :80). This variant listens on 8080 and keeps
# its runtime state where an arbitrary user can write it.
FROM nginxinc/nginx-unprivileged:1.27-alpine

# Everything below needs root; the base image drops back to a non-root user at
# the end, and so do we.
USER root

COPY docs/nginx.conf /etc/nginx/conf.d/default.conf
COPY docs/site/ /usr/share/nginx/html/
COPY docs/docker-entrypoint.d/ /docker-entrypoint.d/

# 20-doc-version.sh rewrites the pages in place at start-up, so the html tree
# has to be writable by whichever UID actually runs.
#
# BOTH halves of the ownership matter, for two different runtimes:
#   owner 101  — plain docker/kind honours the `USER 101` below, whose primary
#                group is nginx(101), NOT root, so group-write alone leaves it
#                unable to write (sed fails, and with it the whole entrypoint).
#   group 0    — OpenShift ignores USER and picks an arbitrary UID, which is
#                always a member of GID 0. `g=u` gives that UID the same rights.
RUN chmod +x /docker-entrypoint.d/*.sh \
    && chown -R 101:0 /usr/share/nginx/html /docker-entrypoint.d \
    && chmod -R g=u /usr/share/nginx/html /docker-entrypoint.d

USER 101

# Overridden by the chart with the deployed image tag; `dev` is what you get
# running this image by hand.
ENV DOC_VERSION=dev

EXPOSE 8080
