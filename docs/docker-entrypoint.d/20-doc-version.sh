#!/bin/sh
# Stamp the deployed version into every page at container start.
#
# Why at start and not at build: the shared CI workflow
# (team-redbull/.github ghcr-build-push.yml) builds this image with no build
# args, then bumps `image.tag` in helm-charts-workflows-docs. The chart passes
# that same tag back in as DOC_VERSION, so the string in the header is by
# construction the image that is actually running — it cannot go stale, and
# nobody has to remember to bump a constant.
#
# Runs from the nginx image's /docker-entrypoint.d/ hook, before nginx starts.
set -u

VERSION="${DOC_VERSION:-dev}"
ROOT="${DOC_ROOT:-/usr/share/nginx/html}"

# Deliberately NOT `set -e`. nginx's entrypoint runs this with the shell's
# errexit inherited, so a failed sed here would abort start-up and take the
# whole site down over a cosmetic version string. A page whose header reads
# `__DOC_VERSION__` is self-evidently unstamped; a CrashLoopBackOff is not.
failed=0

# Not `-exec … +`: BusyBox find has carried that flag only recently, and a
# read-loop behaves the same everywhere.
find "$ROOT" -type f -name '*.html' | while IFS= read -r page; do
    sed -i "s|__DOC_VERSION__|${VERSION}|g" "$page" || failed=1
done

if [ "$failed" -ne 0 ]; then
    echo "20-doc-version.sh: WARNING could not stamp some pages; serving them unstamped"
fi
echo "20-doc-version.sh: stamped DOC_VERSION=${VERSION}"
