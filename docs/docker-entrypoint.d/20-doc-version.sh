#!/bin/sh
# Stamp the deployed version into every page at container start.
#
# Two jobs, one placeholder: the string shown in the header, AND the ?v= on every
# /assets/ URL. That second one is load-bearing — nginx.conf caches assets for a
# year and relies on this rewrite to change their URLs on redeploy. Stop stamping
# and readers keep last year's CSS; stamp a version that does not change between
# two different builds (e.g. iterating locally on the default `dev`) and the same
# thing happens on that machine, so pass a fresh DOC_VERSION when editing assets.
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
