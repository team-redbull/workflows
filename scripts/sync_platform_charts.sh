#!/usr/bin/env bash
# Copy cluster-upgrade's per-MCE charts from deploy/charts/ into a redbull-platform
# checkout (gitops/charts/<chart>/), so nobody keeps the two copies equal by hand.
#
# Each chart is copied whole (a template removed here is removed there), then the four
# fields the shared build workflow owns there are put back: image.repository and
# image.tag in values.yaml, version and appVersion in Chart.yaml. So the tag the last
# main build published survives a chart-only change. They are put back with yq, the
# tool that build wrote them with: yq re-emits the whole file, so a chart that did not
# change comes out byte-identical to what the last bump left, and the sync commits nothing.
#
# Usage: scripts/sync_platform_charts.sh <redbull-platform checkout>
# Needs mikefarah yq v4 (GitHub's runners have it). Run by build.yml's
# sync-upgrade-charts job on main, which renders, commits and pushes; this script
# commits nothing.
set -euo pipefail

# server-lifecycle-worker is NOT here: redbull-platform's chart of that name is its
# own, reshaped chart, not a copy.
CHARTS=(cluster-upgrade-worker upgrade-agent-worker)

platform="${1:?usage: $0 <redbull-platform checkout>}"
here="$(cd "$(dirname "$0")/.." && pwd)"
[ -d "${platform}/gitops/charts" ] || { echo "no gitops/charts/ in ${platform}" >&2; exit 1; }

for chart in "${CHARTS[@]}"; do
  src="${here}/deploy/charts/${chart}"
  dst="${platform}/gitops/charts/${chart}"
  [ -f "${src}/Chart.yaml" ] || { echo "no chart at ${src}" >&2; exit 1; }

  owned=false
  if [ -f "${dst}/Chart.yaml" ] && [ -f "${dst}/values.yaml" ]; then
    repository="$(yq '.image.repository' "${dst}/values.yaml")"
    tag="$(yq '.image.tag' "${dst}/values.yaml")"
    version="$(yq '.version' "${dst}/Chart.yaml")"
    app_version="$(yq '.appVersion' "${dst}/Chart.yaml")"
    owned=true
  fi

  rm -rf "${dst}"
  cp -R "${src}" "${dst}"

  if [ "${owned}" = true ]; then
    REPOSITORY="${repository}" TAG="${tag}" \
      yq -i '.image.repository = strenv(REPOSITORY) | .image.tag = strenv(TAG)' "${dst}/values.yaml"
    VERSION="${version}" APP_VERSION="${app_version}" \
      yq -i '.version = strenv(VERSION) | .appVersion = strenv(APP_VERSION)' "${dst}/Chart.yaml"
    echo "${chart}: synced, kept ${repository}:${tag} and chart ${version}"
  else
    echo "${chart}: first copy"
  fi
done
