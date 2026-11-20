#!/usr/bin/env bash
# Copy the charts of the limbs that run OUTSIDE the hub (cluster-upgrade's two per-MCE
# limbs, vm-lifecycle's per-KV-cluster one) from deploy/charts/ into a redbull-platform
# checkout (gitops/charts/<chart>/), so nobody keeps the two copies equal by hand.
#
# Each chart is copied whole (a template removed here is removed there), then the four
# fields the shared build workflow owns there are put back: image.repository and
# image.tag in values.yaml, version and appVersion in Chart.yaml. So the tag the last
# main build published survives a chart-only change. They are put back with yq, the
# tool that build wrote them with: yq re-emits the whole file, so a chart that did not
# change comes out byte-identical to what the last bump left, and the sync commits nothing.
#
# A destination of ANOTHER SHAPE is replaced, not "kept": redbull-platform's
# vm-lifecycle-worker was a hub chart with its image under `activityWorker.image`
# until 2026-11-20, so it has no `.image.repository` to put back — yq answers `null`,
# and writing that back would publish `repository: null`. Its chart version and
# appVersion are still kept (the build's `<chart>/vX.Y.Z` tags go on from them); its
# image is the chart's own default until the image's next build writes the tag.
#
# Usage: scripts/sync_platform_charts.sh <redbull-platform checkout>
# Needs mikefarah yq v4 (GitHub's runners have it). Run by build.yml's
# sync-upgrade-charts job on main, which renders, commits and pushes; this script
# commits nothing.
set -euo pipefail

# server-lifecycle-worker is NOT here: redbull-platform's chart of that name is its
# own, reshaped chart, not a copy.
CHARTS=(cluster-upgrade-worker upgrade-agent-worker vm-lifecycle-worker)

platform="${1:?usage: $0 <redbull-platform checkout>}"
here="$(cd "$(dirname "$0")/.." && pwd)"
[ -d "${platform}/gitops/charts" ] || { echo "no gitops/charts/ in ${platform}" >&2; exit 1; }

for chart in "${CHARTS[@]}"; do
  src="${here}/deploy/charts/${chart}"
  dst="${platform}/gitops/charts/${chart}"
  [ -f "${src}/Chart.yaml" ] || { echo "no chart at ${src}" >&2; exit 1; }

  # What the build workflow wrote there and this copy must keep. `// ""` turns a
  # field the destination does not have into an empty string, never yq's `null`.
  versioned=false
  owned=false
  if [ -f "${dst}/Chart.yaml" ] && [ -f "${dst}/values.yaml" ]; then
    version="$(yq '.version // ""' "${dst}/Chart.yaml")"
    app_version="$(yq '.appVersion // ""' "${dst}/Chart.yaml")"
    [ -n "${version}" ] && [ -n "${app_version}" ] && versioned=true
    repository="$(yq '.image.repository // ""' "${dst}/values.yaml")"
    tag="$(yq '.image.tag // ""' "${dst}/values.yaml")"
    # Ours only when the image sits where this chart keeps it.
    [ "${versioned}" = true ] && [ -n "${repository}" ] && owned=true
  fi

  rm -rf "${dst}"
  cp -R "${src}" "${dst}"

  if [ "${versioned}" = true ]; then
    VERSION="${version}" APP_VERSION="${app_version}" \
      yq -i '.version = strenv(VERSION) | .appVersion = strenv(APP_VERSION)' "${dst}/Chart.yaml"
  fi
  if [ "${owned}" = true ]; then
    REPOSITORY="${repository}" TAG="${tag}" \
      yq -i '.image.repository = strenv(REPOSITORY) | .image.tag = strenv(TAG)' "${dst}/values.yaml"
    echo "${chart}: synced, kept ${repository}:${tag} and chart ${version}"
  elif [ "${versioned}" = true ]; then
    echo "${chart}: replaced a chart of another shape (no .image.repository), kept chart ${version}; its image tag is written by the image's next build"
  else
    echo "${chart}: first copy"
  fi
done
