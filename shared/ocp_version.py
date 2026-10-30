"""OCP release versions, as day1 writes them and HyperShift reports them.

Lives in `shared/` because BOTH sides parse them: the limb reads `mastertag` and
`workertag` out of a day1 cluster file and writes them back, and the brain's
path resolver orders and compares them.

Two spellings are accepted, and only two, because they are the only two the
hosted-cluster chart treats as an x86_64 release:

  * `X.Y.Z`         — the chart's `hc.releaseTag` appends `-x86_64` to it;
  * `X.Y.Z-x86_64`  — the chart passes it through unchanged.

Anything else (`4.20.9-multi`, `4.21.0-rc.1`, `latest`) is a tag the chart would
also pass through unchanged, into an image reference this design never
validated — so it is refused here rather than half-understood.
"""

from __future__ import annotations

import re

_RELEASE_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)(?:-x86_64)?")

# Every hosted cluster in the fleet is OCP 4; a 5.x would bring a different
# upgrade model, and nothing here should guess at it.
SUPPORTED_MAJOR = 4

# The suffix the chart's hc.releaseTag appends to a bare X.Y.Z.
_ARCH_SUFFIX = "-x86_64"


def parse_release(text: str) -> tuple[int, int, int]:
    """(major, minor, patch) of an `X.Y.Z` or `X.Y.Z-x86_64` release.

    Raises ValueError for anything else, including a major other than 4.
    """
    match = _RELEASE_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(
            f"not an OCP release version: {text!r} (expected X.Y.Z or X.Y.Z-x86_64)"
        )
    major, minor, patch = (int(part) for part in match.groups())
    if major != SUPPORTED_MAJOR:
        raise ValueError(f"not an OCP {SUPPORTED_MAJOR} release: {text!r}")
    return major, minor, patch


def release_version(text: str) -> str:
    """The bare `X.Y.Z` of a release, whichever of the two spellings it came in."""
    major, minor, patch = parse_release(text)
    return f"{major}.{minor}.{patch}"


def release_tag(version: str) -> str:
    """The mirror tag of a release: `X.Y.Z-x86_64`, as the chart renders it."""
    return release_version(version) + _ARCH_SUFFIX


def minor_of(version: str) -> int:
    """The minor number of a release (16 for `4.16.27`)."""
    return parse_release(version)[1]


def minor_name(minor: int) -> str:
    """`4.<minor>`, the way supported-versions and the docs spell a minor."""
    return f"{SUPPORTED_MAJOR}.{minor}"
