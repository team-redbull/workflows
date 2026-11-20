"""ClusterLifecycleActivitySettings — The cluster-lifecycle hub limb only: the day1 repo, GitLab's API, the DHCP policy, the day2 sig projects and the shortcuts service.

One settings module per worker (shared/settings/__init__.py says why): this file
is loaded by the images that read it and by no other.
"""

from __future__ import annotations

import string
from typing import Annotated

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.models.segment_lifecycle import SegmentType


class ClusterLifecycleActivitySettings(BaseSettings):
    """Config for the cluster-lifecycle HUB limb (`cluster-lifecycle-worker`) only.

    The day1 values repo (the cluster file, its branch, the merge request on
    the repo's git provider), the DHCP exclusion policy the allocation block is
    written with, and the day2 sig repos the cluster is registered in. No
    Kubernetes credential. Keep aligned with redbull-platform's
    gitops/charts/cluster-lifecycle-worker/ (cluster-lifecycle-config and the
    Secrets it names).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- day1: the cluster file, its branch, the merge request ----------------
    # The same repo as upgrade-gitops's and vm-lifecycle's (DAY1_REPO_URL). The
    # token is REDBULL_GIT_TOKEN, the key of the shared `redbull-git-token` Secret
    # (the air-gap's name, the operator 2026-11-11), so the chart passes the Secret
    # through unrenamed. DAY1_GIT_TOKEN is still accepted, for a chart that maps a
    # `day1-git-token` Secret (the dev cluster): REDBULL_GIT_TOKEN wins when both
    # are set. This limb READS `main` (where the cluster goes)
    # and PUSHES a new branch per cluster, `provision-cluster/<cluster>`, which a
    # person merges through the merge request the limb opens. The token must
    # therefore reach GitLab's API too (the merge request): a group, project or
    # personal token with `api` + `write_repository`, and a role that may push
    # the sig projects' protected default branch (Maintainer, by default).
    # GitLab is the ONLY provider (the operator, 2026-11-14: one convention, so
    # there is no key choosing one — do not add it). The API base is derived
    # from this URL's host and path.
    #
    # The clusters root, the branch scheme and the committer identity are NOT
    # config: they are the day1 repo's own layout and this workflow's identity
    # (activities/cluster_lifecycle/day1_writer.py).
    day1_repo_url: str
    redbull_git_token: Annotated[str, Field(validation_alias=AliasChoices("REDBULL_GIT_TOKEN", "DAY1_GIT_TOKEN"))]
    # --- the DHCP exclusion policy, written into every cluster file ------------
    # The ONE DHCP policy knob, PER SEGMENT TYPE: last-octet ranges excluded
    # from distribution, e.g. {"HC": [[1, 10], [241, 254]]}. Together with the
    # network they are the WHOLE written block: no startRange/endRange, so the
    # DHCP API derives .1-.253 and these exclusions carve the ends out of it —
    # one derivation, in the service that owns it. /24 segments only —
    # build_dhcp_values asserts the prefix and rejects anything else as
    # UnsupportedSegmentPrefix. A type's list may be EMPTY (excludes nothing).
    #
    # Keyed by TYPE because the policy genuinely differs per type (a PXE
    # segment reserves a different slice of its /24 than an HC one), and the
    # lookup is DYNAMIC — the activity selects the ranges for the type actually
    # allocated, which a flat per-type key could not express.
    #
    # A type that is NOT LISTED excludes nothing — a type earns an entry by
    # reserving part of its /24, and no operator should have to write an empty
    # list to say "nothing". HC is the one REQUIRED key: it is the only type
    # provision-cluster allocates today, and a forgotten HC policy would quietly
    # hand out the addresses production reserves rather than fail. Types listed
    # ahead of the code that allocates them are validated the same way now.
    # (Moved here from SegmentLifecycleActivitySettings on 2026-11-11, with the
    # cluster file; the policy predates the service-prefix rule, CLAUDE.md §3.)
    dhcp_exclusion_octet_ranges: dict[SegmentType, list[tuple[int, int]]]

    # --- day2: the sig repos the cluster is registered in -----------------------
    # Each sig is its own GitLab project, the tree at its root:
    # https://<gitlab>/redbull/gitops-day2-prod/sigs/{sig}.git — `{sig}` must
    # appear in the URL, or every sig would be registered in one project. The
    # push goes to the project's default branch with DAY2_GIT_TOKEN —
    # REDBULL_GIT_TOKEN when empty, the group token covering both.
    # (A path-prefix template, for sigs kept as folders of ONE repo, was removed
    # 2026-11-14: a sig is always its own project.)
    day2_sig_repo_url_template: str
    day2_git_token: str = ""

    # --- the console shortcut (go-link), provision-cluster's last step ----------
    # The shortcuts service: its base URL (`<url>/check/<name>`, `<url>/create`)
    # and its bearer token — that service's own values, so its prefix (§3).
    shortcuts_api_url: str
    shortcuts_api_token: str
    # The `username` every shortcut is created with — and how a re-run knows a
    # taken name is ITS OWN: the service records the username sent and its check
    # reports it back as `owner`. A name owned by anyone else fails the run.
    shortcut_owner: str = Field(min_length=1)
    # The URL the shortcut points at: the hosted cluster's console, with
    # `{cluster}` = the day1 file stem (`ocp4-prod-herz1-site1-a`), e.g.
    # https://console-openshift-console.apps.{cluster}.<base domain>. A template
    # because day1 records no base domain the run could read.
    cluster_console_url_template: str

    # --- create-vm's placement policy (no service prefix: it is ours, CLAUDE.md §3) ------
    # The share of EVERY KV cluster's usable allocatable that must stay unrequested
    # after a VM is placed, on CPU and on memory — per cluster, never per node: the
    # room live migration needs when a node is drained. ONE value for the whole
    # fleet, here on the hub, handed to each create-vm run with its zone's cluster
    # list (list_kv_clusters), so a run records the buffer it decided with and a
    # changed value applies to the runs started after it. Not a setting of the
    # worker inside a KV cluster (docs/design/create-vm-placement.md §6.4).
    headroom_buffer_percent: int = 15

    @field_validator("headroom_buffer_percent")
    @classmethod
    def _buffer(cls, value: int) -> int:
        if not 0 <= value <= 90:
            raise ValueError(f"headroom_buffer_percent must be 0..90 (got {value})")
        return value

    @field_validator("day1_repo_url")
    @classmethod
    def _require_https_day1_repo_url(cls, url: str) -> str:
        """The token is injected into https:// URLs only (git_repo.authenticated_url).
        Any other scheme would silently drop it: every ls-remote would then fail
        authentication, which is the retryable ValuesRepoGitError, so each run
        would retry forever. Fail at worker startup instead."""
        if not url.startswith("https://"):
            raise ValueError(
                "day1_repo_url must be an https:// URL — the token is only "
                f"injected into https clone URLs (got {url.split('@')[-1]!r})"
            )
        return url.rstrip("/")

    @field_validator("dhcp_exclusion_octet_ranges")
    @classmethod
    def _validate_dhcp_exclusion_octet_ranges(
        cls, policy: dict[SegmentType, list[tuple[int, int]]]
    ) -> dict[SegmentType, list[tuple[int, int]]]:
        """Strict, fail-fast validation of the one DHCP policy knob: a policy
        for HC at minimum, and within EVERY type's ranges each octet a valid
        /24 host octet, pairs ordered, strictly ascending and non-overlapping,
        and at least one distributable octet left. A type's list may be EMPTY
        (that type excludes nothing); the map itself may not be."""
        if not policy:
            raise ValueError("dhcp_exclusion_octet_ranges must not be empty")
        if SegmentType.HC not in policy:
            raise ValueError(
                "dhcp_exclusion_octet_ranges must carry a policy for type "
                f"{SegmentType.HC.value!r} (the only type provision-cluster "
                "allocates); got "
                f"{sorted(segment_type.value for segment_type in policy)}"
            )
        for segment_type, ranges in policy.items():
            label = segment_type.value
            # An EMPTY list is legal and meaningful: that type excludes
            # nothing, so its block carries a network and no exclusions and the
            # scope distributes the DHCP API's whole derived .1-.253. Written
            # explicitly, it says an operator decided — unlike a missing key.
            previous_end = 0
            for start, end in ranges:
                if not (1 <= start <= 254 and 1 <= end <= 254):
                    raise ValueError(
                        f"exclusion octets must be in 1..254 "
                        f"(got [{start}, {end}] for {label})"
                    )
                if start > end:
                    raise ValueError(
                        f"inverted exclusion range [{start}, {end}] for {label}"
                    )
                if start <= previous_end:
                    raise ValueError(
                        "exclusion ranges must be strictly ascending and "
                        f"non-overlapping (got [{start}, {end}] after octet "
                        f"{previous_end} for {label})"
                    )
                previous_end = end
            # The DHCP API distributes .1-.253 (it stops short of the .254
            # gateway it derives), so that is what the exclusions eat into — a
            # policy covering all of it leaves nothing to lease.
            excluded = {
                octet for start, end in ranges for octet in range(start, end + 1)
            }
            if excluded >= set(range(1, 254)):
                raise ValueError(
                    f"dhcp_exclusion_octet_ranges[{label}] excludes every "
                    "distributable octet (.1-.253) — nothing left to distribute"
                )
        return policy

    @field_validator("day2_sig_repo_url_template")
    @classmethod
    def _https_sig_url_template(cls, template: str) -> str:
        if not template.startswith("https://"):
            raise ValueError(
                "day2_sig_repo_url_template must be an https:// URL — the token is only "
                f"injected into https clone URLs (got {template.split('@')[-1]!r})"
            )
        return template.rstrip("/")

    @field_validator("shortcuts_api_url")
    @classmethod
    def _http_shortcuts_api_url(cls, url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"shortcuts_api_url must be an http:// or https:// URL (got {url!r})")
        return url.rstrip("/")

    @field_validator("cluster_console_url_template")
    @classmethod
    def _console_url_template(cls, template: str) -> str:
        """http(s), `{cluster}` and nothing else in braces: a typo (`{clustr}`)
        fails at worker start, not as a KeyError at the end of a merged run."""
        if not template.startswith(("http://", "https://")):
            raise ValueError(f"cluster_console_url_template must be an http:// or https:// URL (got {template!r})")
        try:
            fields = {field for _, field, _, _ in string.Formatter().parse(template) if field is not None}
        except ValueError as err:
            raise ValueError(f"cluster_console_url_template {template!r} is not a format string: {err}") from None
        if fields != {"cluster"}:
            raise ValueError(
                f"cluster_console_url_template {template!r} must name {{cluster}} and nothing else "
                f"(got {sorted(fields) or 'no placeholder'})"
            )
        return template.rstrip("/")

    @model_validator(mode="after")
    def _sig_is_in_the_url_template(self) -> ClusterLifecycleActivitySettings:
        """`{sig}` in the URL template, and nothing else in braces: a typo
        (`{sgi}`) fails at worker start, not as a KeyError on the first run."""
        template = self.day2_sig_repo_url_template
        try:
            fields = {field for _, field, _, _ in string.Formatter().parse(template) if field is not None}
        except ValueError as err:
            raise ValueError(f"day2_sig_repo_url_template {template!r} is not a format string: {err}") from None
        if fields - {"sig"}:
            raise ValueError(f"day2_sig_repo_url_template {template!r} may name only {{sig}} (got {sorted(fields - {'sig'})})")
        if "sig" not in fields:
            raise ValueError(
                f"day2_sig_repo_url_template {template!r} does not name {{sig}}: every sig would be "
                "registered in one project"
            )
        return self

    @property
    def day2_token(self) -> str:
        """The token the sig repos are pushed with: DAY2_GIT_TOKEN, else REDBULL_GIT_TOKEN."""
        return self.day2_git_token or self.redbull_git_token

    def day2_sig_repo_url(self, sig: str) -> str:
        return self.day2_sig_repo_url_template.format(sig=sig)

    def cluster_console_url(self, cluster: str) -> str:
        return self.cluster_console_url_template.format(cluster=cluster)
