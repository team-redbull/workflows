"""day1 `upgrade/ladder.yaml` + `upgrade/mces/<mce>.yaml` into UpgradeRules — pure.

The day1 repo's own validator (`scripts/validate_upgrade_rules.py`, run by
day1_repo.py before this) is the validator of record: this module only maps
the validated YAML onto the model, and refuses what it cannot map rather than
guessing. Key names are the files' (camelCase); field reference in day1's
`upgrade/README.md`.

The `release` block (D25, 2026-10-31): `mirrored` (default true) says whether
releases come from an internal mirror; `cpoRepo`/`cpoTag` are optional and go
together — a layout without them pins no control-plane-operator image, and
HyperShift runs the one the payload carries.
"""

from __future__ import annotations

import datetime
from typing import Any

from activities.upgrade_gitops.day1_values import load_yaml
from shared.exceptions import UpgradeRulesInvalidError
from shared.models.cluster_upgrade import AdminAckRule, LadderRung, ReleaseLayout, UpgradeRules

LADDER_FILE = "upgrade/ladder.yaml"
VALIDATOR = "scripts/validate_upgrade_rules.py"


def mce_file(mce_cluster: str) -> str:
    return f"upgrade/mces/{mce_cluster}.yaml"


def _document(text: str, where: str) -> dict:
    data, problems = load_yaml(text, where)
    if problems:
        raise UpgradeRulesInvalidError("; ".join(problems))
    if not isinstance(data, dict):
        raise UpgradeRulesInvalidError(f"{where}: must be a mapping")
    return data


def _get(data: dict, key: str, kind: type | tuple[type, ...], where: str) -> Any:
    value = data.get(key)
    if isinstance(value, bool) and kind is not bool:
        raise UpgradeRulesInvalidError(f"{where}: {key} must not be true/false")
    if not isinstance(value, kind):
        raise UpgradeRulesInvalidError(f"{where}: {key} is missing or not a {getattr(kind, '__name__', kind)}")
    return value


def _release(data: dict) -> ReleaseLayout:
    where = f"{LADDER_FILE}: release"
    block = _get(data, "release", dict, LADDER_FILE)
    cpo_repo, cpo_tag = block.get("cpoRepo"), block.get("cpoTag")
    if (cpo_repo is None) != (cpo_tag is None):
        raise UpgradeRulesInvalidError(f"{where}: cpoRepo and cpoTag go together, or neither is set")
    mirrored = block.get("mirrored", True)
    if not isinstance(mirrored, bool):
        raise UpgradeRulesInvalidError(f"{where}: mirrored must be true or false")
    return ReleaseLayout(
        image_path=_get(block, "imagePath", str, where),
        repo=_get(block, "repo", str, where),
        tag=_get(block, "tag", str, where),
        cpo_repo=None if cpo_repo is None else str(cpo_repo),
        cpo_tag=None if cpo_tag is None else str(cpo_tag),
        mirrored=mirrored,
    )


def parse_rules(*, revision: str, ladder_text: str, mce_text: str, mce_cluster: str) -> UpgradeRules:
    """The two files as one UpgradeRules; UpgradeRulesInvalidError for anything it cannot map."""
    ladder = _document(ladder_text, LADDER_FILE)
    where_mce = mce_file(mce_cluster)
    mce = _document(mce_text, where_mce)
    rungs = _get(ladder, "ladder", list, LADDER_FILE)
    parsed_rungs = []
    for index, rung in enumerate(rungs):
        at = f"{LADDER_FILE}: ladder[{index}]"
        if not isinstance(rung, dict):
            raise UpgradeRulesInvalidError(f"{at} must be a mapping")
        parsed_rungs.append(
            LadderRung(
                version=_get(rung, "version", str, at),
                must=bool(rung.get("must", False)),
                data_plane_required=rung.get("dataPlane") == "required",
            )
        )
    verified = ladder.get("verifiedAgainstGraph")
    if isinstance(verified, datetime.date):
        verified = verified.isoformat()
    acks = []
    for index, ack in enumerate(ladder.get("adminAcks") or []):
        at = f"{LADDER_FILE}: adminAcks[{index}]"
        if not isinstance(ack, dict):
            raise UpgradeRulesInvalidError(f"{at} must be a mapping")
        acks.append(
            AdminAckRule(
                gate=_get(ack, "gate", str, at),
                apis=[str(api) for api in _get(ack, "apis", list, at)],
                source=str(ack.get("source") or ""),
            )
        )
    mce_block = _get(mce, "mce", dict, where_mce)
    return UpgradeRules(
        revision=revision,
        ladder=parsed_rungs,
        max_worker_skew=_get(ladder, "maxWorkerSkew", int, LADDER_FILE),
        verified_against_graph=None if verified is None else str(verified),
        release=_release(ladder),
        mce_version=_get(mce_block, "version", str, f"{where_mce}: mce"),
        supported_hosted_versions=[
            str(minor) for minor in _get(mce_block, "supportedHostedVersions", list, f"{where_mce}: mce")
        ],
        max_concurrent_control_planes=_get(mce, "maxConcurrentControlPlanes", int, where_mce),
        admin_acks=acks,
        api_usage_ignored_users=[str(user) for user in ladder.get("apiUsageIgnoredUsers") or []],
    )
