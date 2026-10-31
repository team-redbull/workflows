# upgrade-gitops-worker

The cluster-upgrade HUB limb (`activities/upgrade_gitops/`), ONE release on the hub, in the
brain's namespace. It polls `upgrade-gitops-activity` and runs the seven activities of
`shared/interfaces/upgrade_gitops.py`: the day1 rules and cluster files, the upgrade's commits to
day1 `main`, release pins and payload checks in the registry, day2's version layers, and Brain
incidents. It calls no Kubernetes API: no RBAC, and its ServiceAccount mounts no token.

## What it needs

| | Where from | Required |
|---|---|---|
| `TEMPORAL_HOST`, `TEMPORAL_NAMESPACE` | the brain's `workflows-orchestrator-config` (`globalConfigMap`), or `temporal.host` | yes |
| day1 repo, https | `day1.repoUrl` | yes |
| a token that may push to day1 `main` | existing Secret `day1.tokenSecret` (default `day1-git-token`, key `DAY1_GIT_TOKEN`, the segment-lifecycle worker's) | yes |
| day1's validator, run before every push | `day1.validateCommand` | no (each commit logs its absence) |
| a registry auth file | existing Secret `registry.authSecret` (a `kubernetes.io/dockerconfigjson` pull secret as is) | for the payload check on quay.io |
| day2 repo (+ token) | `day2.repoUrl`, `day2.tokenSecret` | no (check 4 then says it was not checked) |
| Brain repo + push token | `brain.repoUrl`, `brain.tokenSecret` | no (verdicts then stay in the run's history) |

The image carries git and `oc` (from `quay.io/openshift/origin-cli:4.20`); it runs as any
non-root UID with a read-only root filesystem, everything it writes on the `/tmp` emptyDir.

## Install (the sandbox hub)

The chart creates no Secret. With the cluster's pull secret copied into the namespace as
`upgrade-pull-secret` (any dockerconfigjson holding a `quay.io` credential):

```bash
helm install upgrade-gitops-worker deploy/charts/upgrade-gitops-worker -n redbull-workflows \
  --set image.tag=<main-short sha published by CI> \
  --set day1.repoUrl=https://github.com/team-redbull/gitops-day1-platform-config.git \
  --set registry.authSecret.name=upgrade-pull-secret
```

Brain, when wanted: `--set brain.repoUrl=https://github.com/team-redbull/redbull-brain.git
--set brain.tokenSecret.name=<Secret with BRAIN_GIT_TOKEN>`. Day2 the same way, with the mock
`team-redbull/gitops-day2-prod` (public: no token needed).

## Failures, and what each one means

- Planning fails (permanent, the run's error says which): the rules missing or refused by
  `scripts/validate_upgrade_rules.py` (`UpgradeRulesInvalidError`), no or two cluster files of
  that name (`ClusterFileNotFoundError`, `AmbiguousClusterFileError`).
- A step waits for a person: someone else changed the cluster file (`Day1FileChangedError`), or
  the day1 validator or the line editor refused the change (`Day1ChangeRejectedError`).
- Retried until it clears, visible on the pending activity: git unreachable or a push rejected
  three times (`ValuesRepoGitError`), the registry unreachable, a 5xx, or credentials refused on
  the release repository itself (`ReleaseRegistryError`).
- Reported, never raised: a release tag the registry lacks, a payload image missing or not
  readable with the auth file (planning's check 2 lists them).
