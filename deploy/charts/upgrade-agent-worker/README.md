# upgrade-agent-worker

The cluster-upgrade agent limb (`activities/upgrade_agent/`), one release per MCE. It polls
`upgrade-agent-activity-<mceCluster>` and runs `invoke_upgrade_agent`: a POST of the
`AgentRequest` to the upgrade agent service, heartbeating until the `AgentVerdict` comes back.

The agent service is not in this repo. It is the `upgrade-agent` chart of the k8s-agent folder: a
kubernetes-sigs agent-sandbox `Sandbox` (`agents.x-k8s.io/v1beta1`) behind a Service, holding the
model endpoint and the agent's own read-only identities. Install it first, then this:

```bash
helm install upgrade-agent-worker <chart dir> -n upgrade-agent \
  --set mceCluster=<mce> \
  --set temporal.host=<temporal frontend host:port, as reachable from this MCE> \
  --set image.tag=<vX.Y.Z> \
  --set upgradeAgent.url=http://upgrade-agent.upgrade-agent.svc:8080 \
  --set upgradeAgent.tokenSecret.name=upgrade-agent-token   # the same Secret the agent's apiToken names
```

`<chart dir>`: this chart, in the workflows repo at `deploy/charts/upgrade-agent-worker/` or its
copy in redbull-platform at `gitops/charts/upgrade-agent-worker/` (no `gitops/services/` entry
yet, so Argo CD deploys nothing from it; CI's `sync-upgrade-charts` copies this chart there on
every push to `main` that changes it, so edit it here, never there). `<vX.Y.Z>`: every push to `main` that touches `activities/` or `shared/` publishes
`ghcr.io/team-redbull/upgrade-agent-worker:vX.Y.Z` (`build-upgrade-agent-worker`, the shared
build workflow) and writes that tag into redbull-platform's copy, so installed from there
`image.tag` can be left unset. A branch build is tagged `<branch-slug>-<short sha>`.

For TLS to the hub's Temporal (a passthrough Route), set `temporal.tls.*` as in the workflows
repo's `deploy/charts/server-lifecycle-worker/README.md`.

What it holds: no Kubernetes credential and no RBAC (its ServiceAccount mounts no token), only the
agent service's URL and, optionally, its bearer token. The URL and the Temporal endpoint are in
the ConfigMap `upgrade-agent-config` (`<domain>-config`, CLAUDE.md §8; before, this chart named it
`upgrade-agent-worker-config` — an upgrade of the release replaces it and rolls the pods, nothing
to do by hand). `serviceAccount.create: false` with `serviceAccount.name` uses an existing
ServiceAccount instead. Two replicas by default: the agent call is the one activity with a
schedule-to-start timeout, so a lone pod restarting would send a gate to needs-human. The pod runs
as a non-root UID with a read-only root filesystem, `/tmp` its only writable path. Failures: an agent that cannot answer
(unreachable, timeout, 429, 5xx) is retried once by Temporal with the same invocation id; a refusal
(another 4xx) is non-retryable; a reply that is not an `AgentVerdict` comes back as `needs_human`.
