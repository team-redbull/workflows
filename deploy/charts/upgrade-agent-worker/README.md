# upgrade-agent-worker

The cluster-upgrade agent limb (`activities/upgrade_agent/`), one release per MCE. It polls
`upgrade-agent-activity-<mceCluster>` and runs `invoke_upgrade_agent`: a POST of the
`AgentRequest` to the upgrade agent service, heartbeating until the `AgentVerdict` comes back.

The agent service is not in this repo. It is the `upgrade-agent` chart of the k8s-agent folder: a
kubernetes-sigs agent-sandbox `Sandbox` (`agents.x-k8s.io/v1beta1`) behind a Service, holding the
model endpoint and the agent's own read-only identities. Install it first, then this:

```bash
helm install upgrade-agent-worker deploy/charts/upgrade-agent-worker -n upgrade-agent \
  --set mceCluster=<mce> \
  --set temporal.host=<temporal frontend host:port, as reachable from this MCE> \
  --set upgradeAgent.url=http://upgrade-agent.upgrade-agent.svc:8080 \
  --set upgradeAgent.tokenSecret.name=upgrade-agent-token   # the same Secret the agent's apiToken names
```

For TLS to the hub's Temporal (a passthrough Route), set `temporal.tls.*` as in
`deploy/charts/server-lifecycle-worker/README.md`.

What it holds: no Kubernetes credential and no RBAC (its ServiceAccount mounts no token), only the
agent service's URL and, optionally, its bearer token. Failures: an agent that cannot answer
(unreachable, timeout, 429, 5xx) is retried once by Temporal with the same invocation id; a refusal
(another 4xx) is non-retryable; a reply that is not an `AgentVerdict` comes back as `needs_human`.
