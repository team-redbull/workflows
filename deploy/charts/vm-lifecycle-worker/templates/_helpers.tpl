{{/* Release-scoped names. The default full name is the chart name, so a
release called vm-lifecycle-worker yields exactly the resource names the
rest of the stack expects (CLAUDE.md §2: `-worker` names the pod). */}}
{{- define "vlw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "vlw.serviceAccountName" -}}
{{- default (include "vlw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{/* The limb's own ConfigMap, named after the DOMAIN (CLAUDE.md §2/§8, `<domain>-config`). */}}
{{- define "vlw.configName" -}}
{{- printf "%s-config" (include "vlw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "vlw.selectorLabels" -}}
app: {{ include "vlw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "vlw.labels" -}}
{{ include "vlw.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/component: activity-worker
app.kubernetes.io/part-of: workflows
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
workflows.redbull/kv-cluster: {{ .Values.kvCluster | quote }}
{{- end -}}

{{/* Fail the render, not the pod: Argo shows a template error at sync time,
where a missing value is one edit away, instead of a crash-looping worker —
and a KV cluster whose worker does not start is skipped as `no-worker` by
every create-vm run of its zone. The same checks the worker's settings make
at start (shared/settings/vm_lifecycle.py). */}}
{{- define "vlw.validate" -}}
{{- if not .Values.kvCluster -}}{{- fail "kvCluster is required: the KV cluster this worker runs in, EXACTLY its day1 file name (ocp4-<lifecycle>-kv-<site>-<zone>-<id>)" -}}{{- end -}}
{{- if not (regexMatch "^ocp4-[a-z0-9]+-kv-[a-z0-9]+(-[a-z0-9]+)*-az[1-9][0-9]*-[a-z]$" .Values.kvCluster) -}}{{- fail (printf "kvCluster %q is not a KV cluster's day1 file name: ocp4-<lifecycle>-kv-<site>-<zone>-<id>, never abbreviated" .Values.kvCluster) -}}{{- end -}}
{{- if not .Values.config.vmMacPrefix -}}{{- fail "config.vmMacPrefix is required: `0e` + the cluster's VLAN ID, e.g. 0e:03:f2 for VLAN 1010" -}}{{- end -}}
{{- if not (regexMatch "^0[eE](:[0-9a-fA-F]{2}){2}$" .Values.config.vmMacPrefix) -}}{{- fail (printf "config.vmMacPrefix %q must be three hex octets starting with 0e (0e:xx:xx): locally administered, and neither KubeMacPool's 02 nor OVN-Kubernetes' 0a" .Values.config.vmMacPrefix) -}}{{- end -}}
{{- if not .Values.vm.storageClassName -}}{{- fail "vm.storageClassName is required: the root disk's storage class (Immediate binding, ReadWriteMany + Filesystem)" -}}{{- end -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?/[a-z0-9]([-a-z0-9.]*[a-z0-9])?$" (toString .Values.vm.networkName)) -}}{{- fail (printf "vm.networkName %q must be <namespace>/<name> of the VMs' NetworkAttachmentDefinition" (toString .Values.vm.networkName)) -}}{{- end -}}
{{- if not .Values.temporal.host -}}{{- fail "temporal.host is required: the hub's Temporal frontend as reachable from this KV cluster" -}}{{- end -}}
{{- end -}}
