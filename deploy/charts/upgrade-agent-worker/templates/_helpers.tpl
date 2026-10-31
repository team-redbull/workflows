{{/* Release-scoped names: a release called upgrade-agent-worker yields exactly that name (CLAUDE.md §2). */}}
{{- define "uaw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "uaw.serviceAccountName" -}}
{{- default (include "uaw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{/* The limb's own ConfigMap, named after the DOMAIN (CLAUDE.md §2/§8, `<domain>-config`):
upgrade-agent-config, as upgrade-gitops-worker's is upgrade-gitops-config. */}}
{{- define "uaw.configName" -}}
{{- printf "%s-config" (include "uaw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "uaw.selectorLabels" -}}
app: {{ include "uaw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "uaw.labels" -}}
{{ include "uaw.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/component: activity-worker
app.kubernetes.io/part-of: workflows
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
workflows.redbull/mce-cluster: {{ .Values.mceCluster | quote }}
{{- end -}}

{{/* Fail the render, not the pod. */}}
{{- define "uaw.validate" -}}
{{- if not .Values.mceCluster -}}{{- fail "mceCluster is required: the MCE this worker serves" -}}{{- end -}}
{{- if not .Values.temporal.host -}}{{- fail "temporal.host is required: the hub's Temporal frontend as reachable from this MCE" -}}{{- end -}}
{{- if not .Values.upgradeAgent.url -}}{{- fail "upgradeAgent.url is required: the upgrade agent service's URL" -}}{{- end -}}
{{- end -}}
