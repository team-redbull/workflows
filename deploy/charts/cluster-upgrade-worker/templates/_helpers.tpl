{{/* Release-scoped names. The default full name is the chart name, so a
release called cluster-upgrade-worker yields exactly the resource names the
rest of the stack expects (CLAUDE.md §2: `-worker` names the pod). */}}
{{- define "cuw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "cuw.serviceAccountName" -}}
{{- default (include "cuw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{/* The limb's own ConfigMap, named after the DOMAIN (CLAUDE.md §2/§8, `<domain>-config`):
cluster-upgrade-config, as upgrade-gitops-worker's is upgrade-gitops-config. */}}
{{- define "cuw.configName" -}}
{{- printf "%s-config" (include "cuw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "cuw.selectorLabels" -}}
app: {{ include "cuw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cuw.labels" -}}
{{ include "cuw.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/component: activity-worker
app.kubernetes.io/part-of: workflows
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
workflows.redbull/mce-cluster: {{ .Values.mceCluster | quote }}
{{- end -}}

{{/* Fail the render, not the pod: Argo shows a template error at sync time,
where a missing value is one edit away, instead of a crash-looping worker. */}}
{{- define "cuw.validate" -}}
{{- if not .Values.mceCluster -}}{{- fail "mceCluster is required: the MCE this worker serves (its folder in day1)" -}}{{- end -}}
{{- if not .Values.temporal.host -}}{{- fail "temporal.host is required: the hub's Temporal frontend as reachable from this MCE" -}}{{- end -}}
{{- if not .Values.argo.namespace -}}{{- fail "argo.namespace is required: where the hosted clusters' Applications live" -}}{{- end -}}
{{- end -}}
