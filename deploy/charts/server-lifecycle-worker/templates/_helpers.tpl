{{/* Release-scoped names. The default full name is the chart name, so a
release called server-lifecycle-worker yields exactly the resource names the
rest of the stack expects (CLAUDE.md §2: `-worker` names the pod). */}}
{{- define "slw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "slw.serviceAccountName" -}}
{{- default (include "slw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "slw.credentialsSecret" -}}
{{- default (printf "%s-credentials" (include "slw.fullname" .)) .Values.credentials.existingSecret -}}
{{- end -}}

{{- define "slw.selectorLabels" -}}
app: {{ include "slw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "slw.labels" -}}
{{ include "slw.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/component: activity-worker
app.kubernetes.io/part-of: workflows
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
workflows.redbull/mce-cluster: {{ .Values.mceCluster | quote }}
{{- end -}}

{{/* Fail the render, not the pod: Argo shows a template error at sync time,
where a missing value is one edit away, instead of a crash-looping worker. */}}
{{- define "slw.validate" -}}
{{- if not .Values.mceCluster -}}{{- fail "mceCluster is required: the MCE this worker serves" -}}{{- end -}}
{{- if not .Values.temporal.host -}}{{- fail "temporal.host is required: the hub's Temporal frontend as reachable from this MCE" -}}{{- end -}}
{{- if not .Values.serverScan.url -}}{{- fail "serverScan.url is required (base URL including /api/v1)" -}}{{- end -}}
{{- if and .Values.credentials.existingSecret .Values.credentials.create -}}{{- fail "set credentials.existingSecret OR credentials.create, not both" -}}{{- end -}}
{{- end -}}
