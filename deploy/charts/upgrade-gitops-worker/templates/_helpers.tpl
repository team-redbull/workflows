{{/* Release-scoped names. The default full name is the chart name, so a
release called upgrade-gitops-worker yields exactly the resource names the
rest of the stack expects (CLAUDE.md §2: `-worker` names the pod). */}}
{{- define "ugw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "ugw.serviceAccountName" -}}
{{- default (include "ugw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{/* The limb's own ConfigMap: named after the limb, like segment-lifecycle-config. */}}
{{- define "ugw.configName" -}}
{{- printf "%s-config" (include "ugw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "ugw.selectorLabels" -}}
app: {{ include "ugw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ugw.labels" -}}
{{ include "ugw.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/component: activity-worker
app.kubernetes.io/part-of: workflows
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Fail the render, not the pod: Argo shows a template error at sync time,
where a missing value is one edit away, instead of a crash-looping worker. */}}
{{- define "ugw.validate" -}}
{{- if not (hasPrefix "https://" .Values.day1.repoUrl) -}}{{- fail "day1.repoUrl is required, and https:// (the token is injected into https URLs only)" -}}{{- end -}}
{{- if not .Values.day1.tokenSecret.name -}}{{- fail "day1.tokenSecret.name is required: an existing Secret with a token that may push to day1 main" -}}{{- end -}}
{{- if and .Values.day2.repoUrl (not (hasPrefix "https://" .Values.day2.repoUrl)) -}}{{- fail "day2.repoUrl must be https:// when set (the token is injected into https URLs only; the worker refuses anything else at start)" -}}{{- end -}}
{{- if and .Values.brain.repoUrl (not (hasPrefix "https://" .Values.brain.repoUrl)) -}}{{- fail "brain.repoUrl must be https:// when set (the token is injected into https URLs only; the worker refuses anything else at start)" -}}{{- end -}}
{{- if and .Values.brain.repoUrl (not .Values.brain.tokenSecret.name) -}}{{- fail "brain.tokenSecret.name is required once brain.repoUrl is set: incidents are pushed to Brain main" -}}{{- end -}}
{{- if and (not .Values.globalConfigMap) (not .Values.temporal.host) -}}{{- fail "set temporal.host, or globalConfigMap to the brain's ConfigMap holding TEMPORAL_HOST" -}}{{- end -}}
{{- end -}}
