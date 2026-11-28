{{- define "upw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "upw.serviceAccountName" -}}
{{- default (include "upw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "upw.configName" -}}
{{- printf "%s-config" (include "upw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "upw.sitesName" -}}
{{- printf "%s-sites" (include "upw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "upw.selectorLabels" -}}
app: {{ include "upw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "upw.labels" -}}
{{ include "upw.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "upw.validate" -}}
{{- if not .Values.temporal.host }}{{ fail "temporal.host is required" }}{{ end -}}
{{- if not .Values.installer.image }}{{ fail "installer.image is required: bootstrap-installer's image" }}{{ end -}}
{{- end -}}
