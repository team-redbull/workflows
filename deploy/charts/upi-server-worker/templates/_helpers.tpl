{{- define "usw.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "usw.serviceAccountName" -}}
{{- default (include "usw.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{/* The limb's own ConfigMap, named after the LIMB (CLAUDE.md §2/§8). */}}
{{- define "usw.configName" -}}
{{- printf "%s-config" (include "usw.fullname" . | trimSuffix "-worker") -}}
{{- end -}}

{{- define "usw.selectorLabels" -}}
app: {{ include "usw.fullname" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "usw.labels" -}}
{{ include "usw.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "usw.isoBaseUrl" -}}
{{- if .Values.iso.baseUrl -}}
{{- .Values.iso.baseUrl | trimSuffix "/" -}}
{{- else if .Values.iso.route.host -}}
{{- printf "http://%s" .Values.iso.route.host -}}
{{- end -}}
{{- end -}}

{{- define "usw.validate" -}}
{{- if not .Values.upiCluster }}{{ fail "upiCluster is required: the UPI cluster's name as create-upi-cluster was given it" }}{{ end -}}
{{- if not .Values.temporal.host }}{{ fail "temporal.host is required: the hub's Temporal frontend, host:port" }}{{ end -}}
{{- if not .Values.serverScan.url }}{{ fail "serverScan.url is required" }}{{ end -}}
{{- if not (include "usw.isoBaseUrl" .) }}{{ fail "iso.baseUrl or iso.route.host is required: the URL the BMCs fetch ISOs from" }}{{ end -}}
{{- end -}}
