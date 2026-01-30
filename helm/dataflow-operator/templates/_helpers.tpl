{{/*
Expand the name of the chart.
*/}}
{{- define "dataflow-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dataflow-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dataflow-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dataflow-operator.labels" -}}
helm.sh/chart: {{ include "dataflow-operator.chart" . }}
{{ include "dataflow-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dataflow-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dataflow-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "dataflow-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dataflow-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Full name for GUI deployment and service
*/}}
{{- define "dataflow-operator.gui.fullname" -}}
{{- printf "%s-gui" (include "dataflow-operator.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Service account name for GUI (uses operator SA if gui.serviceAccount.name not set)
*/}}
{{- define "dataflow-operator.gui.serviceAccountName" -}}
{{- if .Values.gui.serviceAccount.name }}
{{- .Values.gui.serviceAccount.name }}
{{- else }}
{{- include "dataflow-operator.serviceAccountName" . }}
{{- end }}
{{- end }}

