{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "plexor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "plexor.selectorLabels" -}}
app: {{ .Chart.Name }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "plexor.labels" -}}
helm.sh/chart: {{ include "plexor.chart" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{ include "plexor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ (split ":" .Values.imagesVersion )._1 | replace "+" "_" | trunc 63 | trimSuffix "-" | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Service name
*/}}
{{- define "plexor.gename" -}}
{{ .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}


