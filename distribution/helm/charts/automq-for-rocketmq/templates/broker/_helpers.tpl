{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "rocketmq-broker.fullname" -}}
{{- if .Values.broker.fullnameOverride }}
{{- .Values.broker.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else -}}
rocketmq-broker
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "rocketmq-broker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "rocketmq-broker.labels" -}}
{{ include "rocketmq-broker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "rocketmq-broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rocketmq-broker.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/cluster: {{ include "rocketmq-broker.clusterName" . }}
{{- end }}

{{- define "rocketmq-broker.clusterName" -}}
{{- if .Values.broker.clusterNameOverride }}
{{- .Values.broker.clusterNameOverride | trunc 63 | trimSuffix "-" }}
{{- else -}}
{{ .Release.Name }}
{{- end }}
{{- end }}

{{- define "rocketmq-broker.brokerImage" -}}
{{ .Values.broker.image.repository }}:{{ .Values.broker.image.tag | default .Chart.AppVersion }}
{{- end }}