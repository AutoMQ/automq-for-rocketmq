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

{{- define "rocketmq-broker.config" -}}
{{- $name := include "rocketmq-broker.fullname" . }}
{{- $clusterName := include "rocketmq-broker.clusterName" . }}
{{- $brokerNamePrefix := include "rocketmq-broker.brokerNamePrefix" . }}
{{- $config := .Values.broker.conf.name }}
{{- $s3stream := .Values.broker.conf.s3Stream }}
{{- $bindAddress := .Values.broker.service }}
{{- $innerKey := .Values.broker.conf.inner }}
{{- $db := .Values.broker.conf.db }}
{{- $controller := .Values.broker.conf.controller }}
{{- $metrics := .Values.broker.conf.metrics }}
{{- $store := .Values.broker.conf.store }}
{{- $replicaCount := .Values.broker.replicaCount | int }}
{{- range $index := until $replicaCount }}
  {{ $clusterName }}-{{ $name }}-{{ $index }}: |
    name: {{ $clusterName }}-{{ $name }}-{{ $index }}
    instanceId: {{ $brokerNamePrefix }}-{{ $index }}
    bindAddress: "0.0.0.0:{{ $bindAddress.port }}"
    innerAccessKey: {{ $innerKey.accessKey }}
    innerSecretKey: {{ $innerKey.secretKey }}
    s3Stream:
      s3WALPath: {{ $s3stream.s3WALPath }}
      s3Endpoint: {{ $s3stream.s3Endpoint }}
      s3Bucket: {{ $s3stream.s3Bucket }}
      s3Region: {{ $s3stream.s3Region }}
      s3ForcePathStyle: {{ $s3stream.s3ForcePathStyle }}
      s3AccessKey: {{ $s3stream.s3AccessKey }}
      s3SecretKey: {{ $s3stream.s3SecretKey }}
    db:
      url: {{ $db.url }}
      userName: {{ $db.userName }}
      password: {{ $db.password }}
    store:
      kvPath: {{ $store.kvPath }}
    controller:
      recycleS3IntervalInSecs: {{ $controller.recycleS3IntervalInSecs }}
      dumpHeapOnError: {{ $controller.dumpHeapOnError }}
    metrics:
      exporterType: {{ $metrics.exporterType }}
      grpcExporterTarget: {{ $metrics.grpcExporterTarget }}
      grpcExporterHeader: {{ $metrics.grpcExporterHeader }}
      grpcExporterTimeOutInMills: {{ $metrics.grpcExporterTimeOutInMills }}
      periodicExporterIntervalInMills: {{ $metrics.periodicExporterIntervalInMills }}
      promExporterPort: {{ $metrics.promExporterPort }}
      promExporterHost: {{ $metrics.promExporterHost }}
      labels: {{ $metrics.labels }}
      exportInDelta: {{ $metrics.exportInDelta }}
{{ $config | indent 4 }}
{{- end }}
{{- end }}