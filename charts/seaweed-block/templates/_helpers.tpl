{{/*
Common template helpers for Seaweed Block.
*/}}
{{- define "seaweed-block.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "seaweed-block.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "seaweed-block.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "seaweed-block.labels" -}}
app.kubernetes.io/name: {{ include "seaweed-block.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "seaweed-block.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "seaweed-block.csiImage" -}}
{{- if .Values.csiImage.digest -}}
{{- printf "%s@%s" .Values.csiImage.repository .Values.csiImage.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.csiImage.repository .Values.csiImage.tag -}}
{{- end -}}
{{- end -}}

{{- define "seaweed-block.statePermissionsImage" -}}
{{- if .Values.blockmaster.statePermissionsImage.digest -}}
{{- printf "%s@%s" .Values.blockmaster.statePermissionsImage.repository .Values.blockmaster.statePermissionsImage.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.blockmaster.statePermissionsImage.repository .Values.blockmaster.statePermissionsImage.tag -}}
{{- end -}}
{{- end -}}

{{- define "seaweed-block.blockmasterAddress" -}}
{{- printf "%s.%s.svc.cluster.local:%v" .Values.blockmaster.serviceName .Release.Namespace .Values.blockmaster.listenPort -}}
{{- end -}}

{{- define "seaweed-block.expectedSlotsPerVolume" -}}
{{- if gt (int .Values.replication.expectedSlotsPerVolume) 0 -}}
{{- .Values.replication.expectedSlotsPerVolume -}}
{{- else -}}
{{- .Values.storageClass.replicationFactor -}}
{{- end -}}
{{- end -}}

{{/* Mutual TLS toward blockmaster for an in-cluster client: the Secret
     .Values.tls.clientSecret mounted at /etc/sw-block/master-tls. */}}
{{- define "seaweed-block.masterTLSClientFlags" -}}
- "--master-tls-cert=/etc/sw-block/master-tls/tls.crt"
- "--master-tls-key=/etc/sw-block/master-tls/tls.key"
- "--master-ca=/etc/sw-block/master-tls/ca.crt"
{{- end -}}

{{- define "seaweed-block.masterTLSClientEnv" -}}
- name: SW_BLOCK_TLS_CERT
  value: /etc/sw-block/master-tls/tls.crt
- name: SW_BLOCK_TLS_KEY
  value: /etc/sw-block/master-tls/tls.key
- name: SW_BLOCK_TLS_CA
  value: /etc/sw-block/master-tls/ca.crt
{{- end -}}

{{- define "seaweed-block.masterTLSClientMount" -}}
- name: master-tls
  mountPath: /etc/sw-block/master-tls
  readOnly: true
{{- end -}}

{{- define "seaweed-block.masterTLSClientVolume" -}}
- name: master-tls
  secret:
    secretName: {{ .Values.tls.clientSecret }}
{{- end -}}
