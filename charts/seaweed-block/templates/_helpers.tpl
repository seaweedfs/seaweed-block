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

{{/* Mutual TLS toward blockmaster for an in-cluster client: its own Secret
     (one of .Values.tls.clientSecrets, passed to masterTLSClientVolume)
     mounted at /etc/sw-block/master-tls. */}}
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

{{/* The CSI node and the status operator only read: refuse a Secret named
     like an admin client's or blockmaster's own (the chart sees names, not
     roles; the role file decides the role). */}}
{{- define "seaweed-block.readOnlyClientSecrets" -}}
{{- $s := .Values.tls.clientSecrets -}}
{{- if eq $s.csiNode $s.csiController $s.failbackExecutor .Values.tls.masterSecret -}}
{{- fail (printf "tls.clientSecrets.csiNode %q is the CSI controller's, the failback executor's or blockmaster's Secret: the node plugin never mounts an admin key" $s.csiNode) -}}
{{- end -}}
{{- if eq $s.operatorStatus $s.csiController $s.failbackExecutor .Values.tls.masterSecret -}}
{{- fail (printf "tls.clientSecrets.operatorStatus %q is the CSI controller's, the failback executor's or blockmaster's Secret: the status operator never mounts an admin key" $s.operatorStatus) -}}
{{- end -}}
{{- end -}}

{{- define "seaweed-block.masterTLSClientVolume" -}}
- name: master-tls
  secret:
    secretName: {{ . }}
{{- end -}}
