#!/usr/bin/env bash
# Sync controller-gen CRD manifests into Helm chart templates with install/keep conditionals.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CRD_SOURCE="config/crd/bases/dataflow.dataflow.io_dataflows.yaml"
HELM_TARGET="../helm-charts/charts/dataflow-operator/templates/crd.yaml"
CRON_CRD_SOURCE="config/crd/bases/dataflow.dataflow.io_dataflowcrons.yaml"
CRON_HELM_TARGET="../helm-charts/charts/dataflow-operator/templates/crd-dataflowcrons.yaml"

HEADER='{{- if .Values.crds.install }}'
FOOTER='{{- end }}'

sync_crd() {
  local source="$1"
  local target="$2"

  {
    echo "$HEADER"
    awk '
      /^---$/ { next }
      /^metadata:$/ {
        print
        getline
        if ($0 ~ /^  annotations:/) {
          print
          while (getline > 0 && $0 ~ /^    [^ ]/) {
            print
          }
          print "    {{- if .Values.crds.keep }}"
          print "    \"helm.sh/resource-policy\": keep"
          print "    {{- end }}"
          print "  labels:"
          print "    {{- include \"dataflow-operator.labels\" . | nindent 4 }}"
          print
        }
        next
      }
      { print }
    ' "$source"
    echo "$FOOTER"
  } > "$target"

  echo "Synced CRD to $target"
}

sync_crd "$CRD_SOURCE" "$HELM_TARGET"
sync_crd "$CRON_CRD_SOURCE" "$CRON_HELM_TARGET"
