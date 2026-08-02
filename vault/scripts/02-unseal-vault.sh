#!/bin/bash
# Run this after every Vault pod restart (crash, node reboot, minikube
# stop/start, pod eviction, a Helm upgrade that recreates the pod...). Vault
# always starts sealed; unsealing is not persisted, it's a runtime state
# rebuilt in memory every time the process starts. Safe to re-run - if
# Vault is already unsealed, this is a no-op.
#
# Reads 3 of the 5 unseal keys from vault-init-output.json automatically
# (see lib.sh) - nothing is typed in by hand.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

sealed="$(vault_status_json | jq -r 'if .sealed then "true" else "false" end')"

if [ "$sealed" = "false" ]; then
  echo "Vault is already unsealed - nothing to do."
  exit 0
fi

init_file="$(find_vault_init_file)"
mapfile -t keys < <(jq -r '.unseal_keys_b64[0:3][]' "$init_file")

kubectl exec -n vault vault-0 -- vault operator unseal "${keys[0]}"
kubectl exec -n vault vault-0 -- vault operator unseal "${keys[1]}"
kubectl exec -n vault vault-0 -- vault operator unseal "${keys[2]}"

kubectl exec -n vault vault-0 -- vault status
