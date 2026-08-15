#!/bin/bash
# Run after every Vault pod restart - unsealing is runtime state, not
# persisted. Safe to re-run; no-op if already unsealed.
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
