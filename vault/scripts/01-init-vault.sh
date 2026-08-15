#!/bin/bash
# Safe to re-run: no-op if already initialized. Never re-initializes
# existing data - a second `vault operator init` against initialized
# storage mints new keys for data encrypted under the old ones, locking it
# out permanently. Writes vault-init-output.json (5 unseal keys + root
# token) only on first run - move it to secure storage immediately.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

initialized="$(vault_status_json | jq -r 'if .initialized then "true" else "false" end')"

if [ "$initialized" = "true" ]; then
  echo "Vault is already initialized - nothing to do."
  exit 0
fi

kubectl exec -n vault vault-0 -- vault operator init \
  -key-shares=5 \
  -key-threshold=3 \
  -format=json > vault-init-output.json

echo "Wrote vault-init-output.json - 5 unseal keys + the root token."
echo "Move it out of this directory to secure storage now, then delete the local copy."
