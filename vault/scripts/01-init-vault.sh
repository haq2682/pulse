#!/bin/bash
# Safe to re-run: if Vault is already initialized, this is a no-op. Never
# re-initializes existing data automatically - a second `vault operator
# init` against already-initialized storage would mint a brand-new root
# token/unseal keys for data that's encrypted under the OLD ones, which
# permanently locks it out. If Vault reports initialized=true but you have
# no vault-init-output.json anywhere (current directory or ~/.vault-pulse/),
# that data is unrecoverable - there is no safe automated fix for that.
#
# Writes vault-init-output.json in the current directory, containing 5
# unseal keys and the root token, only on a genuine first run. .gitignore
# already excludes this file, but treat that as a backstop, not the plan -
# move it out of this directory into a password manager or offline vault
# immediately (or ~/.vault-pulse/, which every other script here also knows
# to check), then delete the local copy.
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
