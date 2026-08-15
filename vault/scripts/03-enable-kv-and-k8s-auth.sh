#!/bin/bash
# Run once, after Vault is initialized and unsealed. Safe to re-run - Vault
# reports "path is already in use" for anything already enabled.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

echo "Logging in to Vault ..."
vault_login_as_root

echo "Enabling KV v2 secrets engine at secret/ ..."
kubectl exec -n vault vault-0 -- vault secrets enable -path=secret kv-v2 || true

echo "Enabling the Kubernetes auth method ..."
kubectl exec -n vault vault-0 -- vault auth enable kubernetes || true

echo "Pointing it at this cluster's own API server ..."
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

echo "Done."
