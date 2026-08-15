#!/bin/bash
# Run once, after 03-enable-kv-and-k8s-auth.sh. Safe to re-run.
#
# Writes one read-only policy per secret, then a single Kubernetes-auth
# role bound to ESO's ServiceAccount holding all five policies. ESO is the
# only thing that authenticates to Vault directly.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

echo "Logging in to Vault ..."
vault_login_as_root

cd "$(dirname "$0")/.."   # vault/

for svc in postgresql minio airflow api nifi; do
  echo "Writing policy pulse-${svc}-read ..."
  kubectl cp "policies/pulse-${svc}-read.hcl" "vault/vault-0:/tmp/pulse-${svc}-read.hcl"
  kubectl exec -n vault vault-0 -- vault policy write "pulse-${svc}-read" "/tmp/pulse-${svc}-read.hcl"
done

# `audience` hardens this beyond bound_service_account_names/_namespaces:
# requires the JWT's own `aud` claim to match too. Single string field, not
# `bound_audiences` (looks plausible but doesn't exist on this auth backend).
echo "Writing the external-secrets Kubernetes-auth role ..."
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/role/external-secrets \
  bound_service_account_names=external-secrets \
  bound_service_account_namespaces=kube-system \
  audience=https://kubernetes.default.svc.cluster.local \
  policies=pulse-postgresql-read,pulse-minio-read,pulse-airflow-read,pulse-api-read,pulse-nifi-read \
  ttl=1h

echo "Done."
