#!/bin/bash
# Run once, after 03-enable-kv-and-k8s-auth.sh. Safe to re-run - policies
# and roles are just overwritten with the same content.
#
# Writes one read-only policy per secret, then a single Kubernetes-auth
# role bound to the External Secrets Operator's own ServiceAccount, holding
# all five policies. ESO is the only thing that ever authenticates to
# Vault directly - individual pulse-* pods never do, they just read the
# plain Kubernetes Secrets ESO creates.
#
# Logs in as root itself (see lib.sh) rather than assuming
# 03-enable-kv-and-k8s-auth.sh's login is still cached in the vault-0 pod -
# that assumption breaks silently if the pod restarts in between.
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

# `audience` hardens this beyond bound_service_account_names/_namespaces
# alone: it requires the presented JWT's own `aud` claim to match too, so
# a token that's merely FOR the right ServiceAccount but issued for a
# different audience (e.g. a token minted for some other in-cluster
# consumer) is rejected. This is a single string field, per `vault path-
# help auth/kubernetes/role/<name>` on this Vault version - NOT
# `bound_audiences` (a plausible-looking name that doesn't actually exist
# on this auth backend and is silently ignored by `vault write` rather
# than rejected outright). The value below is this cluster's real
# ServiceAccount token audience - confirmed by actually reading a live
# token minted for the external-secrets ServiceAccount and decoding its
# `aud` claim, not assumed. Vault warns (not errors) if this is left
# unset, which was this script's previous behavior.
echo "Writing the external-secrets Kubernetes-auth role ..."
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/role/external-secrets \
  bound_service_account_names=external-secrets \
  bound_service_account_namespaces=kube-system \
  audience=https://kubernetes.default.svc.cluster.local \
  policies=pulse-postgresql-read,pulse-minio-read,pulse-airflow-read,pulse-api-read,pulse-nifi-read \
  ttl=1h

echo "Done."
