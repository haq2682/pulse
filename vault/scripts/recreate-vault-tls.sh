#!/bin/bash
# Run once, right after `terraform apply -target=kubernetes_namespace.vault`
# and before the full `terraform apply` that installs Vault's Helm release -
# any time the cluster comes back up after a `terraform destroy`.
#
# vault-tls is never a Terraform resource (that would put the private key
# in .tfstate), so `terraform destroy` deletes it along with the `vault`
# namespace with no way for `terraform apply` alone to recreate it.
#
# Reuses an existing vault.crt/vault.key if found (current directory, then
# ~/.vault-pulse/); only generates a new keypair if neither exists.
set -euo pipefail

VAULT_DIR="$HOME/.vault-pulse"

if ! kubectl get namespace vault >/dev/null 2>&1; then
  echo "Namespace 'vault' doesn't exist yet - run 'terraform apply -target=kubernetes_namespace.vault' first." >&2
  exit 1
fi

if [ -f vault.crt ] && [ -f vault.key ]; then
  cert_dir="."
elif [ -f "$VAULT_DIR/vault.crt" ] && [ -f "$VAULT_DIR/vault.key" ]; then
  cert_dir="$VAULT_DIR"
else
  echo "No existing vault.crt/vault.key found (checked ./ and $VAULT_DIR/) - generating a new self-signed cert ..."
  mkdir -p "$VAULT_DIR"
  openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
    -keyout "$VAULT_DIR/vault.key" -out "$VAULT_DIR/vault.crt" \
    -subj "/CN=vault.vault.svc.cluster.local/O=pulse-dev" \
    -addext "subjectAltName=DNS:vault,DNS:vault.vault,DNS:vault.vault.svc,DNS:vault.vault.svc.cluster.local,DNS:vault-internal,DNS:vault-internal.vault,DNS:vault-internal.vault.svc,DNS:vault-internal.vault.svc.cluster.local,DNS:vault-0.vault-internal,DNS:vault-0.vault-internal.vault.svc.cluster.local,DNS:localhost,IP:127.0.0.1"
  chmod 600 "$VAULT_DIR/vault.key"
  cert_dir="$VAULT_DIR"
  echo "Wrote $VAULT_DIR/vault.crt and $VAULT_DIR/vault.key (move these out of the repo directory if they ended up in it)."
fi

echo "Using cert/key from $cert_dir/"
echo "Creating/updating the vault-tls Secret in the vault namespace ..."
kubectl create secret tls vault-tls -n vault \
  --cert="$cert_dir/vault.crt" --key="$cert_dir/vault.key" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Done. Now run the full 'terraform apply' (README step 2) if you haven't yet."
echo "If vault-0 already existed and was stuck in ContainerCreating waiting on"
echo "this Secret, it should proceed to Running (sealed) within a few seconds -"
echo "check with: kubectl get pod vault-0 -n vault"
