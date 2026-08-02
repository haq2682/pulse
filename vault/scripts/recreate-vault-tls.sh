#!/bin/bash
# Run this once, right after `terraform apply -target=kubernetes_namespace.vault`
# (README step 2), any time you're standing the cluster back up after a
# `terraform destroy` - and before the full `terraform apply` that installs
# Vault's Helm release.
#
# Why this is a separate manual step at all: Vault's pod mounts a Secret
# named vault-tls unconditionally, but nothing creates that Secret
# automatically - it was deliberately never made a Terraform resource,
# because doing so would put the TLS private key in .tfstate (see
# docs/SECRETS_MANAGEMENT.md's TLS section). `terraform destroy` deletes
# the `vault` namespace, which cascades to delete everything inside it
# INCLUDING this Secret, even though Terraform never created it and has no
# record of it in state. So every destroy+apply cycle needs this re-run -
# there is no way to make `terraform apply` alone bring it back without
# giving up the "key material never touches Terraform state" property.
#
# Reuses your existing vault.crt/vault.key if you still have them (checked
# in the current directory, then ~/.vault-pulse/ - the same secure-storage
# location docs/SECRETS_MANAGEMENT.md has you move them to). Only generates
# a brand new self-signed keypair if neither is found; there's no need to
# rotate this cert on every recreate, unlike the Vault unseal
# keys/root token, which really do change on a genuine re-init.
#
# Safe to re-run - uses `apply`, not `create`, so running it again against
# an already-current Secret is a no-op.
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
