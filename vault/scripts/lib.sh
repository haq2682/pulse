#!/bin/bash
# Shared helpers, sourced (not executed directly) by the numbered scripts.

# Checks the current directory, this scripts/ directory, then
# ~/.vault-pulse/ for vault-init-output.json.
find_vault_init_file() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  if [ -f vault-init-output.json ]; then
    echo "vault-init-output.json"
  elif [ -f "$script_dir/vault-init-output.json" ]; then
    echo "$script_dir/vault-init-output.json"
  elif [ -f "$HOME/.vault-pulse/vault-init-output.json" ]; then
    echo "$HOME/.vault-pulse/vault-init-output.json"
  else
    echo "vault-init-output.json not found in the current directory, $script_dir/, or ~/.vault-pulse/ - run 01-init-vault.sh first, or restore your copy from secure storage." >&2
    exit 1
  fi
}

# Vault CLI exits non-zero when sealed, not just on real failures - exit
# code deliberately not checked here so `set -e` callers don't die on that.
vault_status_json() {
  set +e
  kubectl exec -n vault vault-0 -- vault status -format=json 2>/dev/null
  set -e
}

# Token is piped over stdin, not passed as a CLI arg, so it never appears
# in `ps` output inside the pod or on this machine.
vault_login_as_root() {
  local init_file root_token
  init_file="$(find_vault_init_file)"
  root_token="$(jq -r '.root_token' "$init_file")"
  kubectl exec -i -n vault vault-0 -- vault login - <<< "$root_token" > /dev/null
}
