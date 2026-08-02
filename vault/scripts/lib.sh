#!/bin/bash
# Shared helpers, sourced (not executed directly) by the numbered scripts in
# this directory. Centralizes two things that used to require typing values
# in by hand: finding vault-init-output.json wherever it currently lives,
# and logging in as root.

# Prints the path to vault-init-output.json and exits 0, or prints an error
# to stderr and exits 1. Checks the current directory first (where
# 01-init-vault.sh writes it originally), then ~/.vault-pulse/ (the secure
# storage location docs/SECRETS_MANAGEMENT.md tells you to move it to
# afterward) - so this keeps working no matter which of the two you're
# running from.
find_vault_init_file() {
  if [ -f vault-init-output.json ]; then
    echo "vault-init-output.json"
  elif [ -f "$HOME/.vault-pulse/vault-init-output.json" ]; then
    echo "$HOME/.vault-pulse/vault-init-output.json"
  else
    echo "vault-init-output.json not found in the current directory or ~/.vault-pulse/ - run 01-init-vault.sh first, or restore your copy from secure storage." >&2
    exit 1
  fi
}

# Prints `vault status -format=json`'s output. Vault CLI exits non-zero
# whenever sealed (2) or erroring (1), not just on real failures - under
# `set -e` that would kill the calling script even though the JSON body is
# still perfectly valid, so the exit code is deliberately not checked here.
vault_status_json() {
  set +e
  kubectl exec -n vault vault-0 -- vault status -format=json 2>/dev/null
  set -e
}

# Logs the Vault CLI running inside the vault-0 pod in as root, using the
# token from vault-init-output.json. Every script that writes to Vault
# calls this itself rather than assuming a previous script's login is still
# cached in the pod - that assumption breaks silently the moment the pod
# restarts between two script runs.
#
# The token is piped over stdin (`vault login -` reads the token from
# stdin instead of argv), not passed as a CLI argument - a CLI argument
# would be visible to anyone who can `kubectl exec`/inspect the process
# list inside vault-0 (`ps` shows a process's full argv), even without
# filesystem access to vault-init-output.json itself. `<<<` (a herestring)
# is used rather than `echo "$root_token" | kubectl exec ...` so the token
# never appears as an argv on THIS machine either - echo would be its own
# process with the token as its argument.
vault_login_as_root() {
  local init_file root_token
  init_file="$(find_vault_init_file)"
  root_token="$(jq -r '.root_token' "$init_file")"
  kubectl exec -i -n vault vault-0 -- vault login - <<< "$root_token" > /dev/null
}
