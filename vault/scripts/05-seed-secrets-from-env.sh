#!/bin/bash
# Reads .env and writes it into Vault, split into the same 5 logical groups
# already used for the Kubernetes Secrets deployment.yaml expects.
#
# A few keys the app needs aren't in every .env (this project's .env.example
# doesn't even list NIFI_SENSITIVE_PROPS_KEY - docker-compose.yml hardcodes
# it inline instead). Where a key is missing, this script generates one
# instead of failing, rather than blocking the whole seed on a value nobody
# was ever asked to set: AIRFLOW_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD, and
# NIFI_SENSITIVE_PROPS_KEY get a fresh random value; AIRFLOW_ADMIN_USER
# defaults to "admin". Rotate any of these later with `vault kv put`
# directly - see docs/SECRETS_MANAGEMENT.md.
#
# Logs in as root itself (see lib.sh) rather than assuming an earlier
# script's login is still cached in the vault-0 pod - that assumption
# breaks silently if the pod restarts in between. Must still be run from
# the repo root (or wherever your real .env lives) - that part isn't
# something this script can locate on its own the way vault-init-output.json
# can be (see lib.sh's find_vault_init_file), since there's no single
# documented "secure" location for .env to fall back to.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [ ! -f .env ]; then
  echo ".env not found in the current directory." >&2
  exit 1
fi

echo "Logging in to Vault ..."
vault_login_as_root

# Deliberately NOT `source .env` - that executes the file as bash, and .env
# values are free-form data, not code. A value like
# `FROM_EMAIL=Pulse Analytics Engine <someone@example.com>` is completely
# valid .env content (and is exactly what docker-compose's own env_file
# parser expects), but bash would try to interpret that unquoted `<` as
# input redirection and fail. Parsing line-by-line as plain KEY=VALUE data
# avoids that entirely.
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  key="${line%%=*}"
  [[ -z "$key" || "$key" == \#* ]] && continue
  # `IFS='=' read -r key value` (the previous approach here) silently drops
  # a trailing `=` from value - `read` treats IFS characters as separators
  # and discards a trailing empty field, exactly like it does trailing
  # whitespace. That's fatal for base64 values (Fernet keys, etc.), which
  # almost always end in `=` padding - splitting on the FIRST `=` only via
  # parameter expansion instead preserves the rest of the line verbatim,
  # trailing `=` included.
  value="${line#*=}"
  # Strip one matching pair of surrounding quotes, if present.
  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value="${value%\"}"
    value="${value#\"}"
  fi
  export "$key=$value"
done < .env
set +a

kubectl exec -n vault vault-0 -- vault kv put secret/pulse/postgresql \
  POSTGRES_USER="$POSTGRES_USER" \
  POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  POSTGRES_DB="$POSTGRES_DATABASE_NAME" \
  DEBEZIUM_PASSWORD="$DEBEZIUM_PASSWORD"

kubectl exec -n vault vault-0 -- vault kv put secret/pulse/minio \
  MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  MINIO_SECRET_KEY="$MINIO_SECRET_KEY"

kubectl exec -n vault vault-0 -- vault kv put secret/pulse/airflow \
  AIRFLOW_FERNET_KEY="$AIRFLOW_FERNET_KEY" \
  AIRFLOW_SECRET_KEY="${AIRFLOW_SECRET_KEY:-$(openssl rand -hex 32)}" \
  AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}" \
  AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-$(openssl rand -base64 18)}"

kubectl exec -n vault vault-0 -- vault kv put secret/pulse/api \
  SECRET_KEY="$SECRET_KEY" \
  GEMINI_API_KEY="$GEMINI_API_KEY" \
  GOOGLE_CLIENT_ID="$GOOGLE_CLIENT_ID" \
  GOOGLE_CLIENT_SECRET="$GOOGLE_CLIENT_SECRET" \
  SMTP_USER="$SMTP_USER" \
  SMTP_PASSWORD="$SMTP_PASSWORD"

kubectl exec -n vault vault-0 -- vault kv put secret/pulse/nifi \
  NIFI_ADMIN_USER="$NIFI_ADMIN_USER" \
  NIFI_ADMIN_PASSWORD="$NIFI_ADMIN_PASSWORD" \
  NIFI_SENSITIVE_PROPS_KEY="${NIFI_SENSITIVE_PROPS_KEY:-$(openssl rand -base64 24)}"

echo "Seeded secret/pulse/{postgresql,minio,airflow,api,nifi} from .env."
