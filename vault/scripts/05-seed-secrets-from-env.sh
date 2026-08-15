#!/bin/bash
# Reads .env and writes it into Vault, split into the same 5 logical groups
# used by deployment.yaml's Secrets. Must be run from the repo root (or
# wherever your real .env lives).
#
# A few keys aren't in every .env: AIRFLOW_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD,
# and NIFI_SENSITIVE_PROPS_KEY get a generated random value if missing;
# AIRFLOW_ADMIN_USER defaults to "admin". Rotate later with `vault kv put`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

if [ ! -f .env ]; then
  echo ".env not found in the current directory." >&2
  exit 1
fi

echo "Logging in to Vault ..."
vault_login_as_root

# Deliberately not `source .env` - .env values are free-form data (e.g.
# unquoted `<` in an email display name), not bash code.
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  key="${line%%=*}"
  [[ -z "$key" || "$key" == \#* ]] && continue
  # Parameter expansion (not `IFS='=' read`) preserves a trailing `=` -
  # fatal to drop for base64 values like Fernet keys.
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
