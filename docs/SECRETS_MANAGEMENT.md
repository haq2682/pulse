# Secrets management: HashiCorp Vault + External Secrets Operator

How credentials get from your `.env` file into running pods, without ever
being stored in git at all.

## Flow

```
.env (local only, already gitignored)
  -> vault/scripts/05-seed-secrets-from-env.sh
  -> vault kv put secret/pulse/<service>   (stored inside Vault's own PVC)
  -> ExternalSecret object (plain YAML, safe to commit - contains no
     values, just a pointer: "sync secret/pulse/postgresql -> a Secret
     named pulse-postgresql-secret")
  -> ArgoCD applies the ExternalSecret like any other manifest
  -> External Secrets Operator (running in-cluster) reads it, authenticates
     to Vault via its own Kubernetes ServiceAccount, fetches the data
  -> a real Kubernetes Secret appears, named to match
  -> pods read it via envFrom/secretKeyRef, same as any Secret
```

The key property, different from the project's earlier Sealed-Secrets
approach: **secret values never touch git, in any form, encrypted or
not.** Only pointers (`ExternalSecret` objects) live in the repo. Pods
never talk to Vault directly - only ESO does.

## What's installed

- **Vault** - `terraform/resource.tf` (`helm_release.vault`), chart
  `hashicorp/vault` 0.34.0, namespace `vault`. Running in **standalone
  mode**: single replica, "file" storage backend on a PVC
  (`pulse-storage`, 10Gi) - not Vault's multi-node Raft/HA mode. This is
  the simplest correct setup for a single-node cluster; on a multi-node
  production cluster you'd want Raft storage across 3+ Vault pods instead.
  TLS-enabled - see [TLS](#tls) below.
- **External Secrets Operator (ESO)** - `terraform/resource.tf`
  (`helm_release.external_secrets`), chart
  `external-secrets/external-secrets` 2.8.0, namespace `kube-system`,
  ServiceAccount explicitly named `external-secrets` (so Vault's
  Kubernetes-auth role has a fixed identity to bind to).
- **`vault/policies/`** - one HCL file per secret, each granting read-only
  access to exactly one KV v2 path.
- **`vault/scripts/`** - the one-time/as-needed bootstrap scripts, run in
  order (01 → 05) on a fresh cluster. All already run once on this cluster.

## The five secrets

| Secret name | Vault path | Keys |
|---|---|---|
| `pulse-postgresql-secret` | `secret/pulse/postgresql` | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DEBEZIUM_PASSWORD` |
| `pulse-minio-secret` | `secret/pulse/minio` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` |
| `pulse-airflow-secret` | `secret/pulse/airflow` | `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` |
| `pulse-api-secret` | `secret/pulse/api` | `SECRET_KEY`, `GEMINI_API_KEY`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `SMTP_USER`, `SMTP_PASSWORD` |
| `pulse-nifi-secret` | `secret/pulse/nifi` | `NIFI_ADMIN_USER`, `NIFI_ADMIN_PASSWORD`, `NIFI_SENSITIVE_PROPS_KEY` |

All five were seeded from the project's real local `.env`. Four keys aren't
present in that `.env` (`AIRFLOW_SECRET_KEY`, `AIRFLOW_ADMIN_USER`,
`AIRFLOW_ADMIN_PASSWORD`, `NIFI_SENSITIVE_PROPS_KEY` - `.env.example`
doesn't list the NiFi one either; `docker-compose.yml` hardcodes it inline
instead) - `05-seed-secrets-from-env.sh` auto-generated those rather than
failing the whole seed over values nobody was ever asked to set. Rotate any
of them the normal way below whenever you have a real value.

## How Vault auth works

- Vault's **Kubernetes auth method** is enabled and configured to trust
  this cluster's own API server (`03-enable-kv-and-k8s-auth.sh`).
- A single Vault role, `external-secrets`, is bound to the
  `external-secrets` ServiceAccount in `kube-system` and holds all 5
  read-only policies (`04-apply-policies-and-roles.sh`). ESO is the only
  identity that ever authenticates to Vault - individual `pulse-*` pods
  never do; they just read the plain Kubernetes Secrets ESO creates.
- `.k8s/bases/secrets/vault-cluster-secret-store.yaml` is the
  `ClusterSecretStore` that ties this together - it's what every
  `ExternalSecret` in this directory references via `secretStoreRef`. It
  connects to `https://vault.vault.svc:8200`, trusting Vault's cert via
  `caProvider` (see [TLS](#tls) below) - ESO can already read that Secret
  cluster-wide without any extra RBAC, verified against the live
  `external-secrets-controller` ClusterRole rather than assumed.
- `04-apply-policies-and-roles.sh` also sets `audience` on the
  `external-secrets` role (Vault's kubernetes auth method rejects a token
  whose own `aud` claim doesn't match, on top of the ServiceAccount name/
  namespace check) - confirmed against a real token minted for that exact
  ServiceAccount, not guessed. This is additional hardening, not something
  that was broken before; Vault only warned about its absence.

## TLS

Vault's listener runs with `tls_disable = 0` - every connection to it,
including from ESO and from `vault/scripts/*.sh`, is HTTPS. Implementing
this correctly required overriding several vault-helm chart defaults that
aren't obvious from the chart's own docs - see the comment on
`helm_release.vault` in `terraform/resource.tf` for the specifics (the
short version: `global.tlsDisable=false` alone changes nothing on its own
and even actively breaks things, since `VAULT_ADDR` re-templates to
`https://` while the listener itself keeps speaking plain HTTP unless
`server.standalone.config` is also overridden - and that override has to
carry the original `storage "file"` stanza forward verbatim, or Vault
starts against the wrong location and every existing secret appears gone).

**The certificate** - self-signed, SANs covering every DNS name Vault's own
pod, ESO, and `vault/scripts/*.sh` actually use (`vault`, `vault.vault.svc`,
`vault.vault.svc.cluster.local`, `vault-internal` + its pod-DNS form,
`localhost`, `127.0.0.1`). Loaded directly into the cluster as a Secret
named `vault-tls` in the `vault` namespace - bootstrap material like
`pulse-tls`, **not committed anywhere in this repo**. A copy of the
key/cert lives at `~/.vault-pulse/vault.key` / `vault.crt` on this machine
(`chmod 600` on the key).

Regenerate and reload at any time:

```bash
openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
  -keyout vault.key -out vault.crt \
  -subj "/CN=vault.vault.svc.cluster.local/O=pulse-dev" \
  -addext "subjectAltName=DNS:vault,DNS:vault.vault,DNS:vault.vault.svc,DNS:vault.vault.svc.cluster.local,DNS:vault-internal,DNS:vault-internal.vault,DNS:vault-internal.vault.svc,DNS:vault-internal.vault.svc.cluster.local,DNS:vault-0.vault-internal,DNS:vault-0.vault-internal.vault.svc.cluster.local,DNS:localhost,IP:127.0.0.1"

kubectl delete secret vault-tls -n vault
kubectl create secret tls vault-tls -n vault --cert=vault.crt --key=vault.key
kubectl delete pod vault-0 -n vault   # picks up the new cert - see below
./vault/scripts/02-unseal-vault.sh    # every pod restart reseals Vault
```

`ClusterSecretStore`'s `caProvider` reads the same live Secret directly -
no separate copy to keep in sync.

**The `OnDelete` gotcha** - the vault-helm chart's StatefulSet uses
`updateStrategyType: OnDelete` (the chart's own default, not something set
in this repo), meaning a `helm upgrade`/`terraform apply` that changes
Vault's config updates the StatefulSet's pod template but never restarts
the already-running pod - it silently keeps running the OLD config
indefinitely until something deletes it. `terraform/resource.tf`'s
`null_resource.vault_tls_restart` automates this: it re-runs `kubectl
delete pod vault-0` automatically whenever the Vault helm values actually
change (tracked via a hash of them), so `terraform apply` alone is enough -
no manual pod deletion needed for a config change made through Terraform.
It does **not** also auto-unseal afterward - see the next point.

**Still true regardless of TLS**: Vault starts sealed on every pod restart
(TLS-triggered or otherwise) - see [Unseal key custody](#unseal-key-custody---read-this-before-anything-else)
below. `null_resource.vault_tls_restart` deliberately doesn't chase this
with an automatic unseal call, because on a genuinely first-ever
`terraform apply`, Vault isn't initialized yet at that point in the
documented flow (`vault/scripts/01` runs afterward) - an automatic unseal
attempt there would fail and fail the whole `terraform apply` with it.

## Rotating a secret

No re-encryption, no commit, no push. Just write the new value directly:

```bash
kubectl exec -n vault vault-0 -- vault kv put secret/pulse/nifi \
  NIFI_ADMIN_USER=admin \
  NIFI_ADMIN_PASSWORD=<new-password> \
  NIFI_SENSITIVE_PROPS_KEY=<existing-or-new-value>
```

(Like before: `vault kv put` replaces the *entire* value at that path, so
include every key, not just the one changing.) The corresponding
`ExternalSecret` picks it up automatically within `refreshInterval` (set to
`1h` on all five) - no GitOps cycle involved at all. This is the headline
difference from the Sealed Secrets approach this project used before:
rotation there required re-sealing, committing, and pushing a file; here it
never touches git.

## Unseal key custody - read this before anything else

`vault operator init` (already run once, via `01-init-vault.sh`) produced 5
unseal keys (3 needed to unseal) and a root token, written to
`vault-init-output.json` and immediately moved to
`~/.vault-pulse/vault-init-output.json` (outside the repo, `chmod 600`).
**Move this to real secure storage (password manager, offline vault) and
delete the local copy.** Anyone holding 3 of the 5 keys, or the root
token, has full read access to every secret Vault will ever hold - the
same severity as the Age private key was for this project's earlier SOPS
attempt, or the sealed-secrets controller's private key for the attempt
after that.

Vault starts **sealed** every time its pod restarts (crash, node reboot,
`minikube stop`/`start`, a Helm upgrade that recreates the pod) - unsealing
is runtime state, not persisted. Run `vault/scripts/02-unseal-vault.sh`
after every restart - it reads its 3 keys from `vault-init-output.json`
itself (see `vault/scripts/lib.sh`), nothing is typed in by hand. Nothing
new can start reading secrets from Vault (existing already-synced Secrets
keep working) until that happens. There is no auto-unseal configured (e.g.
a cloud KMS transit seal) - this is a deliberate simplicity tradeoff for a
single-node standalone Vault, not an oversight, but it does mean this
manual step is required after every restart with no alternative.

## Reproducing on a fresh cluster

```bash
kubectl apply -f .k8s/bases/storageclass.yaml   # Vault's PVC needs this to exist

# Namespace first, via Terraform itself (not `kubectl create namespace` -
# that would conflict with Terraform's own state for kubernetes_namespace.vault).
cd terraform
terraform apply -target=kubernetes_namespace.vault
cd ..

# The vault-tls Secret has to exist BEFORE the Vault pod ever starts -
# server.extraVolumes mounts it unconditionally, see terraform/resource.tf.
openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
  -keyout vault.key -out vault.crt \
  -subj "/CN=vault.vault.svc.cluster.local/O=pulse-dev" \
  -addext "subjectAltName=DNS:vault,DNS:vault.vault,DNS:vault.vault.svc,DNS:vault.vault.svc.cluster.local,DNS:vault-internal,DNS:vault-internal.vault,DNS:vault-internal.vault.svc,DNS:vault-internal.vault.svc.cluster.local,DNS:vault-0.vault-internal,DNS:vault-0.vault-internal.vault.svc.cluster.local,DNS:localhost,IP:127.0.0.1"
kubectl create secret tls vault-tls -n vault --cert=vault.crt --key=vault.key

cd terraform
terraform apply -target=helm_release.vault -target=null_resource.vault_tls_restart -target=helm_release.external_secrets
cd ..
./vault/scripts/01-init-vault.sh
./vault/scripts/02-unseal-vault.sh
./vault/scripts/03-enable-kv-and-k8s-auth.sh
./vault/scripts/04-apply-policies-and-roles.sh
./vault/scripts/05-seed-secrets-from-env.sh
```

No keys, tokens, or `vault login` calls need typing in by hand - every
script after `01` reads `vault-init-output.json` itself (checking the
current directory first, then `~/.vault-pulse/`, via `vault/scripts/lib.sh`)
and logs in as root on its own. Each script is also safe to re-run: `01`
and `02` no-op if Vault is already initialized/unsealed, and `03`/`04` just
overwrite existing config/policies with the same content.

A new cluster means a brand-new Vault keypair - `vault-init-output.json`
from a different cluster will not unseal this one, and vice versa.

## No CI-driven resealing this time

The project's earlier Sealed Secrets iteration had a
`.github/workflows/reseal-secrets.yaml` that re-encrypted a secret and
pushed the result to git on a manual trigger. That pattern doesn't apply
here at all - rotating a Vault-backed secret is a direct `vault kv put`
against the running cluster (see above), which by design never touches
git. No workflow was built for this iteration; there's nothing for one to
do.

## Known gaps / not yet done

- **Single Vault replica, no HA.** Standalone mode is explicitly not
  scalable past one replica. A production multi-node cluster would want
  Vault's Raft storage backend across 3+ pods instead.
- **No auto-unseal.** Every Vault pod restart requires a manual
  `vault/scripts/02-unseal-vault.sh` run (automated in the sense that it
  needs no keys typed in by hand, but someone/something still has to
  trigger it) - see [TLS](#tls) above and [Unseal key
  custody](#unseal-key-custody---read-this-before-anything-else) below. A
  cloud KMS transit seal would remove this step entirely but adds a real
  external dependency this local/learning setup doesn't otherwise have.
