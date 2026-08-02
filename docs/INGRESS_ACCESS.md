# Browser and DB-client access on minikube

How to actually reach the web UIs and Postgres from your host machine, and
why Kafka isn't part of this.

## 1. Add these to `/etc/hosts`

Not done automatically — add these lines yourself (`sudo nano /etc/hosts`
or equivalent):

```
192.168.49.2  pulse-engine.com
192.168.49.2  spark.pulse-engine.com
192.168.49.2  nifi.pulse-engine.com
192.168.49.2  airflow.pulse-engine.com
192.168.49.2  minio.pulse-engine.com
192.168.49.2  argocd.pulse-engine.com
```

`192.168.49.2` is this minikube profile's node IP (`minikube ip`). It's
stable across reboots but **will change if you ever delete and recreate
the minikube cluster** (`minikube delete && minikube start`) - re-run
`minikube ip` and update `/etc/hosts` if that happens.

## 2. The URLs (note the port numbers)

This cluster's ingress-nginx controller is a `NodePort` Service, not a
`LoadBalancer` - confirmed by checking it directly
(`kubectl get svc -n ingress-nginx`), not assumed. That means it listens on
high, randomly-assigned-once ports rather than the plain 443/80 you'd
normally expect, so the port number in the URL isn't optional:

```
https://pulse-engine.com:31775/          - main app (nginx gateway → frontend + /api + /upload)
https://spark.pulse-engine.com:31775/    - Spark Master dashboard
https://nifi.pulse-engine.com:31775/     - NiFi
https://airflow.pulse-engine.com:31775/  - Airflow UI
https://minio.pulse-engine.com:31775/    - MinIO console
https://argocd.pulse-engine.com:31775/   - ArgoCD UI
```

### ArgoCD - a second Ingress, in a different namespace

Unlike everything else above (all defined in `ingress.yaml`, one Ingress in
`production`), ArgoCD's Ingress is created by its own Helm chart (see
`terraform/resource.tf`'s `helm_release.argocd`) in the `argocd` namespace.
A Secret's `tls.secretName` reference only ever resolves within its own
namespace, so the same `pulse-tls` cert/key pair had to be loaded as a
**second, separate Secret object** in `argocd`, not just `production`:

```bash
kubectl create secret tls pulse-tls -n argocd \
  --cert=~/.pulse-tls/pulse-engine.crt --key=~/.pulse-tls/pulse-engine.key
```

argocd-server runs with `server.insecure=true` (TLS terminates at
ingress-nginx, same as everywhere else in this repo - see the comment on
`helm_release.argocd`), so plain `http://argocd.pulse-engine.com:31911/`
now correctly 308-redirects to the `:31775` HTTPS URL above instead of
looping forever (its pre-fix behavior, verified both ways with `curl`).

The `argocd` CLI's native gRPC protocol doesn't go through this Ingress
(only a browser's HTTP/gRPC-Web traffic does) - use one of:

```bash
argocd login argocd.pulse-engine.com:31775 --grpc-web   # via this Ingress
argocd login $(minikube ip):30443 --insecure             # direct NodePort, native gRPC
```

Your browser will show a certificate warning on first visit to each host -
expected, this is a self-signed certificate (see below), not a sign
anything is wrong. Click through it ("Advanced" → "Proceed").

**Want plain `https://pulse-engine.com` with no port number?** That needs
the ingress controller to be a `LoadBalancer` instead of `NodePort`, plus
`minikube tunnel` running in a spare terminal the whole time you're using
it:

```bash
kubectl patch svc ingress-nginx-controller -n ingress-nginx -p '{"spec":{"type":"LoadBalancer"}}'
minikube tunnel   # leave this running; asks for your sudo password once
```

This wasn't done for you - patching an addon-managed Service is a step
with its own trade-off (revert with the same command but `"type":"NodePort"`
if the `ingress` addon ever gets disabled/re-enabled and you want it back
to default), so it's here as an option rather than applied.

### Until the app is actually deployed, these all return `503`

`kustomization.yaml`'s `images:` transformer now points at the real
`haq2682/pulse-*` Docker Hub repositories (no more
`REPLACE_ME_DOCKERHUB_USERNAME` placeholder), but that alone doesn't put
anything in the cluster - the Services these hosts route to (`pulse-nginx`,
`pulse-spark-master`, etc.) won't exist until `.k8s/bases` has actually been
pushed to GitHub and synced in by ArgoCD (see the README's DevOps Pipeline
Setup section, steps 6-7), and the images themselves won't exist on Docker
Hub until CI has run at least once. The TLS handshake and host-based
routing were verified end-to-end with `curl` against the live ingress
controller; a `503` from nginx is the correct, expected response when it
can't find a backend - not a misconfiguration. These URLs will start
returning real content the moment the app is actually synced in.

## 3. Postgres - DBeaver / Beekeeper Studio

Ingress can't help here - it's an HTTP(S)-only mechanism, and Postgres
speaks its own binary wire protocol, not HTTP. It's exposed instead via a
second Service, `pulse-postgresql-nodeport` (same pods as the internal
`pulse-postgresql` Service, different Service/exposure - see `svc.yaml`),
already applied and live:

```
Host:     192.168.49.2   (minikube ip)
Port:     30432
Database: whatever POSTGRES_DB is set to in pulse-postgresql-secret (default: "pulse")
Username: whatever POSTGRES_USER is set to (default: "postgres")
Password: whatever POSTGRES_PASSWORD is set to
```

(See `docs/SECRETS_MANAGEMENT.md` for how to read the actual current values
out of Vault: `kubectl exec -n vault vault-0 -- vault kv get secret/pulse/postgresql`.)

This will only actually connect once the `pulse-postgresql` Deployment
itself is running - same "app not deployed yet" caveat as above.

## 4. Why Kafka isn't exposed here

Kafka's client protocol is also not HTTP - same fundamental reason
Postgres can't go through Ingress. Unlike Postgres, though, a plain
NodePort doesn't fully solve it either: Kafka clients don't just connect to
one bootstrap address and stay there - after the initial connection, the
broker tells the client its own *advertised* address for every subsequent
connection (including from producers/consumers that first discovered it
through a completely different address), and `pulse-kafka`'s
`KAFKA_ADVERTISED_LISTENERS` is currently set to the internal
`pulse-kafka:9092` cluster address (correct for every in-cluster caller).
An external client would get told to reconnect to `pulse-kafka:9092`,
which means nothing outside the cluster - so it would fail after the first
handshake even with a NodePort in place.

Properly exposing Kafka externally means giving it a **second listener**
with an advertised address your host can actually reach (typically
`EXTERNAL://<nodeIP>:<nodePort>` alongside the existing internal one), which
is a real, well-documented pattern but a genuine config change to
`pulse-kafka`'s Deployment - not something that fits as a side-effect of
this Ingress file. Flagging it rather than doing it as a surprise: say the
word and it's a scoped follow-up.

If what you actually want is to *browse* Kafka topics/messages (rather than
point a native Kafka client at it), a web-based Kafka UI (e.g. Provectus
`kafka-ui` or Kafdrop) deployed as its own service would be genuinely
Ingress-compatible, since it talks HTTP to your browser and Kafka's native
protocol internally on your behalf - that's a new component, not part of
the existing stack, so also left as a follow-up rather than added here.

## 5. The TLS certificate

Self-signed, covering `pulse-engine.com` and `*.pulse-engine.com` (one cert
for every host above), generated locally and loaded directly into the
cluster as a Secret named `pulse-tls` - **not committed anywhere in this
repo**, including the private key, which also isn't stored under version
control. A copy lives at `~/.pulse-tls/` on this machine (`chmod 600` on the
key) if you need to inspect or reuse it.

The Ingress (`pulse-ingress` in `ingress.yaml`) now lives in the
`production` namespace along with everything else in `.k8s/bases/` - a
Secret's `secretName` reference in an Ingress's `tls:` block must resolve
to a Secret in that **same namespace**, so `pulse-tls` needs to exist in
`production`, not `default`.

Regenerate and reload at any time:

```bash
openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
  -keyout pulse-engine.key -out pulse-engine.crt \
  -subj "/CN=pulse-engine.com/O=pulse-dev" \
  -addext "subjectAltName=DNS:pulse-engine.com,DNS:*.pulse-engine.com"

kubectl delete secret pulse-tls -n production
kubectl create secret tls pulse-tls -n production --cert=pulse-engine.crt --key=pulse-engine.key
```

(`-n production` assumes the namespace already exists - either applied
manually, or created automatically by ArgoCD's `CreateNamespace=true` sync
option, see `.k8s/argocd/argocd.yaml`. This Secret is bootstrap material
like the Vault unseal keys - it's loaded directly into the cluster with
`kubectl`, not committed to git, so ArgoCD never manages it.)

If you ever point this at a real, publicly-resolvable domain instead of a
local `/etc/hosts` entry, replace this with a real certificate (Let's
Encrypt via cert-manager, or any CA) - self-signed only works because
you're manually telling your own browser to trust it; nobody else's
browser will.
