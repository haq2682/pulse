# Read-only access to exactly one KV v2 secret path. KV v2 stores data
# under "<mount>/data/<path>" internally - "secret/data/..." here, even
# though everywhere else (vault kv put/get, ExternalSecret remoteRef.key)
# just uses "secret/pulse/postgresql" without the literal "data/" segment.
path "secret/data/pulse/postgresql" {
  capabilities = ["read"]
}
