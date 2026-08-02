resource "helm_release" "argocd" {
    name       = "argocd"
    repository = "https://argoproj.github.io/argo-helm"
    chart      = "argo-cd"
    namespace  = kubernetes_namespace.argocd.metadata[0].name
    version    = "10.1.4"

    # HTTPS-at-the-edge, same pattern already used by pulse-nginx/pulse-
    # ingress: TLS terminates at the ingress-nginx controller, and the
    # connection from there to argocd-server's own pod is plain HTTP -
    # never plain HTTP from a browser's point of view. This is Argo CD's
    # own documented "Option 2: SSL termination at the ingress controller"
    # (https://argo-cd.readthedocs.io/en/stable/operator-manual/ingress/) -
    # NOT the same as leaving TLS off. `server.insecure=true` tells
    # argocd-server itself to stop expecting to terminate TLS on its own
    # (it otherwise issues its own https redirect regardless of how the
    # request actually arrived, which loops forever behind a TLS-
    # terminating proxy - this was verified failing before this change).
    #
    # extraTls reuses the same wildcard pulse-tls Secret (*.pulse-engine.com)
    # already used by ingress.yaml - it must exist in THIS namespace too
    # (argocd), since a Secret referenced by an Ingress's tls block has to
    # live in the same namespace as the Ingress. See docs/INGRESS_ACCESS.md.
    #
    # No dedicated gRPC ingress (server.ingressGrpc) - the chart supports
    # one, but it requires a second hostname and a backend-protocol: GRPC
    # ingress-nginx annotation Kubernetes doesn't let you mix into this
    # same Ingress. The web UI doesn't need it (grpc-web works fine over
    # plain HTTPS), and the `argocd` CLI already has a working native-gRPC
    # path via the NodePort service below (`argocd login <node-ip>:30443`)
    # or `argocd login argocd.pulse-engine.com --grpc-web` through this
    # Ingress - so skipping it keeps this to one Ingress, one hostname, the
    # existing cert, matching the "avoid unnecessary complexity" approach
    # used everywhere else in this repo.
    set = [{
        name  = "server.service.type"
        value = "NodePort"
    }, {
        name = "server.ingress.enabled"
        value = "true"
    }, {
        name  = "server.ingress.ingressClassName"
        value = "nginx"
    }, {
        name  = "server.ingress.hostname"
        value = "argocd.pulse-engine.com"
    }, {
        name: "server.ingress.path"
        value: "/"
    }, {
        name: "server.ingress.pathType"
        value: "Prefix"
    }, {
        name  = "server.ingress.extraTls[0].hosts[0]"
        value = "argocd.pulse-engine.com"
    }, {
        name  = "server.ingress.extraTls[0].secretName"
        value = "pulse-tls"
    }, {
        # Both of these use a Helm map key that itself contains literal
        # dots (server.insecure is one key in argocd-cmd-params-cm, and
        # nginx.ingress.kubernetes.io/... is one annotation key) - the
        # backslash-escaped dots below are required so Terraform's helm
        # provider doesn't parse them as nested map paths instead.
        name  = "configs.params.server\\.insecure"
        value = "true"
    }, {
        name  = "server.ingress.annotations.nginx\\.ingress\\.kubernetes\\.io/force-ssl-redirect"
        value = "true"
    }, {
        name  = "server.ingress.annotations.nginx\\.ingress\\.kubernetes\\.io/backend-protocol"
        value = "HTTP"
    }]

    timeout = 3600
    wait = true
    atomic = true

    depends_on = [kubernetes_namespace.argocd]
}

resource "helm_release" "vault" {
    name       = "vault"
    repository = "https://helm.releases.hashicorp.com"
    chart      = "vault"
    namespace  = kubernetes_namespace.vault.metadata[0].name
    version    = "0.34.0"

    # Standalone mode (single replica, "file" storage backend on a PVC) -
    # the simplest beginner-friendly setup, not Vault's multi-node HA/Raft
    # mode. Vault still starts sealed and needs manual init + unseal - see
    # vault/scripts/ - that's inherent to Vault itself, not a mode choice.
    #
    # TLS - verified against the actual vault-helm v0.34.0 chart source
    # before writing this, not assumed:
    #   - global.tlsDisable=false alone does NOTHING to the listener - the
    #     chart's default server.standalone.config is a hardcoded raw
    #     string with tls_disable=1 baked in, not templated from
    #     global.tlsDisable. It DOES retemplate VAULT_ADDR/the readiness
    #     probe scheme to https though, so setting only the global flag
    #     creates an active protocol mismatch (https client vs http
    #     listener), not a harmless no-op. Both have to change together -
    #     see the full server.standalone.config override below.
    #   - That override REPLACES the chart's default config string
    #     wholesale (Helm `set` doesn't merge raw strings) - the storage
    #     "file" { path = "/vault/data" } stanza from the original default
    #     is preserved verbatim below. Dropping it is the single most
    #     dangerous mistake here: Vault would come up against the wrong
    #     (empty) storage location and every secret would appear gone.
    #   - server.extraVolumes mounts the vault-tls Secret (created directly
    #     with kubectl - bootstrap material, never committed, same
    #     treatment as pulse-tls - see docs/SECRETS_MANAGEMENT.md) at
    #     /vault/userconfig/vault-tls/, which is where the config below
    #     points tls_cert_file/tls_key_file.
    #   - server.extraEnvironmentVars sets VAULT_CACERT inside the
    #     container - without it, vault/scripts/*.sh (which run `vault`
    #     directly via `kubectl exec` into this pod) fail TLS verification
    #     against the self-signed cert forever. The chart's OWN readiness
    #     probe doesn't need this (it already runs `vault status -tls-
    #     skip-verify` by default), but our own scripts aren't using that
    #     flag, so they need real CA trust instead.
    set = [{
        name  = "server.standalone.enabled"
        value = "true"
    }, {
        name  = "server.dataStorage.storageClass"
        value = "pulse-storage"
    }, {
        name  = "global.tlsDisable"
        value = "false"
    }, {
        name  = "server.extraVolumes[0].type"
        value = "secret"
    }, {
        name  = "server.extraVolumes[0].name"
        value = "vault-tls"
    }, {
        name  = "server.extraEnvironmentVars.VAULT_CACERT"
        value = "/vault/userconfig/vault-tls/tls.crt"
    }, {
        name = "server.standalone.config"
        value = <<-EOT
          ui = true

          listener "tcp" {
            tls_disable = 0
            address = "[::]:8200"
            cluster_address = "[::]:8201"
            tls_cert_file = "/vault/userconfig/vault-tls/tls.crt"
            tls_key_file  = "/vault/userconfig/vault-tls/tls.key"
          }
          storage "file" {
            path = "/vault/data"
          }
        EOT
    }]

    timeout = 600
    wait    = true
    atomic  = true

    depends_on = [kubernetes_namespace.vault]
}

# The vault-helm chart's StatefulSet uses updateStrategyType: OnDelete (its
# own chart default, verified in values.yaml - not a setting made here) -
# meaning helm_release.vault above updates the StatefulSet's pod template
# on every apply, but the ALREADY-RUNNING vault-0 pod never actually picks
# up the new config until someone deletes it so the StatefulSet controller
# recreates it. This resource does that automatically instead of leaving it
# as a manual step: it re-runs (via the config_hash trigger) whenever the
# Vault helm values actually change, deleting vault-0 so it comes back with
# the new config.
#
# Deliberately does NOT also run vault/scripts/02-unseal-vault.sh here -
# on a genuinely first-ever apply, Vault isn't initialized yet at this
# point in the documented flow (vault/scripts/01 hasn't run), so an
# automatic unseal attempt here would fail and take the whole `terraform
# apply` down with it. Unsealing after this restart (like after any other
# Vault pod restart) is still a required manual step - see
# docs/SECRETS_MANAGEMENT.md.
resource "null_resource" "vault_tls_restart" {
    triggers = {
        config_hash = sha256(jsonencode(helm_release.vault.set))
    }

    depends_on = [helm_release.vault]

    provisioner "local-exec" {
        command = "kubectl delete pod vault-0 -n vault --ignore-not-found=true"
    }
}

resource "helm_release" "external_secrets" {
    name       = "external-secrets"
    repository = "https://charts.external-secrets.io"
    chart      = "external-secrets"
    namespace  = "kube-system"
    version    = "2.8.0"

    # Explicit name (rather than the chart's generated default) so
    # vault/scripts/04-apply-policies-and-roles.sh can bind a Vault
    # Kubernetes-auth role to a known identity.
    set = [{
        name  = "serviceAccount.name"
        value = "external-secrets"
    }]

    timeout = 600
    wait    = true
    atomic  = true
}

resource "helm_release" "argocd_image_updater" {
    name       = "argocd-image-updater"
    repository = "https://argoproj.github.io/argo-helm"
    chart      = "argocd-image-updater"
    namespace  = kubernetes_namespace.argocd.metadata[0].name
    version    = "0.11.2"

    timeout = 3600
    wait    = true
    atomic  = true

    depends_on = [helm_release.argocd]
}


resource "helm_release" "kube-prometheus-stack" {
    name       = "kube-prometheus-stack"
    repository = "https://prometheus-community.github.io/helm-charts"
    chart      = "kube-prometheus-stack"
    namespace  = kubernetes_namespace.monitoring.metadata[0].name
    version    = "87.17.0"

    set = [{
        name  = "prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues"
        value = "false"
    }, {
        name  = "prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues"
        value = "false"
    }, {
        name  = "prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues"
        value = "false"
    }]

    timeout = 3600
    wait = true
    atomic = true

    depends_on = [kubernetes_namespace.monitoring]
}
