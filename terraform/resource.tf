resource "helm_release" "argocd" {
    name       = "argocd"
    repository = "https://argoproj.github.io/argo-helm"
    chart      = "argo-cd"
    namespace  = kubernetes_namespace.argocd.metadata[0].name
    version    = "10.1.4"

    # TLS terminates at ingress-nginx; server.insecure=true stops
    # argocd-server from also issuing its own https redirect (avoids a
    # redirect loop). extraTls secret must live in this namespace too.
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
        # Escaped dots: these are literal keys, not nested map paths.
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

    # Standalone mode: single replica, "file" storage backend on a PVC,
    # not multi-node HA/Raft. Starts sealed - see vault/scripts/.
    #
    # server.standalone.config below replaces the chart's default config
    # wholesale (Helm `set` doesn't merge raw strings) - keep the
    # storage "file" stanza or Vault loses its data path. VAULT_CACERT
    # lets vault/scripts/*.sh trust the self-signed cert via kubectl exec.
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

# vault-helm's StatefulSet uses updateStrategyType: OnDelete, so config
# changes alone never restart vault-0 - this deletes it so the new config
# is picked up. Does not auto-unseal; that stays a manual step (vault/scripts/02).
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
