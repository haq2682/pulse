terraform {
    required_version = ">= 1.5.0"
    required_providers {
        kubernetes = {
        source  = "hashicorp/kubernetes"
        version = "~> 2.30"
        }
        helm = {
        source  = "hashicorp/helm"
        version = "~> 3.0"
        }
        # Used by null_resource.vault_tls_restart in resource.tf - the
        # vault-helm chart's StatefulSet uses updateStrategyType: OnDelete,
        # so a config change alone never restarts the running pod; this
        # provider is what lets Terraform run that kubectl delete for you.
        null = {
        source  = "hashicorp/null"
        version = "~> 3.2"
        }
    }
}