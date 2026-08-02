resource "kubernetes_namespace" "production" {
    metadata {
        labels = {
            name = "production"
        }
        name = "production"
    }
}

resource "kubernetes_namespace" "staging" {
    metadata {
        labels = {
            name = "staging"
        }
        name = "staging"
    }
}

resource "kubernetes_namespace" "argocd" {
    metadata {
        labels = {
            name = "argocd"
        }
        name = "argocd"
    }
}

resource "kubernetes_namespace" "monitoring" {
    metadata {
        labels = {
            name = "monitoring"
        }
        name = "monitoring"
    }
}

resource "kubernetes_namespace" "vault" {
    metadata {
        labels = {
            name = "vault"
        }
        name = "vault"
    }
}
