# =============================================================================
# Pulse reverse proxy / gateway
#   - Serves the built React SPA from /usr/share/nginx/html
#   - Proxies /api/*  -> FastAPI incl. WebSocket upgrade
#   - Proxies /upload -> NiFi
# The nginx.conf / templates files are copied in so the image is self-
# contained (mountable over the top in docker-compose, or used as-is in K8s).
#
# nginx/templates/*.conf.template (not nginx/conf.d/*.conf directly) holds
# the server block, because it references the API/NiFi upstream hostnames,
# which differ between environments (Docker Compose's service names vs.
# Kubernetes' Service names - see API_UPSTREAM_HOST/NIFI_UPSTREAM_HOST
# below). The base nginx:stable-alpine image's own entrypoint (still active
# here - only CMD is overridden below, not ENTRYPOINT) auto-renders every
# /etc/nginx/templates/*.template file into /etc/nginx/conf.d/ via envsubst
# before nginx starts, substituting real environment variable values in for
# any ${VAR} placeholder it finds. nginx's OWN variables (like $host,
# $remote_addr) are untouched by this, since those aren't OS environment
# variables and envsubst only substitutes names it finds in the environment.
# =============================================================================

# ── Stage 1: Build the React frontend ────────────────────────────────────────
FROM node:25-alpine AS build

WORKDIR /app

COPY ./frontend/package*.json ./
RUN npm ci

COPY ./frontend/ ./

ARG VITE_API_URL
ARG VITE_NIFI_UPLOAD_URL
ENV VITE_API_URL=${VITE_API_URL:-/api}
ENV VITE_NIFI_UPLOAD_URL=${VITE_NIFI_UPLOAD_URL:-/upload}

RUN npm run build

# ── Stage 2: nginx gateway ───────────────────────────────────────────────────
FROM nginx:stable-alpine

# Remove default configs shipped with the image
RUN rm -f /etc/nginx/conf.d/default.conf

# React static assets
COPY --from=build /app/dist /usr/share/nginx/html

# nginx configs (main config + templated server blocks)
COPY ./nginx/nginx.conf /etc/nginx/nginx.conf
COPY ./nginx/templates/ /etc/nginx/templates/

# Defaults match docker-compose.yml's actual service names (`api`, `nifi`).
# deployment.yaml overrides both to the real Kubernetes Service names
# (pulse-api, pulse-nifi) for the pulse-nginx container - see the comment
# above and nginx/templates/upload.conf.template.
ENV API_UPSTREAM_HOST=api
ENV NIFI_UPSTREAM_HOST=nifi

EXPOSE 80 443

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
