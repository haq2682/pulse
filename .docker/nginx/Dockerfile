# =============================================================================
# Pulse reverse proxy / gateway
#   - Serves the built React SPA from /usr/share/nginx/html
#   - Proxies /api/*  -> FastAPI (api:8000) incl. WebSocket upgrade
#   - Proxies /upload -> NiFi
# The nginx.conf / conf.d files are copied in so the image is self-contained
# (mountable over the top in docker-compose, or used as-is in K8s).
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

# nginx configs (main + server blocks)
COPY ./nginx/nginx.conf /etc/nginx/nginx.conf
COPY ./nginx/conf.d/ /etc/nginx/conf.d/

EXPOSE 80 443

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
