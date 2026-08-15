# Pulse reverse proxy / gateway - serves the React SPA, proxies /api to
# FastAPI (incl. WebSocket upgrade) and /upload to NiFi. nginx/templates/
# holds the server block (not nginx/conf.d directly) since it references
# upstream hostnames that differ between Compose and Kubernetes - rendered
# via the base image's own envsubst entrypoint before nginx starts.
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

FROM nginx:stable-alpine

RUN rm -f /etc/nginx/conf.d/default.conf

COPY --from=build /app/dist /usr/share/nginx/html

COPY ./nginx/nginx.conf /etc/nginx/nginx.conf
COPY ./nginx/templates/ /etc/nginx/templates/

# Defaults match docker-compose's service names; deployment.yaml overrides
# both to the real Kubernetes Service names for the pulse-nginx container.
ENV API_UPSTREAM_HOST=api
ENV NIFI_UPSTREAM_HOST=nifi

EXPOSE 80 443

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
