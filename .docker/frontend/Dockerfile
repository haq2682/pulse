# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM node:25-alpine AS build

WORKDIR /app

COPY ./frontend/package*.json ./
RUN npm ci

COPY ./frontend/ ./

# Vite inlines VITE_* vars at build time. In production the app is served
# same-origin through the nginx gateway, so a relative base (/api) is the default.
ARG VITE_API_URL
ARG VITE_NIFI_UPLOAD_URL
ENV VITE_API_URL=${VITE_API_URL:-/api}
ENV VITE_NIFI_UPLOAD_URL=${VITE_NIFI_UPLOAD_URL:-/upload}

RUN npm run build

# ── Stage 2: Serve with Nginx ────────────────────────────────────────────────
FROM nginx:stable-alpine

# Copy built assets from build stage
COPY --from=build /app/dist /usr/share/nginx/html

# SPA routing: serve index.html for all unmatched routes
RUN printf 'server {\n\
    listen 80;\n\
    server_name localhost;\n\
    root /usr/share/nginx/html;\n\
    index index.html;\n\
    location / {\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://localhost:80/ || exit 1

CMD ["nginx", "-g", "daemon off;"]