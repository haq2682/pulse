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

COPY --from=build /app/dist /usr/share/nginx/html

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