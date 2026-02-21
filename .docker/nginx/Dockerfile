FROM nginx:alpine

# Remove default nginx configs
RUN rm -f /etc/nginx/conf.d/default.conf \
    && rm -f /etc/nginx/conf.d/*.conf

# The custom nginx.conf will be mounted via docker-compose
EXPOSE 80 443

CMD ["nginx", "-g", "daemon off;"]