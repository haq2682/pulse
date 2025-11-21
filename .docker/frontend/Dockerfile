FROM node:25-alpine

WORKDIR /app

COPY ./frontend/package*.json ./

RUN npm install

COPY ./frontend/ ./

EXPOSE 3000

CMD ["npm", "start"]

# FROM nginx:stable-alpine
# COPY --from=build /app/build /usr/share/nginx/html
# EXPOSE 80
# CMD ["nginx", "-g", "daemon off;"]