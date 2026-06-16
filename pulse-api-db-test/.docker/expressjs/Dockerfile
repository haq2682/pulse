FROM node:lts-alpine3.22

WORKDIR /app

# Copy package files first for better layer caching
COPY ./node/package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the app
COPY ./node .

EXPOSE 3000

CMD ["npm", "run", "dev"]
