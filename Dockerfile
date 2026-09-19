FROM node:24-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
COPY migrations ./migrations
COPY test ./test

ENV NODE_ENV=production
EXPOSE 3000

CMD ["npm", "start"]
