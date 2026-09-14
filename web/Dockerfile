FROM node:24-alpine

WORKDIR /app/web

COPY web/package.json web/package-lock.json ./
RUN npm ci

COPY web ./
COPY shared /app/shared

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
