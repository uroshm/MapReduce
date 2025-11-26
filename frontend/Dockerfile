FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

RUN npm run build

FROM node:20-alpine

WORKDIR /app

COPY --from=builder /app/dist ./dist

RUN npm install express

COPY server.js ./

ENV NODE_ENV=production
ENV PORT=5173

EXPOSE 5173

CMD ["node", "server.js"]