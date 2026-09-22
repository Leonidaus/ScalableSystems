FROM node:22-alpine

WORKDIR /app

# Install deps first so this layer is cached while only source changes.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

EXPOSE 3001
CMD ["node", "index.js"]
