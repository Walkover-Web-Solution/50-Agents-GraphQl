# syntax=docker/dockerfile:1

FROM node:18

WORKDIR /app

COPY ["package.json", "package-lock.json*", "./"]
RUN ls -a
RUN npm install
COPY . .
# RUN npm test
RUN npm run build
CMD [ "node", "dist/index.js" ]

