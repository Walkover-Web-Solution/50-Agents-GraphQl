# syntax=docker/dockerfile:1

FROM node:18
EXPOSE 8080

WORKDIR /app

COPY ["package.json", "package-lock.json*", "./"]
RUN ls -a
RUN npm install
COPY . .
# RUN npm test
RUN npm run build
CMD [ "npm", "run", "dev" ]

