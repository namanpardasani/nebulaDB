# The Inspector is a static single-page app; it talks to the admin port from the browser.
#   docker build -t nebuladb-inspector inspector
FROM node:24 AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
