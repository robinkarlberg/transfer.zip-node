FROM node:alpine3.19

WORKDIR /app

# Need python3 for node-gyp-build (bufferutil)
# RUN apk add --update python3 make g++\
#    && rm -rf /var/cache/apk/*

COPY package.json package-lock.json ./
RUN npm i

COPY lib ./lib
COPY conf.json ./
COPY server.js ./

EXPOSE 3050

CMD ["npm", "start"]