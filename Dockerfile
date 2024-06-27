# docker build -t pepmartinez/keuss-cli:2.0.0 .
# docker push pepmartinez/keuss-cli:2.0.0

FROM node:20-slim

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --only=production

COPY . .

ENTRYPOINT [ "node", "cli.js" ]
CMD [ "--help" ]

