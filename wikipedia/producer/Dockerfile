FROM node:8-jessie as build

ENV DEST=/opt/wikipedia-monitor

RUN apt-get update && apt-get install -y build-essential libicu-dev

RUN mkdir $DEST
WORKDIR $DEST
ADD package.json *.js $DEST/
RUN npm install

FROM node:8-jessie

ENV DEST=/opt/wikipedia-monitor

RUN apt-get update && apt-get install -y libicu52
COPY --from=build $DEST/ $DEST/
WORKDIR $DEST

CMD ["node", "monitor.js"]
