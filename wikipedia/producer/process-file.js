const {processIRCMessage} = require('./processor');
const {ConsoleProducer, KafkaProducer} = require('./producer');

const prom = require('prom-client');
const stat = {
  ircMessages: new prom.Counter({
    name: 'irc_messages',
    help: 'IRC messages received'
  }),
  links: new prom.Counter({
    name: 'links',
    help: 'Links messages generated'
  }),
  restarted: new prom.Counter({
    name: 'restarted',
    help: 'Times watchdog restarted IRC client'
  }),
  produceFailures: new prom.Counter({
    name: 'produce_failures',
    help: 'number of producer send failures',
    labelNames: ['topic']
  })
};

const readline = require('readline');

var inProgress = 0;

var producer;
const kafka_host = process.env.KAFKA_HOST;
if (kafka_host) {
  producer = new KafkaProducer(kafka_host, stat);
} else {
  producer = new ConsoleProducer();
}

const reader = readline.createInterface({
  input: process.stdin
});

var inProgress = 0, paused = false;
var lineno = 0;

reader.on('line', line => {
  console.log("%d: %s", ++lineno, line);
  if (++inProgress > 8) {
    reader.pause();
    paused = true;
  }
  // using fixed channel name because it's not saved in IRC message feed.
  // language name is not important except for "wikidata".
  let channel = line.match(/www\.wikidata\.org/) ? "#wikidata.wikpedia" :
      "#en.wikipedia";
  processIRCMessage(channel, line, producer, () => {
    if (--inProgress < 6 && paused) {
      reader.resume();
    }
  });
}).on('close', () => {
  process.exit(0);
});
