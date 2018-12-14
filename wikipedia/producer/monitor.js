var irc = require('irc');

var express = require('express');
var app = express();

const {processIRCMessage} = require('./processor');
var wikipedias = require('./wikipedias.js');

var kafka = require('kafka-node');

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

// whether to monitor the 1,000,000+ articles Wikipedias
var MONITOR_SHORT_TAIL_WIKIPEDIAS = true;

// whether to monitor the 100,000+ articles Wikipedias
var MONITOR_LONG_TAIL_WIKIPEDIAS = true;

// whether to also monitor the << 100,000+ articles Wikipedias
var MONITOR_REALLY_LONG_TAIL_WIKIPEDIAS = true;

// whether to monitor the knowledge base Wikidata
var MONITOR_WIKIDATA = true;

// whether to monitor Wikinews
var MONITOR_WIKINEWS = true;

// IRC details for the recent changes live updates
var IRC_SERVER = 'irc.wikimedia.org';
var IRC_NICK = 'Wikipedia-IA-external-links-monitor';
var IRC_REAL_NAME_AND_CONTACT = 'Vinay Goel (vinay@archive.org)';

var IRC_CHANNELS = [];
var PROJECT = '.wikipedia';
if (MONITOR_SHORT_TAIL_WIKIPEDIAS) {
  Object.keys(wikipedias.millionPlusLanguages).forEach(function(language) {
    if (wikipedias.millionPlusLanguages[language]) {
      IRC_CHANNELS.push('#' + language + PROJECT);
    }
  });
}
if (MONITOR_LONG_TAIL_WIKIPEDIAS) {
  Object.keys(wikipedias.oneHundredThousandPlusLanguages).forEach(
      function(language) {
    if (wikipedias.oneHundredThousandPlusLanguages[language]) {
      IRC_CHANNELS.push('#' + language + PROJECT);
    }
  });
}
if (MONITOR_REALLY_LONG_TAIL_WIKIPEDIAS) {
  Object.keys(wikipedias.reallyLongTailWikipedias).forEach(function(language) {
    if (wikipedias.reallyLongTailWikipedias[language]) {
      IRC_CHANNELS.push('#' + language + PROJECT);
    }
  });
}
if (MONITOR_WIKIDATA) {
  Object.keys(wikipedias.wikidata).forEach(function(language) {
    if (wikipedias.wikidata[language]) {
      IRC_CHANNELS.push('#' + language + PROJECT);
    }
  });
}
if (MONITOR_WIKINEWS) {
  Object.keys(wikipedias.wikinews).forEach(function(language) {
    if (wikipedias.wikinews[language]) {
      IRC_CHANNELS.push('#' + language + '.wikinews');
    }
  });
}

var client = null;
var lastSeenMessageTimestamp = null;
var MAX_WAIT_FOR_RESET = 120000;

var producer;

function newClient() {
  console.log("Monitor: Starting a new IRC client");
  client = new irc.Client(
    IRC_SERVER,
    IRC_NICK,
    {
      userName: IRC_NICK,
      realName: IRC_REAL_NAME_AND_CONTACT,
      floodProtection: true,
      showErrors: true,
      stripColors: true
    }
  );

  client.addListener('registered', message => {
    console.log('Connected to IRC server ' + IRC_SERVER);
    // connect to IRC channels
    IRC_CHANNELS.forEach(channel => {
      console.log('Joining channel %s', channel);
      client.join(channel);
    });
  });

  // // fired whenever the client connects to an IRC channel
  // client.addListener('join', function(channel, nick, message) {
  //   console.log(nick + ' joined channel ' + channel);
  // });
  // // fired whenever someone parts a channel
  // client.addListener('part', function(channel, nick, reason, message) {
  //   console.log('User ' + nick + ' has left ' + channel + ' (' + reason + ')');
  // });
  // // fired whenever someone quits the IRC server
  // client.addListener('quit', function(nick, reason, channels, message) {
  //   console.log('User ' + nick + ' has quit ' + channels + ' (' + reason + ')');
  // });
  // fired whenever someone sends a notice
  client.addListener('notice', (nick, to, text, message) => {
    console.log('Notice from %s to %s: %s', nick || 'server', to, text);
  });
  // // fired whenever someone gets kicked from a channel
  // client.addListener('kick', function(channel, nick, by, reason, message) {
  //   console.warn('User ' + (by === undefined? 'server' : by) + ' has kicked ' +
  //       nick + ' from ' + channel +  ' (' + reason + ')');
  // });
  // // fired whenever someone is killed from the IRC server
  // client.addListener('kill', function(nick, reason, channels, message) {
  //   console.warn('User ' + nick + ' was killed from ' + channels +  ' (' +
  //       reason + ')');
  // });
  // fired whenever the client encounters an error
  client.addListener('error', function(message) {
    console.warn('IRC error: %s', message);
  });

  // fires whenever a new IRC message arrives on any of the IRC rooms
  client.addListener('message', (from, to, message) => {
    // "rc-pmtpa" is the name of the Wikipedia IRC bot that announces live
    // changes.
    if (from == 'rc-pmtpa') {
      lastSeenMessageTimestamp = Date.now();
      // logging all rc-tma mesages for  research purposes
      producer.sendMessage(message);
      processIRCMessage(to, message, producer);
    }
  });
}

// for testing
class ConsoleProducer {
  sendMessage(message) {
    console.log('Wiki-IRC-Message:%s', message);
  }
  sendLinksResults(obj) {
    console.log('Wiki-Links-Results:%s', JSON.stringify(obj));
  }
}
class KafkaProducer {
  constructor(kafkaHost) {
    this.client = new kafka.KafkaClient({
      kafkaHost
    });
    this.producer = new kafka.Producer(this.client)
    this.producer.on('error', err => {
      console.log("kafka: some general error occurred: %s",err);
    });
    this.producer.on("brokerReconnectError", err => {
      console.log("kafka: could not reconnect: %s", err);
      console.log("kafka: will retry on next send()");
    });
  }
  sendMessage(message) {
    const payload = {
      topic: "wiki-irc",
      messages: message,
    };
    stat.ircMessages.inc(1, Date.now());
    this.producer.send([payload], (err, data) => {
      if (err) {
        console.log("kafka: send error to topic - wiki-irc: %s", err);
        stats.produceFailures.labels('wiki-irc').inc(1, Date.now());
      }
    });
  }
  sendLinksResults(obj) {
    const payload = {
      topic: "wiki-links",
      messages: JSON.stringify(obj)
    };
    stat.links.inc(1, Date.now());
    this.producer.send([payload], (err, data) => {
      if (err) {
        console.log("kafka: send error to topic - wiki-links: %s", err);
        stats.produceFailures.labels('wiki-links').inc(1, Date.now());
      }
    });
  }
}

// default collection interval 10 seconds.
prom.collectDefaultMetrics();
app.get('/metrics', (req, res) => {
  res.send(prom.register.metrics());
});

const kafka_host = process.env.KAFKA_HOST;
if (kafka_host) {
  producer = new KafkaProducer(kafka_host);
} else {
  producer = new ConsoleProducer();
}

newClient();

//watchdog
function resetClientIfDead() {
  var now = Date.now();
  if (lastSeenMessageTimestamp === null || (now - lastSeenMessageTimestamp) > MAX_WAIT_FOR_RESET) {
    // reset Wiki client
    newClient();
    stats.restarted.inc(1, Date.now());
  }
}
setInterval(resetClientIfDead, 2 * MAX_WAIT_FOR_RESET);

// start the server
var port = process.env.PORT || 8080;
console.log('Wikipedia IA External Links Monitor running on port ' + port);
app.listen(port);
