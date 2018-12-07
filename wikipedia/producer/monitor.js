var irc = require('irc');
var request = require('request');

var express = require('express');
var app = express();

var $ = require('cheerio');
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

// required for Wikipedia API
var USER_AGENT = 'Wikipedia External Links Monitor * IRC nick: Wikipedia-IA-external-links-monitor * Contact: vinay(a)archive.org';

// Wikipedia edit bots can account for many false positives, so usually we want
// to discard them
var DISCARD_WIKIPEDIA_BOTS = true;

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
  client.addListener('message', processIRCMessage);
}

function parseMessage(message, to) {
  // get the editor's username or IP address
  // the IRC log format is as follows (with color codes removed):
  // rc-pmtpa: [[Juniata River]]  http://en.wikipedia.org/w/index.php?diff=516269072&oldid=514659029 * Johanna-Hypatia * (+67) Category:Place names of Native American origin in Pennsylvania
  
  var [article_flags_url, editor, delta_comment] = message.split(' * ');
  // named match group could be nice, but it is a new feature in node 10.
  var match = article_flags_url.match(/\[\[(.+?)\]\] (\S*) (.+)$/);
  if (!match) {
    return false;
  }
  var [, article, flags, jsonUrl] = match;
  // discard non-article namespaces, as listed here:
  // http://www.mediawiki.org/wiki/Help:Namespaces
  // this means only listening to messages without a ':' essentially
  if (article.indexOf(':') !== -1) {
    return false;
  }
  // discard edits made by bots.
  // bots are identified by a B flag, as documented here
  // http://www.mediawiki.org/wiki/Help:Tracking_changes
  // (the 'b' is actually uppercase in IRC)
  //
  // bots must identify themselves by prefixing or suffixing their
  // username with "bot".
  // http://en.wikipedia.org/wiki/Wikipedia:Bot_policy#Bot_accounts
  if (DISCARD_WIKIPEDIA_BOTS) {
    if ((/B/.test(flags)) ||
        (/\bbot/i.test(editor)) ||
        (/bot\b/i.test(editor))) {
      return;
    }
  }
  // normalize article titles to follow the Wikipedia URLs
  article = article.replace(/\s/g, '_');
  // the language format follows the IRC room format: "#language.project"
  var language = to.substring(1, to.indexOf('.'));
  editor = language + ':' + editor;
  // diff URL
  if (!jsonUrl) {
    console.warn('no DiffUrl field in the second component: %s', message)
    return;
  }
  if ((jsonUrl.indexOf('diff') !== -1) &&
      (jsonUrl.indexOf('oldid') !== -1)) {
    var toRev = jsonUrl.replace(/.*\?diff=(\d+).*/, '$1');
    var fromRev = jsonUrl.replace(/.*&oldid=(\d+).*/, '$1');
    if (language === 'wikidata') {
      jsonUrl = 'http://wikidata.org/w/api.php?action=compare&torev=' +
        toRev + '&fromrev=' + fromRev + '&format=json';
    } else {
      jsonUrl = 'http://' + language +
        '.wikipedia.org/w/api.php?action=compare&torev=' + toRev +
        '&fromrev=' + fromRev + '&format=json';
    }
  } else {
    if (language === 'wikidata') {
      jsonUrl = 'http://wikidata.org/w/api.php?action=parse&page=' +
	encodeURIComponent(article) +
        '&prop=externallinks&format=json';
    } else {
      jsonUrl = 'http://' + language +
        '.wikipedia.org/w/api.php?action=parse&page=' +
	encodeURIComponent(article) +
        '&prop=externallinks&format=json';
    }
  }

  match = delta_comment.match(/\(([-+]\d+)\)\s(.*)$/);
  if (match) {
    var [, delta, comment] = match;
  } else {
    var delta = '', comment = '';
  }
  return {
    article,
    editor,
    language,
    delta,
    comment,
    jsonUrl
  }
}


function processIRCMessage(from, to, message) {
  // this is the Wikipedia IRC bot that announces live changes
  if (from !== 'rc-pmtpa') {
    return;
  }
  var now = Date.now();
  stat.ircMessages.inc(1, now);

  //logging all rc-pmta messages for research purposes
  producer.sendMessage(message);

  var components = parseMessage(message, to);
  if (!components) {
    return;
  }

  var { article, editor, delta, comment, language, jsonUrl } = components;

  //set the last seen message TS
  lastSeenMessageTimestamp = now;

  request.get(
    {
      uri: jsonUrl,
      headers: {'User-Agent': USER_AGENT}
    },
    function(error, response, body) {
      var values;
      var type = "compare";
      if (jsonUrl.indexOf('action=compare') !== -1) {
        values = getLinksFromCompareUrl(error, body);
      }
      else {
	type = "parse";
        values = getLinksFromParseUrl(error, body);
      }
      if (values) {
	var resultsObj = {
	  article, editor, delta, comment, language, now,
	  links: values.links
	}
	producer.sendLinksResults(resultsObj);
	stat.links.inc(1, Date.now());
      }
    }
  );
}

function getLinksFromParseUrl(error, body) {
  if (!error) {
    var json;
    try {
      json = JSON.parse(body);
    } catch(e) {
      json = false;
    }
    if(json && json.parse && json.parse && json.parse['externallinks']) {
      var links = json.parse['externallinks'];
      if(links.length > 0) {
	return {
	  links: links
	};
      }
    }
  }
}

function getLinksFromCompareUrl(error, body) {
  if (!error) {
    var json;
    try {
      json = JSON.parse(body);
    } catch(e) {
      json = false;
    }
    if ((json && json.compare && json.compare['*'])) {
      var parsedHtml = $.load(json.compare['*']);
      var addedLines = parsedHtml('.diff-addedline');
      var addedText = "";
      addedLines.each(function(i, elem) {
        var text = $(this).text().trim();
	addedText = addedText.concat(text);
      });

      //find all occurrences of links
      //reference: http://stackoverflow.com/questions/1986121/match-all-urls-in-string-and-return-in-array-in-javascript
      geturl = new RegExp(
        //"(^|[ \t\r\n])((ftp|http|https):(([A-Za-z0-9$_.+!*(),;/?:@&~=-])|%[A-Fa-f0-9]{2}){2,}(#([a-zA-Z0-9][a-zA-Z0-9$_.+!*(),;/?:@&~=%-]*))?([A-Za-z0-9$_+!*();/?:~-]))"
        "((ftp|http|https):(([A-Za-z0-9$_.+!*(),;/?:@&~=-])|%[A-Fa-f0-9]{2}){2,}(#([a-zA-Z0-9][a-zA-Z0-9$_.+!*(),;/?:@&~=%-]*))?([A-Za-z0-9$_+!*();/?:~-]))"
        ,"g"
      );
      var links = addedText.match(geturl);
      if (links && links.length > 0) {
        return {
          links: links
        };
      }
    }
  }
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
