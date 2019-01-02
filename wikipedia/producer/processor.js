const request = require('request');
const cheerio = require('cheerio');

// Wikipedia edit bots can account for many false positives, so usually we want
// to discard them
const DISCARD_WIKIPEDIA_BOTS = true;
// required for Wikipedia API
const USER_AGENT = 'Wikipedia External Links Monitor * IRC nick: Wikipedia-IA-external-links-monitor * Contact: vinay(a)archive.org';

// message (string): IRc message
// to (string): IRC channel name, in "#<language>:<project>" format.
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

function getLinksFromParseUrl(body) {
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

function getLinksFromCompareUrl(body) {
  var json;
  try {
    json = JSON.parse(body);
  } catch(e) {
    console.warn("failed to parse response body as JSON");
    json = false;
  }
  if (json && json.compare && json.compare['*']) {
    const $ = cheerio.load('<table>' + json.compare['*'] + '</table>');
    var addedLines = $('.diff-addedline');
    var addedText = "";
    addedLines.each(function() {
      let text = $(this).text().trim();
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

function processIRCMessage(to, message, producer, callback) {
  const now = Date.now();

  let components = parseMessage(message, to);
  if (!components) {
    if (callback) callback();
    return;
  }

  let { article, editor, delta, comment, language, jsonUrl } = components;

  request.get(
    {
      uri: jsonUrl,
      headers: {'User-Agent': USER_AGENT}
    },
    (error, response, body) => {
      if (error) {
        console.warn('%s: %s', jsonUrl, error);
	if (callback) callback();
        return;
      }
      var values;
      var type = "compare";
      if (jsonUrl.indexOf('action=compare') !== -1) {
        values = getLinksFromCompareUrl(body);
      } else {
        type = "parse";
        values = getLinksFromParseUrl(body);
      }
      if (values) {
        var resultsObj = {
          article, editor, delta, comment, language, now,
          links: values.links
        }
        producer.sendLinksResults(resultsObj);
      }
      if (callback) callback();
    }
  );
}

module.exports = {
  processIRCMessage,
  parseMessage,
  DISCARD_WIKIPEDIA_BOTS
}
