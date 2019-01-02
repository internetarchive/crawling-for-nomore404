const kafka = require('kafka-node');

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
  constructor(kafkaHost, stat) {
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
    this.stat = stat;
  }
  sendMessage(message) {
    const payload = {
      topic: "wiki-irc",
      messages: message,
    };
    this.stat.ircMessages.inc(1, Date.now());
    this.producer.send([payload], (err, data) => {
      if (err) {
        console.log("kafka: send error to topic - wiki-irc: %s", err);
        this.stat.produceFailures.labels('wiki-irc').inc(1, Date.now());
      }
    });
  }
  sendLinksResults(obj) {
    const payload = {
      topic: "wiki-links",
      messages: JSON.stringify(obj)
    };
    this.stat.links.inc(1, Date.now());
    this.producer.send([payload], (err, data) => {
      if (err) {
        console.log("kafka: send error to topic - wiki-links: %s", err);
        this.stat.produceFailures.labels('wiki-links').inc(1, Date.now());
      }
    });
  }
}

module.exports = {
  ConsoleProducer,
  KafkaProducer
}
