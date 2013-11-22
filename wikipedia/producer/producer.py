import sys
import re
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

kafka = KafkaClient("crawl-db02.us.archive.org", 9092)
wikiIRCProducer = SimpleProducer(kafka, "wiki-irc", async=False,
                          req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE)

#wikiIRCProducer = SimpleProducer(kafka, "wiki-irc");
#wikiLinksProducer = SimpleProducer(kafka, "wiki-links");

wikiLinksProducer = SimpleProducer(kafka, "wiki-links", async=False,
                          req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE)

#read in lines from wikipedia live monitor server
for line in sys.stdin:
	try:
    		line.decode('utf-8')
	except UnicodeDecodeError:
    		continue
	line = line.strip()
	if line:
		kafkaSuccess = False
		lineToConsider = False
		if re.match(r'^Wiki-IRC-Message:.*$', line):
			lineToConsider = True
			message = line.replace("Wiki-IRC-Message:","",1);
			response = wikiIRCProducer.send_messages(message);
			if response and response[0].error == 0:
    				kafkaSuccess = True
		elif re.match(r'^Wiki-Links-Results:.*$', line):
			lineToConsider = True
			message = line.replace("Wiki-Links-Results:","",1);
			response = wikiLinksProducer.send_messages(message);
			if response and response[0].error == 0:
    				kafkaSuccess = True
	if lineToConsider and not kafkaSuccess:
		# FLUSH to disk
		#print this line out so we can feed these lines from STDIN to this script at a later time (retry failed push) 
		print line
kafka.close()	
