#/usr/bin/python
from pig_util import outputSchema


@outputSchema("y:bag{t:tuple(bagtuple:chararray)}") 
def collectBagFromString(bagString):
	outBag = []
	for word in bagString.split():
		outBag.append(word)
	return outBag
