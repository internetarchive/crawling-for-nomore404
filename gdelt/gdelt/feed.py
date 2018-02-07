#!/usr/bin/env python
#
import sys
import os
import re
import urllib2
import logging

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

from urlparse import urlsplit
from urllib2 import quote,unquote
from ConfigParser import ConfigParser

quoted_url = re.compile('^https?%3A%2F%2F', re.IGNORECASE)


class Deduper(object):
	LAST_FILE = 'LAST'
	THIS_FILE = 'THIS'
	def __init__(self, datadir):
		self.datadir = datadir
		self.log = logging.getLogger(
			'{0}.{1.__name__}'.format(__name__,Deduper))
		
	def dedup(self, source):
		lastfile = os.path.join(self.datadir, self.LAST_FILE)
		if not os.path.exists(lastfile):
			with open(lastfile, 'w') as f:
				pass

		self.lastf = open(lastfile, 'r')
		self.thisf = open(os.path.join(self.datadir, self.THIS_FILE), 'w+')
		
		for l in source:
			self.thisf.write(l + '\n')
		self.thisf.seek(0, 0)

		# assume new lines are appended at the bottom
		lt = self.thisf.readline()
		while True:
			ll = self.lastf.readline()
			if ll == '' or ll == lt:
				break
		if ll != '':
			# assumption was right
			while True:
				lt = self.thisf.readline()
				ll = self.lastf.readline()
				if ll == '' or lt != ll:
					break
			while lt != '':
				yield lt.rstrip()
				lt = self.thisf.readline()
		else:
			# assumption was wrong - new lines are inserted at the top
			# (also the case where LAST and THIS has no overwrap at all)
			self.lastf.seek(0, 0)
			ll = self.lastf.readline()
			while True:
				yield lt.rstrip()
				lt = self.thisf.readline()
				if lt == '' or lt == ll:
					break

		self.lastf.close()
		self.thisf.close()
		
	def step(self):
		lastfile = os.path.join(self.datadir, self.LAST_FILE)
		thisfile = os.path.join(self.datadir, self.THIS_FILE)
		if os.path.exists(thisfile):
			os.rename(thisfile, lastfile)
		else:
			self.log.warn('%s does not exist, step is no-op.', thisfile)


class URLHandler(object):
	def __init__(self, source):
		self.log = logging.getLogger(
			'{0}.{1.__name__}'.format(__name__,URLHandler))
		self.source = source
	
	def __iter__(self):
		return self
		
	def unquote_url(self,url):
		unq = unquote(url)
		# quote back anon-ascii characters.
		chars = [quote(c) if ord(c)>127 else c for c in unq]
		url = ''.join(chars)
		return url
	
	# TODO: url validation rules instead of `urlsplit` test.
	def validate_url(self,url):
		try:
			urlsplit(url)
			return True
		except Exception as ex:		
			self.log.error('Invalid url: {}'.format(url), exc_info=1)
			return False
			
	def next(self):
		while True:
			url = next(self.source)
			# normalize escaped URL.
			if re.match(quoted_url, url):
				url = self.unquote_url(url)
			valid = self.validate_url(url)
			if not valid:
				continue
			else:
				return url		

			
class FeedReader(object):
	"""
	Simple parser for RSS feed.
	Uses etree :func:`iterparse` to reduce memory footprint for large
	feeds.
	"""
	def __init__(self, source):
		self.log = logging.getLogger(
			'{0}.{1.__name__}'.format(__name__,FeedReader))
		self.parse = ET.iterparse(source, ['start', 'end'])
		self._item = None
		
	def __iter__(self):
		return self
		
	def next(self):
		while True:
			event, elem = next(self.parse)
			if event == 'start':
				if elem.tag == 'item':
					self._item = elem
			elif event == 'end':
				if elem.tag == 'item':
					self._item = None
				elif elem.tag == 'link':
					if self._item is not None:
						if elem.text:
							return elem.text
						
							
	
	
