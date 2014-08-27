#!/usr/bin/env python
#
import sys
import os

import urllib2

import logging
import xml.etree.ElementTree as ET
from ConfigParser import ConfigParser

class Deduper(object):
    LAST_FILE = 'LAST'
    THIS_FILE = 'THIS'
    def __init__(self, datadir):
        self.datadir = datadir
        self.log = logging.getLogger(__name__)

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

class FeedReader(object):
    """
    Simple parser for RSS feed.
    Uses etree :func:`iterparse` to reduce memory footprint for large
    feeds.
    """
    def __init__(self, source):
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
                        return elem.text
