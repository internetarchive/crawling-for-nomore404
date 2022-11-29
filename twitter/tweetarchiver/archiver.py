"""
class for saving tweets in a proprietary format.

:class:`Archiver` saves each tweet message in its own gzipped record, which
is appended to a file until file gets bigger than set `rollsize`.
"""
import os
try:
    import ujson as json
except ImportError:
    import json
from gzip import GzipFile
from datetime import datetime

import logging

__all__ = ['Archiver']

class Archiver(object):
    def __init__(self, rollsize=int(1e9), prefix='twitter-stream-sample',
                 tsformat='%Y%m%d%H%M%S', destdir='.'):
        self.rollsize = rollsize
        self.prefix = prefix
        self.tsformat = tsformat
        self.destdir = destdir

        self.arc = None

    def __del__(self):
        self.close()

    def close(self):
        if self.arc: self.arc.close()
        self.arc = None

    def _makefn(self):
        t = datetime.now()
        fn = "%s-%s.gz" % (self.prefix, t.strftime(self.tsformat))
        return os.path.join(self.destdir, fn)

    def archive_message(self, message):
        if isinstance(message, dict):
            message = json.dumps(message)
        if self.arc is None:
            self.arc = ArchiveFile(self._makefn())
            
        self.arc.write_record(message)

        if self.arc.size() > self.rollsize:
            self.arc = None

class ArchiveFile(object):
    def __init__(self, fn, complevel=2):
        self.fn = fn
        self.complevel = complevel
        self.f = open(self.fn + '.open', 'wb')
    def __del__(self):
        self.close()
    def close(self):
        if self.f:
            self.f.close()
            self.f = None
            try:
                os.rename(self.fn + '.open', self.fn)
            except Exception as ex:
                logging.warning('failed to rename %s.open to %s (%s)',
                             self.fn, self.fn, ex)
    def write_record(self, message):
        """message must be one whole streaming message."""
        if self.f is None:
            raise IOError("attempted to write into closed file %s" % self.fn)
        z = GzipFile(fileobj=self.f, mode='wb', compresslevel=self.complevel)
        z.write(message.encode('utf-8')+b'\r\n')
        z.close()
        self.f.flush()

    def size(self):
        if self.f:
            return self.f.tell()
        else:
            return os.path.getsize(self.fn)
