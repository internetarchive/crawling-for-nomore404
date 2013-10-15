#!/usr/bin/env python3
# vim: set sw=4 et:
#

import sys
import re
from datetime import datetime
import gzip

def parse_date(s):
    if re.match(br'^\d{4}-\d{2}-\d{2}(\.?$| .*)', s):
        # 2014-10-19 (YYYY-MM-DD)
        # 2018-01-15.
        return datetime.strptime(s[:10].decode('utf-8'), '%Y-%m-%d')
    elif re.match(br'^\d{2}-[A-Z][a-z][a-z]-\d{4} \d{2}:\d{2}:\d{2} [A-Z]+$', s):
        # 03-May-2015 12:34:26 UTC
        return datetime.strptime(s.decode('utf-8'), '%d-%b-%Y %H:%M:%S %Z')
    elif re.match(br'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+Z$', s):
        # 2012-04-02T14:30:00.000Z ... ignore milliseconds
        # 2014-10-01T00:00:00.0Z ... ignore tenths of second
        return datetime.strptime(s[:19].decode('utf-8'), '%Y-%m-%dT%H:%M:%S')
    elif re.match(br'^\d\d/\d\d/\d\d\d\d \d\d:\d\d:\d\d', s):
        # 20/05/2014 00:00:00 EEST 
        # 18/08/2016 23:59:59
        return datetime.strptime(s[:19].decode('utf-8'), '%d/%m/%Y %H:%M:%S')
    elif re.match(br'^\d\d/\d\d/\d\d\d\d \d\d:\d\d:\d\d.\d\d\d\d\d\d [A-Z]+$', s):
        # 21/11/2013 23:21:16.461082 EET ... ignore TZ, guess it's nonstandard sometimes
        return datetime.strptime(s[:25].decode('utf-8'), '%d/%m/%Y %H:%M:%S.%f')
    elif re.match(br'^\d{2} [A-Z][a-z][a-z] \d{4} \d{2}:\d{2} [A-Z]+$', s):
        # 27 Jul 2014 00:00 UTC ... ignore sometimes nonstandard TZ
        return datetime.strptime(s[:17].decode('utf-8'), '%d %b %Y %H:%M')
    elif re.match(br'^[A-Z][a-z][a-z] [A-Z][a-z][a-z] \d\d \d\d:\d\d:\d\d [A-Z]+ \d\d\d\d$', s):
        # Sat Nov 02 23:59:59 GMT 2013
        return datetime.strptime(s.decode('utf-8'), '%a %b %d %H:%M:%S %Z %Y')
    elif re.match(br'^\d\d\d\d/\d\d/\d\d', s):
        # 2022/09/25
        return datetime.strptime(s.decode('utf-8'), '%Y/%m/%d')
    elif re.match(br'^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$', s):
        # 2014-03-18 08:09:30
        return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')
    elif re.match(br'^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d+$', s):
        # 2007-01-28 11:14:49.12
        return datetime.strptime(s[:19].decode('utf-8'), '%Y-%m-%d %H:%M:%S')
    elif re.match(br'^\d\d-[A-Za-z][a-z][a-z]-\d\d\d\d\.?$', s):
        # "30-jul-2014", "30-Jul-2014."
        return datetime.strptime(s[:11].decode('utf-8'), '%d-%b-%Y')
    elif re.match(br'^\d\d? [A-Za-z][a-z][a-z] \d\d\d\d$', s):
        # 21 Jul 2015
        return datetime.strptime(s.decode('utf-8'), '%d %b %Y')
    elif re.match(br'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$', s):
        # 2014-03-18T04:09:30Z
        return datetime.strptime(s.decode('utf-8'), '%Y-%m-%dT%H:%M:%SZ')
    elif re.match(br'^\d\d\d\d\d\d\d\d$', s):
        # 20131217
        return datetime.strptime(s.decode('utf-8'), '%Y%m%d')
    elif re.match(br'^[A-Z][a-z]+ \d\d \d\d\d\d$', s):
        # February 13 2014
        return datetime.strptime(s.decode('utf-8'), '%B %d %Y')
    elif re.match(br'^[A-Z][a-z][a-z] [A-Z][a-z][a-z] [ \d]\d \d\d:\d\d:\d\d \d\d\d\d$', s):
        # Tue Feb  4 23:59:00 2014
        return datetime.strptime(s.decode('utf-8'), '%a %b %d %H:%M:%S %Y')
    elif re.match(br'^\d\d\d\d\. \d\d\. \d\d\.$', s):
        # 2016. 10. 15.
        return datetime.strptime(s.decode('utf-8'), '%Y. %m. %d.')
    elif re.match(br'^\d{2}-[A-Z][a-z][a-z]-\d{4} \d{2}:\d{2}:\d{2}$', s):
        # 23-Aug-2014 00:00:00
        return datetime.strptime(s.decode('utf-8'), '%d-%b-%Y %H:%M:%S')
    elif re.match(br'^\d\d-\d\d-\d\d\d\d', s):
        # '14-06-2014  '
        return datetime.strptime(s[:10].decode('utf-8'), '%d-%m-%Y')
    elif re.match(br'^\d\d?\.\d\d?\.\d\d\d\d$', s):
        # 28.12.2013
        # 21.3.2015
        return datetime.strptime(s.decode('utf-8'), '%d.%m.%Y')
    elif re.match(br'^\d\d [A-Z][a-z][a-z] \d\d\d\d \d\d:\d\d:\d\d', s):
        # 17 Mar 2014 13:13:54 UTC
        # 11 Feb 2014 01:07:00
        return datetime.strptime(s[:20].decode('utf-8'), '%d %b %Y %H:%M:%S')
    elif re.match(br'^\d\d\d\d-[A-Z][a-z][a-z]-\d\d\.', s):
        # 2015-Feb-4.
        return datetime.strptime(s[:11].decode('utf-8'), '%Y-%b-%d')
    elif re.match(br'^\d\d\d\d-[A-Z][a-z][a-z]-\d\.', s):
        # 2015-Feb-24.
        return datetime.strptime(s[:10].decode('utf-8'), '%Y-%b-%d')
    elif re.match(br'^\d\d/\d\d/\d\d\d\d$', s):
        # whois://whois.registry.pf/paea-musculation.org.pf - "Expire (JJ/MM/AAAA) : 07/08/2014"
        return datetime.strptime(s.decode('utf-8'), '%d/%m/%Y')
    elif re.match(br'^\d\d\d\d\.\d\d\.\d\d \d\d:\d\d:\d\d$', s):
        # whois://whois.dns.pl/rezerwuje.pl - "expiration date:       2014.03.21 19:47:27"
        return datetime.strptime(s.decode('utf-8'), '%Y.%m.%d %H:%M:%S')
    elif re.match(br'^\d\d\.\d\d\.\d\d\d\d \d\d:\d\d:\d\d$', s):
        # 18.10.2013 13:53:53
        return datetime.strptime(s.decode('utf-8'), '%d.%m.%Y %H:%M:%S')
    else:
        return None

class WarcReadStates:
    EXPECTING_HEADER = 1
    IN_HEADER = 2
    IN_CONTENT = 3
    EXPECTING_RECORD_TERMINATOR_1 = 4
    EXPECTING_RECORD_TERMINATOR_2 = 5

def main(argv):
    if len(argv) > 1:
        sys.stderr.write('no arguments expected, reads warc data from stdin')
        return 1

    state = WarcReadStates.EXPECTING_HEADER

    content_length = None
    content_bytes_read = None
    url = None

    # fd = gzip.GzipFile(fileobj=sys.stdin.buffer, mode='rb')
    # fd = gzip.GzipFile(filename='WEB-20130903191312393-00000-984~desktop-nlevitt.sf.archive.org~6440.warc.gz', mode='rb')
    fd = sys.stdin.buffer
    line = fd.readline(8192)
    while line != b'':
        if state == WarcReadStates.EXPECTING_HEADER:
            if re.match(br'^WARC/\d+\.\d+\r\n$', line):
                state = WarcReadStates.IN_HEADER
            else:
                raise StandardError('warc header not found where expected')

        elif state == WarcReadStates.IN_HEADER:
            if line == b'\r\n':
                state = WarcReadStates.IN_CONTENT
                content_bytes_read = 0
                if content_length == None:
                    raise StandardError('Content-Length header not found')
            m = re.match(br'^(?i)Content-Length: (\d+)\r\n$', line)
            if m != None:
                content_length = int(m.group(1))
            m = re.match(br'^(?i)WARC-Target-URI: (.*)\r\n$', line)
            if m != None:
                url = m.group(1)

        elif state == WarcReadStates.EXPECTING_RECORD_TERMINATOR_1:
            if line != b'\r\n':
                raise StandardError('did not see first expected \\r\\n')
            state = WarcReadStates.EXPECTING_RECORD_TERMINATOR_2

        elif state == WarcReadStates.EXPECTING_RECORD_TERMINATOR_2:
            if line != b'\r\n':
                raise StandardError('did not see second expected \\r\\n')
            state = WarcReadStates.EXPECTING_HEADER

        elif state == WarcReadStates.IN_CONTENT:
            if url != None and url.startswith(b'whois://') and not re.match(br'^.*\d+\.\d+\.\d+\.\d+$', url):
                # print('=== {0} ==='.format(record.url))

                # Record expires on 30-Jul-2014.
                # Record expires on Tue Feb  4 23:59:00 2014
                # Expires on              : 2014-01-01
                # Expires On:27-May-2014 17:38:13 UTC
                #    Record expires on 2014-10-19 (YYYY-MM-DD)
                # renewal:      02-February-2014
                # validity:     25-09-2015
                # Valid-date          2013-11-29
                #
                # XXX is this an expiration date
                # whois://whois.jprs.jp/blogspot.jp 
                # "[BM-8z4|8B]                      2013/10/31"
                #
                m = re.search(br'(?im)(?:exp|valid|renewal)[^:\n]*?(?: on \s*:|:| on )\s*([^\r\n]+)', line)

                if m != None:
                    d = parse_date(m.group(1))
                    if d != None:
                        url_parts = re.split(br'[/+]', url)
                        domain = url_parts[-1]
                        d14 = d.strftime('%Y%m%d%H%M%S')
                        print('{0} {1} {2}'.format(url.decode('utf-8'), domain.decode('utf-8'), d14))

            content_bytes_read += len(line)
            if content_bytes_read == content_length:
                state = WarcReadStates.EXPECTING_RECORD_TERMINATOR_1
                content_length = None
                content_bytes_read = None
                url = None
            elif content_bytes_read > content_length:
                raise StandardError('warc record too long')

        readlimit = 8192
        if state == WarcReadStates.IN_CONTENT:
            readlimit = min(readlimit, content_length - content_bytes_read)
        line = fd.readline(readlimit)

    return 0

if __name__ == '__main__':
    main(sys.argv)

