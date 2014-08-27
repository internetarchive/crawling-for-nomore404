from gdelt.feed import Deduper, FeedReader

import pytest
import py

def test_dedup_new_at_bottom(tmpdir):
    tmpdir.join('LAST').write(
        "A\n"
        "B\n"
        "C\n"
        "D\n"
        "E\n"
        )
    source = ["B", "C", "D", "E", "F"]

    dedup = Deduper(str(tmpdir))

    out = list(dedup.dedup(source))

    assert out == ["F"]

def test_dedup_new_at_top(tmpdir):
    tmpdir.join('LAST').write(
        "A\n"
        "B\n"
        "C\n"
        "D\n"
        "E\n"
        )
    source = ["F", "A", "B", "C", "D"]

    dedup = Deduper(str(tmpdir))

    out = list(dedup.dedup(source))

    assert out == ["F"]


def test_feed_reader():
    sample = py.path.local(__file__).dirpath(
        'internet_archive_no404_export.rss')
    assert sample.isfile()
    
    reader = FeedReader(sample.open('rb'))
    
    url1 = next(reader)
    assert url1 == ("http://lasvegassun.com/news/2014/aug/24/"
                    "californians-tear-out-lawns-cope-drought/")
    for url in reader:
        assert url is not None
