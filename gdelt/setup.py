from setuptools import setup, find_packages

setup(
    name='no404-gdelt',
    version='0.1.1',
    author='Kenji Nagahashi',
    author_email='kenji@archive.org',
    packages=['gdelt'],
    description="crawl schedule pipeline reading RSS feed from GDELT",
    #packages=find_packages(),
    scripts=['process-feed.py'],
    install_requires=[
        # requires libevent
        #'gevent>=0.13.6',
        'crawllib>=0.1.0-4',
        'PyYAML',
        'raven'
        ],
    )
