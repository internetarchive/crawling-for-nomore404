from setuptools import setup, find_packages

setup(
    name="twitter-archiver",
    version="0.3.8",
    author="Kenji Nagahashi",
    author_email="kenji@archive.org",
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'ujson',
        'configobj',
        'kafka-python',
        'PyYAML>=5,<6',
        'warctools',
        'requests'
    ],
    scripts=[
        'kafkastream.py',
        'tweetwarc.py'
    ]
)

