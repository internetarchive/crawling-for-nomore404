from setuptools import setup, find_packages
setup(
    name="consumer-hq",
    version="0.1.0",
    description="Read Wikipedia links feed and schedule crawl",
    author="Kenji Nagahashi",
    author_email="kenji@archive.org",

    scripts=[
        "crawl-schedule.py"
        ],
    install_requires=[
        "ujson",
        # pegged to specific version so as not to be affected by
        # potential interface change.
        "kafka-python==0.9.4",
        # private - from local index
        "crawllib"
        ]
)

