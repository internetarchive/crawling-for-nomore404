from setuptools import setup

"""
Now this application is designed to be set up into a deployment directory
with virtualenv and nodeenv.

Installation Steps:
- create a virtualenv and activate it.
- run ``python setup.py install``
- setup libicu-dev OS package to avoid error while building irc module
- setup node.js and dependencies with ``nodeenv -p -r node-requirements.txt``
"""

setup(
    name="No404WikipediaScraper",
    version="0.1.1",
    install_requires=[
        "nodeenv",
        "kafka-python==0.9.3"
        ],
    scripts=[
        "run-monitor.sh",
        "producer.py",
        # node.js application scripts are installed in bin for now.
        # TODO: find a better layout.
        "monitor.js",
        "wikipedias.js"
        ],
    author="Vinay Goel",
    )
