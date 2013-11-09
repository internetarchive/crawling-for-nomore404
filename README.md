crawling-for-nomore404
======================

Crawling-related code for no-more-404s projects.

There are multiple projects that are mostly independent of each other.
Here are a summary of each projects. Look for README in respective project
subdirectory for more details.

wikipedia
---------
this project scrapes wikipedia IRC channel for updated article, extracts
newly added citations, and feed those URLs for crawling. scraper and
crawl-scheduler are communicating through Kafka messaging, so other apps
can also read a feed of new citations as well as original IRC notifications.

wordpress
---------
this project reads WordPress's official blog update stream, and schedules
each permalink URL of new post for crawling. it is implemented as single
application at this moment.
