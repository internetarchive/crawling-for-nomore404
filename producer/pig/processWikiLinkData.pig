/*
 * Copyright 2013 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. 
 */


%default I_WIKI_LINKS_DIR '/wiki-data/weekly-snapshots/*/wiki-links.log.gz';
%default O_WIKI_STATS_DIR '/user/vinay/wiki-links-stats/';

REGISTER lib/elephant-bird-pig-4.1.jar;
REGISTER lib/json-simple-1.1.1.jar;
REGISTER lib/elephant-bird-core-4.1.jar;
REGISTER lib/elephant-bird-hadoop-compat-4.1.jar;
REGISTER lib/piggybank-0.10.jar;
REGISTER lib/pig-udfs.py using jython as myfuncs;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

Lines = LOAD '$I_WIKI_LINKS_DIR' USING com.twitter.elephantbird.pig.load.JsonLoader();
Lines = DISTINCT Lines;

-- number of edits with citations
LinesCount = FOREACH (GROUP Lines ALL) GENERATE COUNT(Lines) as total;
Lines = FOREACH Lines GENERATE $0#'article' as article, $0#'editor' as editor, UnixToISO($0#'now') as timestamp, $0#'language' as language, $0#'links' as linksArray;

-- number of articles w/ citation edits
Articles = FOREACH Lines GENERATE article;
Articles = DISTINCT Articles;
ArticlesCount = FOREACH (GROUP Articles ALL) GENERATE COUNT(Articles) as total;

-- number of editors
Editors = FOREACH Lines GENERATE editor;
Editors = DISTINCT Editors;
EditorsCount = FOREACH (GROUP Editors ALL) GENERATE COUNT(Editors) as total;

-- number of languages
Languages = FOREACH Lines GENERATE language;
Languages = DISTINCT Languages;
LanguagesCount = FOREACH (GROUP Languages ALL) GENERATE COUNT(Languages) as total;

-- set of days
Days = FOREACH Lines GENERATE SUBSTRING(timestamp,0,10) as day;
Days = DISTINCT Days;
Days = ORDER Days BY day;

-- regex to remove [" and  ]" and replace "," by space
CitationLines = FOREACH Lines GENERATE timestamp, article, REPLACE(REPLACE(linksArray,'^\\["|"\\]$',''),'","',' ') as linkString;
CitationLines = FOREACH CitationLines GENERATE timestamp, article, myfuncs.collectBagFromString(linkString) as links;

-- number of citations
Citations = FOREACH CitationLines GENERATE timestamp, article, FLATTEN(links) as link;
Citations = DISTINCT Citations;
CitationsCount = FOREACH (GROUP Citations ALL) GENERATE COUNT(Citations) as total;

ArticleLinks = FOREACH Citations GENERATE article, link;
ArticleLinks = DISTINCT ArticleLinks;
ArticleLinksGrp = GROUP ArticleLinks BY link;
ArticleLinksGrp = FOREACH ArticleLinksGrp GENERATE group as link, COUNT(ArticleLinks) as numArticles;
ArticleLinksGrp = ORDER ArticleLinksGrp BY numArticles DESC;

STORE LinesCount into '$O_WIKI_STATS_DIR/edits.count/';
STORE ArticlesCount into '$O_WIKI_STATS_DIR/articles.count/';
STORE LanguagesCount into '$O_WIKI_STATS_DIR/languages.count/';
STORE EditorsCount into '$O_WIKI_STATS_DIR/editors.count/';
STORE CitationsCount into '$O_WIKI_STATS_DIR/citations.count/';
STORE Days into '$O_WIKI_STATS_DIR/days/';
STORE ArticleLinksGrp into '$O_WIKI_STATS_DIR/links-by-article-count/';

