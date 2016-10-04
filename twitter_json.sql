-- SQL statements to search through Twitter data in a JSON Collection
-- Requires MySQL > 5.7.8
-- Philip Antoniades, 2016, philip.antoniades@oracle.com

-- make sure to set the active DB to the one wiht your collection
use twitter_mysql

-- using the collection 'mysql_tweets', find all tweets with the hashtag 'MySQL'
SELECT doc->"$.id", doc->"$.entities.hashtags" 
FROM  mysql_tweets 
WHERE json_search( doc, 'all', "MySQL", NULL, "$.entities.hashtags" ) IS NOT NULL;
--- note this includes retweets

-- how many tweets have Coordinates (geo data)?
select count(*) with_coordinates, 
CONCAT( ( count(*)/(select count(*) FROM mysql_tweets))*100, '%') perc_of_tweets 
FROM mysql_tweet;
