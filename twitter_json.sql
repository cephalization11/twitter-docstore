-- SQL statements to search through Twitter data in a JSON Collection
-- Requires MySQL > 5.7.8
-- Philip Antoniades, 2016, philip.antoniades@oracle.com

-- make sure to set the active DB to the one wiht your collection
-- you may additionally have to search & replace for your table name (if you didn't search for 'mysql')
use twitter_mysql

-- using the collection 'mysql_tweets', find all tweets with the hashtag 'MySQL'
-- returns a result set of Arrays of JSON objects (the hashtag sets for each tweet)
SELECT doc->"$.id", doc->"$.entities.hashtags[*].text" 
FROM  mysql_tweets 
WHERE json_search( doc, 'all', "MySQL", NULL, "$.entities.hashtags" ) IS NOT NULL;

-- how many tweets have Coordinates (geo data)?
SELECT count(*) with_coordinates,  CONCAT( ( count(*)/(select count(*) FROM mysql_tweets))*100, '%') perc_of_tweets  FROM mysql_tweets WHERE doc->"$.coordinates" NOT LIKE 'null';

-- how many tweets have the hashtag 'MySQL'?
SELECT count(*) FROM  mysql_tweets  WHERE json_search( doc, 'all', "MySQL", NULL, "$.entities.hashtags" ) IS NOT NULL;

