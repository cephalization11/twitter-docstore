#! /usr/bin/env python

# twitter_normalize - extract documents and fields to other Collections and Tables in MySQL

import mysqlx
import sys
import json
import logging

#logging level
# Levels: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
logging.basicConfig( level = 'DEBUG' )


# only command-line arg is the search term ('mysql', usually)
if( len( sys.argv ) < 2 ):
	print 'Please supply a search term'
	sys.exit(2)

# GLOBALS - for now
term = sys.argv[1]
tweets_collection = term + '_tweets'
SCHEMA_NAME = 'twitter_mysql'
PREPEND_TERM = True

cnf = open( "./mysql_auth.cnf", "r" )
mysql_auth = json.load(cnf)
cnf.close()

# Clean way to add virtual DATETIME column
def add_date_column( session ):
	node_session = session
	sql = ('ALTER TABLE ' + SCHEMA_NAME + '.' + tweets_collection +
	' ADD created_at DATETIME AS ( STR_TO_DATE(doc->>"$.created_at", \'%a %b %d %H:%i:%s +0000 %Y\') ),' 
	' ADD INDEX dt_idx(created_at);') 
	logging.debug( sql )
	node_session.sql( sql ).execute()

# add a 'user screen_name' column
def add_user_column(session): 
	node_session = session
	sql = ('ALTER TABLE ' + SCHEMA_NAME + '.' + tweets_collection +
	' ADD screen_name VARCHAR(128) as ( doc->>\"$.user.screen_name\"), ADD INDEX usr_name_inx( screen_name )' )
	logging.debug( sql )
	node_session.sql( sql ).execute()
	
# The "bad" way - try only if you want to compare times. Takes way longer, needs to be updated to capture new data.
def add_user_column_bad(session): 
	node_session = session
	user_sql = 'UPDATE ' + SCHEMA_NAME + '.' + tweets_collection + ' set user_id = CAST(doc->>\"$.user.id\" AS UNSIGNED)'
	sql = ('ALTER TABLE ' + SCHEMA_NAME + '.' + tweets_collection +
	' ADD user_id bigint(20) UNSIGNED, ADD INDEX usr_inx( user_id )' )
	logging.debug( sql )
	node_session.sql( sql ).execute()
	logging.debug( user_sql )
	node_session.sql( user_sql ).execute()
	# for user in tweets_collection do... ignore duplicate key warnings on User table	

# iterate through the hashtags and create a many-to-many relationship to a Table object
def normalize_hashtags( session ):
	node_session = session
	hashtag_table_name = 'hashtags'
	if( PREPEND_TERM ):
		hashtag_table_name = term + '_' + hashtag_table_name
		create_hash = 'CREATE TABLE IF NOT EXISTS ' + hashtag_table_name + ' ( id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY, name VARCHAR(128) NOT NULL UNIQUE );' 
		create_m2m = 'CREATE TABLE IF NOT EXISTS ' + hashtag_table_name + '_tweets ( hashtag_id INTEGER UNSIGNED NOT NULL, tweet_id bigint(20) UNSIGNED NOT NULL )'
		logging.debug( "Create SQL:\n" + create_hash + "\n" + create_m2m )
		node_session.sql( create_hash )
		node_session.sql( create_m2m )
		logging.debug( "tables created" )

def add_retweet_count_index( session ):
	# this one could use an XSession
	xsession = session
	collection = xsession.get_schema( SCHEMA_NAME ).get_collection( tweets_collection )
	collection.create_index( 'rtwt_idx', False ).field( "$.retweet_count", "INT", False ).execute()

def add_user_table( session ):
	xsession = session
	user_collection_name = 'users'
	tweet_col = xsession.get_schema( SCHEMA_NAME ).get_collection( tweets_collection )
	logging.debug( "Pulling all users" )
	users = []
	try:
		users = tweet_col.find().fields('user').group_by('user.id, user, user.created_at').sort( 'user.created_at').execute().fetch_all()
	except Exception as e:
		print( str( e ) )
	logging.debug( str( type( users ) )+ ": " + str( type( users[0] ) ) )
	if( PREPEND_TERM ):
		user_collection_name = term + '_' + user_collection_name
	user_collection = xsession.get_schema( SCHEMA_NAME).create_collection( user_collection_name, reuse = True )
	for u in users:
		try:
			#logging.debug( u.user[u'screen_name'] )
			user_collection.add( u.user ).execute()	
		except Exception as e:
			print( str( e ))


ns = mysqlx.get_node_session( mysql_auth)
xs = mysqlx.get_session( mysql_auth )
add_date_column(ns)
add_user_column(ns)
add_retweet_count_index( xs )
add_user_table( xs)
