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

def add_date_column( session ):
	node_session = session
	sql = ('ALTER TABLE ' + SCHEMA_NAME + '.' + tweets_collection +
	' ADD created_at DATETIME AS ( STR_TO_DATE(doc->>"$.created_at", \'%a %b %d %H:%i:%s +0000 %Y\') ),' 
	' ADD INDEX dt_idx(created_at);') 
	logging.debug( sql )
	node_session.sql( sql ).execute()

# Takes way longer, needs to be updated!
def add_user_column(session): 
	node_session = session
	user_collection_name = 'users'
	if( PREPEND_TERM ):
		user_collection_name = term + '_' + user_collection_name
	#user_collection = node_session.get_schema( SCHEMA_NAME).create_collection( user_collection_name )
	user_sql = 'UPDATE ' + SCHEMA_NAME + '.' + tweets_collection + ' set user_id = CAST(doc->>\"$.user.id\" AS UNSIGNED)'
	sql = ('ALTER TABLE ' + SCHEMA_NAME + '.' + tweets_collection +
	' ADD user_id bigint(20) UNSIGNED, ADD INDEX usr_inx( user_id )' )
	logging.debug( sql )
	node_session.sql( sql ).execute()
	logging.debug( user_sql )
	node_session.sql( user_sql ).execute()
	# for user in tweets_collection do... ignore duplicate key warnings on User table	



ns = mysqlx.get_node_session( mysql_auth)
add_date_column(ns)
add_user_column(ns)

	
