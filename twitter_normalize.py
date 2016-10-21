#! /usr/bin/env python

# twitter_normalize - extract documents and fields to other Collections and Tables in MySQL

import mysqlx
import sys
import json
import logging

#logging level
# Levels: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
logging.basicConfig( level = 'INFO' )


# only command-line arg is the search term ('mysql', usually)
if( len( sys.argv ) < 2 ):
	print 'Please supply a search term'
	sys.exit(2)

# GLOBALS - for now
term = sys.argv[1]
tweets = term + '_tweets'
SCHEMA_NAME = 'twitter_mysql'

