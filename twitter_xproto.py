#! /usr/bin/env python

# twitter_xproto - Load tweets into MySQL document store from a twitter search

import mysqlx
import sys
import json
import logging
from twython import TwythonStreamer
from requests.exceptions import ChunkedEncodingError
from Queue import Queue 
from threading import Thread
from time import sleep

#logging level
# Levels: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
logging.basicConfig( level = 'INFO' )


# only command-line arg is the search term ('mysql', usually)
if( len( sys.argv ) < 2 ):
	print 'Please supply a search term'
	sys.exit(2)

# GLOBALS - for now
term = sys.argv[1]
# How long to pause if we get "too many request" messages
PAUSE_SECS = 10
SCHEMA_NAME = 'twitter_mysql'
tweet_queue = Queue()

# read twitter and mysql account info from a JSON file 
# in the current dir
cnf = open( "./twitter_auth.cnf", "r" )
twitter_auth = json.load(cnf)
cnf.close()
cnf = open( "./mysql_auth.cnf", "r" )
mysql_auth = json.load(cnf)
cnf.close()

# wraps Twitter API
class TwitterStreamer(TwythonStreamer):
	def on_error( self, status_code, msg ):
		logging.error( 'Error code: ' + str( status_code) )
		logging.error( str( msg ) )
		if( status_code == 420 ):
			logging.warning( "Pausing for " + str( PAUSE_SECS ) + ' seconds to let Twitter relax' )
			sleep( PAUSE_SECS )
		return False

	def on_success( self, tweet ):
		tweet_queue.put( tweet )
		return True

	# need to break and handle buffer overload (ChunkedEncodingError)
	def start( self): # was __call__ for the thread...
		running = True
		while running:
			try:
				logging.info( 'Starting stream' )
				self.statuses.filter( track = term )
			except ChunkedEncodingError as e:
				logging.critical( 'Stream backed up' )
				logging.critical( str( e ) )
				continue
			except KeyboardInterrupt:
				running = False
				raise
	
	def stop( self ):
		self.disconnect()

# END TwitterStreamer

# class to insert tweets to the collection. 
# only thing MySQL-aware
class TweetWriter( Thread ):

	def __init__( self, queue, search_term ):
		Thread.__init__(self)
		self.term = search_term
		self.name = self.term + '_Writer'+ self.name
		self.tweet_queue = queue
		self.inserts = 0
		self.running = True
		self.collection = self.connect()

	def insert( self, tweet_batch ):
		for t in tweet_batch:
			try:
				self.collection.add( t ).execute()
			except mysqlx.errors.OperationalError as op:
				self.collection.remove( ':tweet_id = id' ).bind( 'tweet_id', t[u'id'] ).execute()
				self.collection.add( t )
			logging.info( ' @' + t[u'user'][u'screen_name'] + ': ' + t[u'text']  )
	
	# assumes a db called $SCHEMA_NAME (see above)
	def connect( self ):
		col_name = self.term + '_tweets'
		session = mysqlx.get_session( mysql_auth)
		my_db = session.get_schema( SCHEMA_NAME )
		# Get (or create, if necessary) the Collection
		try:
			col = my_db.get_collection( col_name, check_existence = True ) 
		except mysqlx.errors.ProgrammingError:
			logging.info( "Need to create Collection " + col_name )
			col = my_db.create_collection( col_name )
			# only a NodeSession can execute SQL
			node_session = mysqlx.get_node_session( mysql_auth)
			node_session.sql( 'alter table ' + SCHEMA_NAME + '.' + col_name + ' ADD tweet_id bigint unsigned GENERATED ALWAYS AS ( doc->>"$.id" ), ADD UNIQUE INDEX id_uniq(tweet_id);' ).execute()
		return col
	
	def stop( self ):
		self.running = False
		logging.debug( "Thread %s ending", self.name )
		logging.info( "Inserted %d tweets", self.inserts )
	
	def run( self ):
		logging.debug( "TweetWriter %s starting", self.name ) 
		while self.running == True:
			if( self.tweet_queue.empty() != True ):
				tweet_batch = []
				tweet = self.tweet_queue.get()
				tweet_batch.append( tweet )
				if( tweet.has_key( u'retweeted_status' ) ):
					tweet_batch.append( tweet[u'retweeted_status'] )
					logging.debug( "RETWEET: "+ tweet[u'retweeted_status'][u'text'] + ' count ' + str( tweet[u'retweet_count'] ) )
				self.insert( tweet_batch )
				self.inserts += len( tweet_batch )
				self.tweet_queue.task_done()
			else:
				sleep(5)
# END TweetWriter

# connect to twitter
twy = TwitterStreamer( app_key = twitter_auth['consumer_key'], \
	app_secret = twitter_auth['consumer_secret'], \
	oauth_token = twitter_auth['access_token_key'], \
	oauth_token_secret= twitter_auth['access_token_secret']) 

# stay in process
twy.daemon = False
w1 = TweetWriter(tweet_queue, term )
w2 = TweetWriter(tweet_queue, term )
try:
	w1.start()
	w2.start()
	twy.start()
except KeyboardInterrupt:
	print 'Received SIGINT'
	twy.stop()
	sleep(1)
	w1.stop()
	w2.stop()

logging.info( 'All done' )

# dump unprocessed tweets to a text file (overwrite)
# TODO look for and load file on startup
if( tweet_queue.qsize() > 0 ):
	outfile_name = './tweets.txt'
	qs = tweet_queue.qsize()
	logging.debug( "Queue size at %d", qs )
	with open( outfile_name, "w" ) as outfile:
		while not tweet_queue.empty():
			json.dump( tweet_queue.get(), outfile )
			outfile.write( "\n" )
		outfile.close()
		logging.warning( "Queue not empty. Wrote out %d lines to %s", qs, outfile_name )

sys.exit(0)
