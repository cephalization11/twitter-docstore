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
logging.basicConfig( level = 'INFO' )

# How long to pause if we get "too many request" messages
PAUSE_SECS = 5

# only command-line arg is the search term ('mysql', usually)
if( len( sys.argv ) < 2 ):
	print 'Please supply a search term'
	sys.exit(2)

# GLOBALS - for now
term = sys.argv[1]
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
		logging.critical( 'Error code: ' + str( status_code) )
		logging.critical( str( msg ) )
		if( status_code == 420 ):
			logging.warning( "Pausing for " + PAUSE_SECS + ' to let Twitter relax' )
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
		self.collection.create_index( 'unique_id', True )

	def insert( self, tweet_batch ):
		for t in tweet_batch:
			try:
				self.collection.add( t ).execute()
			except mysqlx.errors.OperationalError as op:
				# I believe the only cause for this is duplicate key 
				logging.info( 'Duplicate key: ' + str( op.args ) + ': ' + str( t[u'id']) )
				old = self.collection.find( ':tweet_id = id' ).bind( 'tweet_id', t[u'id'] ).execute()
				logging.warning( 'Equal? '+ "\n" +  str( old.fetch_all()[0][u'id'] ) + "\n" + str( t[u'id'] ) )
				self.collection.remove( ':tweet_id = id' ).bind( 'tweet_id', t[u'id'] ).execute()
				# could these clash again given the asynchronicity?
				self.collection.add( t )
				logging.info( "Added: " + str( t[u'id'] ) )
	
	# assumes a db called 'twitter_mysql'
	def connect( self ):
		logging.info( self.name + " IN Connect" )
		col_name = self.term + '_tweets'
		session = mysqlx.get_session( mysql_auth)
		my_db = session.get_schema( 'twitter_mysql')
		# create (or get, if exists) the Collection
		try:
			col = my_db.get_collection( col_name, check_existence = True ) 
		except mysqlx.errors.ProgrammingError:
			logging.info( "Need to create table" )
			col = my_db.create_collection( col_name )
			# only a NodeSession can execute SQL
			node_session = mysqlx.get_node_session( mysql_auth)
			node_session.sql( 'alter table twitter_mysql.' + col_name + ' ADD tweet_id bigint unsigned GENERATED ALWAYS AS ( doc->>"$.id" ), ADD UNIQUE INDEX id_uniq(tweet_id);' ).execute()
		return col
	
	def stop( self ):
		self.running = False
		logging.info( "Thread %s ending", self.name )
		logging.info( "Inserted %d tweets", self.inserts )
	
	def run( self ):
		logging.info( "TweetWriter %s starting", self.name ) 
		while self.running == True:
			if( self.tweet_queue.empty() != True ):
				tweet_batch = []
				tweet = self.tweet_queue.get()
				tweet_batch.append( tweet )
				if( tweet.has_key( u'retweeted_status' ) ):
					tweet_batch.append( tweet[u'retweeted_status'] )
					logging.info( "Booyah: "+ tweet[u'retweeted_status'][u'text'] +  str( tweet[u'retweeted_status'][u'retweeted'] ) )
				self.insert( tweet_batch )
				self.inserts += len( tweet_batch )
				self.tweet_queue.task_done()
				for t in tweet_batch:
					logging.info( ' @' + t[u'user'][u'screen_name'] + ': ' + t[u'text']  )
			else:
				sleep(5)


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
	logging.warning( "Queue size at %d", tweet_queue.qsize() )
	with open( "./tweets.txt", "w" ) as outfile:
		while not tweet_queue.empty():
			json.dump( tweet_queue.get(), outfile )
			outfile.write( "\n" )
		outfile.close()

sys.exit(0)
