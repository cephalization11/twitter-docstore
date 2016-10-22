# twitter-docstore

A python example for working with MySQL document stores using Twitter data. The key thing you will need is a Twitter developer account and a working MySQL server (5.7.8 or later) with the X plugin enabled. 

## Dependencies
* Twython
* [mysql-connector-python v 2.2.1 (beta)](https://dev.mysql.com/downloads/connector/python/ "MySQL Downloads" )
* [protobuf 3](https://dev.mysql.com/downloads/connector/python/ "Google Protobuf" )

## Installation
1. Optional - bring up the Vagrant client environment (it will install all the dependencies save Connector/Python)
2. Create your [Twitter Application Keys](https://dev.twitter.com/oauth/overview/application-owner-access-tokens "Twitter Dev site") if you don't already have them
3. Put this repo and the python tar.gz in your Vagrant home directory, unpack and install the python connector (in the vm)
4. Create the twitter_mysql schema in your running MySQL 5.7 server
5. Edit the 2 .cnf files with your twitter keys and MySQL connection information
6. That's it! You should be able to run ./twitter_xproto.py and watch the tweets scroll by.