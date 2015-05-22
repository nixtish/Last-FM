# Script to build MongoDB Colection and carry out aggregate operations
# Assume that a mongod instance in running, can run instance using
#	mongod.exe --dbpath "C:\\mongo_db"  -- for my system
#   mongoexport command to be used from windows shell - mongoexport --db dbname --collection collectionname --out outfile.json

# You should ideally run this script only once.

import json
import urllib2
import httplib
import pymongo
import numpy
from pprint import pprint

# Connect to MongoDB local instance
try:
	con = pymongo.MongoClient()# Conenct to local instance running at 'localhost:27017'
	print "Connection established"
except pymongo.errors.ConnectionFailure,e:
	print "Could not establish connection to mongodb instance %s" % e

# Create database
db = con.lastfmdb
print db 

# Get available databases
print con.database_names()

#insert test collections in lastfmdb database
test_collection = db.test_junk_collection
print test_collection

# Check data in collections
print db.collection_names()

# Last FM API credentials
main_url = "http://ws.audioscrobbler.com/2.0/" # base URL for REST call
# api_key =  Get from JSON object 

""" following block gets tags for artist names and inserts them in a mongodb collection"""

temp_tags_1000 = db.temp_tags_1000 # create a mongodb collection for tags
f = open('artists_list.txt','r').read().split('\n') # open the file with names of all artists and read it line by line

for line in f:
	current_artist = line
	current_artist = current_artist.replace(" ", "%20") # handle space encoding to get valid URL
	current_artist.rstrip('\n') # get rid of any new line or whitespace
	
	try:
		print "x " + current_artist  #debug artist name sent
		#time.sleep(30) # optional time dealy to prevent API call abuse
		tag_request = None
		tag_data = None
		temp_dict ={} # empty dict
		tag_request = urllib2.urlopen(main_url+'?method=artist.gettoptags&artist='+current_artist+'&api_key='+api_key+'&format=json')
		print tag_request.getcode() # print request status(optional)
		temp_dict=json.load(tag_request) # load json data in the dict
		post_id = temp_tags_1000.insert(temp_dict) #use insert() method to insert dicts into mongodb collection
				
	except httplib.BadStatusLine: # handle bad status repsonse froom last fm server
		pass

print db.temp_tags_1000.count() # no of documents in collection
f.close()




