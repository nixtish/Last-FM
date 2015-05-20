""" This script has methods to carry out various operations of the mongodb artists tag collection 
	Assume that local mongodb instance in running.
"""
import pymongo
import json
import time
from pprint import pprint

#  Connect to to local running instance
try:
	con_obj = pymongo.MongoClient()                           # Conenct to local instance running at 'localhost:27017'
	print "Connection established"
except pymongo.errors.ConnectionFailure,e:
	print "Could not establish connection to mongodb instance %s" % e

# get existing database object
lastfm_db_obj = con_obj.lastfmdb                              # lastfmdb is the existing database on the local machine

# get collection object
existing_test_collection = lastfm_db_obj.temp_tags_only3

# ----Functions-----#

def listAllDocs(collection_name):
	""" Method to list all documents in a collection, 
	    takes object of the collection as a parameter"""
	for doc in collection_name.find():
		pprint (doc)
        print collection_name.count()

def listAllArtists(collection_name):
	""" this method lists all artists by name in a given collection"""
	for doc in collection_name.find({},{"toptags.@attr.artist":1,"_id":0}):
		print doc        

def getArtistsForTag(collection_name,tag_name):
	"""Method to get names of artists that have a particular tag.
		tag_name takes a string value"""
	for doc in collection_name.find({"toptags.tag.name":tag_name},{"_id":"1", "toptags.@attr.artist":"1"}):
		print doc

def getArtistsForAnArrayOfTags(collection_name, tag_array):
	""" This method returns the names of artists that have the similar set of tags """
	for doc in collection_name.find({"toptags.tag.name": { $all:tag_array}},{"_id":"1", "toptags.@attr.artist":"1"})

def findMostPopularTags(collection_name, weighted_threshold):
	"""This Method return the most popular tags and their confidence rating which is how many times
	these tags occured in the dataset and they were also has  a tag count above the weighted_threshold """
	pass


def getOneTag(collection_name):
	""" Test Method to get one value with a given key test"""
	for doc in collection_name.find({"toptags.tag.count":"100"}): # FINALLy
	    pprint (doc)        

# Query to return common tags in each document
# def returnCommonTags(collection_name):
#   """ Method to return common tags amongst the documents in the collection"""
#     for doc in existing_test_collection.collection.distinct():
    	
# def getTagNamesAboveThreshold(collection_name, count_threshold):
# 	"""Method to return all tags above a certain tag_count threshold. Uses MongoDB aggregate query.
# 		count_threshold takes an integer value between 0 and 100 """
# 	collection_name.aggregate([{ $group : {_id: "toptags.tag.name":"pop", num_pop:{$sum:1}}}])

""" db.temp_tags_only1.aggregate([{$group: {_id:"$toptags.tag.name["pop"]", num:{$sum:1}}}]) """


# for doc in existing_test_collection.find({}, {"artists":"Rihanna"}): # try if exists
# 	pprint (doc)

def getSimilarArtist(collection_name, artist_name):
	""" This method return artist names similar to a given artist based on my similarity metric"""
	pass


"""Function Calls (Just toggle the comments to execute any function you want)"""

# listAllDocs(existing_test_collection)                     # List all documents in a collection.

# returnCommonTags(existing_test_collection)                # Returns common tags amongst documents

# getOneTag(existing_test_collection)						# Just get one tag based on the request	

# getTagNamesAboveThreshold(existing_test_collection, 20)	# function that return tags are above a certain threshold

# listAllArtists(existing_test_collection)                  # method to list all artist names

# getArtistsForTag(existing_test_collection, "pop")         # works

 getArtistsForAnArrayOfTags(existing_test_collection, ['pop', 'rnb'])
