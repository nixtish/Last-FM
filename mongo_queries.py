""" This script carries out various operations of the mongodb artists tag collection 
	Assume that local mongodb instance in running
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

# ----Functions-----

def listAllDocs(collection_name):
	""" Method to list all documents in a collection, 
	    takes object of the collection as a parameter"""
	for doc in collection_name.find():
		pprint (doc)
        print collection_name.count()


def findMostPopularTags(collection_name, weighted_threshold):
	"""This Method return the most popular tags and their confidence rating which is how many times
	these tags occured in the dataset and they were also has  a tag count above the weighted_threshold """
	pass


def getOneTag(collection_name):
	""" Method to get one value with a given key test"""
	for doc in existing_test_collection.find({"toptags.tag.count":"100"}): # FINALLy
	    pprint (doc)        

# Query to return common tags in each document
# def returnCommonTags():
#   """ Method to return common tags amongst the documents in the collection"""
#     for doc in existing_test_collection.collection.distinct():
    	

	    

# for doc in existing_test_collection.find({}, {"artists":"Rihanna"}): # try if exists
# 	pprint (doc)





#Function Calls (Just toggle the comments to execute any function you want)

# listAllDocs(existing_test_collection)                     # List all documents in a collection.
# returnCommonTags(existing_test_collection)                # Returns common tags amongst documents
getOneTag(existing_test_collection)						    # Just get one tag based on the request	