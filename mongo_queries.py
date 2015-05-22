""" This script has various methods to carry out operations to interesting information from +the mongodb artists tag collections.
	Assume that local mongodb instance in running.
"""
import pymongo
import json
import time
from pprint import pprint
from bson.code import Code

""" Setup datasbe connection and get get collection object"""
try:
	con_obj = pymongo.MongoClient()                           # Conenct to local instance running at 'localhost:27017'
	print "Connection established"
except pymongo.errors.ConnectionFailure,e:
	print "Could not establish connection to mongodb instance %s" % e

# get existing database object
lastfm_db_obj = con_obj.lastfmdb                              # lastfmdb is the existing database on the local machine

# get collection object
existing_test_collection = lastfm_db_obj.temp_tags_only3      # here "temp_tags_only3" is the name of collection that exists in lastfmdb

distinct_tags = []											  # empty list to store all distinct tags

"""Methods to get various different types of results/information from the dataset"""

def listAllDocs(collection_name):
	""" Method to list all documents in a collection, 
	    takes object of the collection as a parameter"""
	for doc in collection_name.find():                        # find() method to show all documents
		pprint (doc)										  # pretty print
        print collection_name.count()
        print "\n"

def listAllArtists(collection_name):
	""" this method lists all artists by name in a given collection"""
	for doc in collection_name.find(
		{},
		{"toptags.@attr.artist":1,"_id":0}):                 # list all artists in the collection, suppress _id 
		print doc
		print "\n"        

def getArtistsForTag(collection_name,tag_name):
	"""Method to get names of artists that have a particular tag.
		tag_name takes a string value. This method doesn't take count_value into account"""
	for doc in collection_name.find(
		    {"toptags.tag.name":tag_name},                   # find tags that match a given tag_name
		    {"_id":"1", "toptags.@attr.artist":"1"}          # return artist name and object_id
		    ):
		print doc
		print "\n"

def getArtistsForAnArrayOfTags(collection_name, tag_array):
	""" This method returns the names of artists that have the similar set of tags """
	for doc in collection_name.find(
		    {
		        "toptags.tag.name":
		             { '$all':tag_array}},{"_id":"1", "toptags.@attr.artist":"1"} # use an array of tags to be matched
	    ):
		print doc

def getAllDistinctTags(collection_name):
	""" Test Method to get all distinct tags in the dataset"""
	for doc in collection_name.distinct("toptags.tag.name"):
		distinct_tags.append(doc)
		print doc		

def getSimilarArtistForaTagListandCountValue(collection_name, tag_list, count_threshold):
	""" Method that would return a collection of artists that are similar, based on the tag value,
	the results can be refined by increasing the count_threshold.

    This idea behind this similarity metric has two key points:
    	1) Since the tag count values are between 0 - 100 , 100 being the most popular tag for the artist, if we Use
			a high count_threshold then we can make sure that the tags being compared are of high quality.
		2) Since we are passing a list of tags that the artist must have, by coupling it with the above condition we can safely 
			say that the artists returned are similar.
	This approach may not be able to provide granular results but for a given set of tags and count_threshold, it will give good results.		 	
	"""
	
	for idx, val in enumerate(tag_list):
		tag_list[idx] = { 
		    "$elemMatch": 
		        {"name":tag_list[idx],
		         "count":{'$gte': count_threshold} 
		         }
		    }

	for doc in collection_name.find(
		    {"toptags.tag": {
		    "$all": tag_list}},
		    {"_id":1, "toptags.@attr.artist":"1"}):
		print doc
		print "\n"


  
	


"""Function Calls (Just toggle the comments to execute any function you want)
	existing_test_collection value has been fixed above but can optionally pass
	db.collection_name as an argument"""

# listAllDocs(existing_test_collection)                              # List all documents in a collection.

# listAllArtists(existing_test_collection)                           # method to list all artist names

# getArtistsForTag(existing_test_collection, "pop")                  # return artists that have the same tag

# getArtistsForAnArrayOfTags(existing_test_collection, ['pop'])      # pass a list of tags to which the artists belong

# getAllDistinctTags(existing_test_collection)				         # Get all distinct tags in the dataset	

# getSimilarArtistForaTagListandCountValue(existing_test_collection, ['pop','rnb'], 30) # method to get artists for a list of tags above a count threshold

# returnCommonTags(existing_test_collection)                         # Returns common tags amongst documents

# getTagNamesAboveThreshold(existing_test_collection, 20)	         # function that return tags are above a certain threshold










""" Unimplemented methods- To be fixed """
# aggregation /map reduce will be used in most of these. along with $unwind, $group -- READ MONGO DOC
"""



def findMostPopularTags(collection_name, count_threshold):
	# This Method return the most popular tags and their confidence rating which is how many times
	#  these tags occured in the dataset and they also had a tag count above the weighted_threshold 

	for idx, val in enumerate(distinct_tags):
		distinct_tags[idx] = { 
		    "$elemMatch": 
		        {"name":distinct_tags[idx],
		         "count":{'$gte': count_threshold} 
		         }
		    }
	for doc in collection_name.find(
		    {"toptags.tag": {
		    "$all": distinct_tags}},
		    {"_id":1, "toptags.tag.name":"1"}):
		print doc
		print "\n"	


def getTagNamesAboveThreshold(collection_name, count_threshold):
	#Method to return all tags above a certain tag_count threshold. Uses MongoDB aggregate query.
	#	count_threshold takes an integer value between 0 and 100 

	PIPELINE = [{"$unwind":"$toptags.tag"},{"$group":{"_id":"$toptags.tag.name", "count":{"$sum": 1}}}]
	list(collection_name.aggregate(PIPELINE))

	# WIll throw an error because of those "#text" values in the data set so $ unwind won't work, 
	#	should I remove those values from the dataset??? -- DONE BAD VALUES REMOVED
	# add count_threshold condition later, fix this for now


def findMostFrequentTags(collection_name):
	#This method returns most frequently occuring tags in the dataset and the number of times they appear,
	#	as well as there total tag count.

	# Use Map Reduce ??
	# emit on one key
	# and then sum across all documents ???    
    pass

def findMostPopularTags(collection_name, count_threshold):
	#This Method return the most popular tags and their confidence rating which is how many times,
	#   these tags occured in the dataset and they also had a tag count above the weighted_threshold 

	# Find a all such tags that have a count > count_threshold
	# once you have these calculate how many times such tag:count > count_threshold pairs existed across the dataset.
	# divide by total no of documents to get a confidence rating
	# can use aggregate and $group ??
	pass


def returnCommonTags(collection_name):
    # Method to return common tags amongst the documents in the collection

    # retuen set of most frequently co-existing tags 
    # so first find tags that co-exist -- VERY EXPENSIVE, can use count to reduce computations but still, worst case would be bad.
    # then find how many times they co-existed 
    pass

def getSimilarArtist(collection_name, artist_name):
	# This method return artist names similar to a given artist based on my similarity metric(defined in pdf)
	pass    

def computeSimilarityBetweenArtists(collection_name, artist_name_list):
    # This method returns computed simila between artists passed as a list in the artist_name_list paramter 

    # from the input list, compute similarity between each possible pair
    # iterate over list and compute similarity between two based on algorithm
    pass

""" 