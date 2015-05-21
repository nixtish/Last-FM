""" This script has methods to carry out various operations of the mongodb artists tag collection 
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


"""Methods to get various different types of results/information from the dataset"""

def listAllDocs(collection_name):
	""" Method to list all documents in a collection, 
	    takes object of the collection as a parameter"""
	for doc in collection_name.find():
		pprint (doc)
        print collection_name.count()
        print "\n"

def listAllArtists(collection_name):
	""" this method lists all artists by name in a given collection"""
	for doc in collection_name.find({},{"toptags.@attr.artist":1,"_id":0}):
		print doc
		print "\n"        

def getArtistsForTag(collection_name,tag_name):
	"""Method to get names of artists that have a particular tag.
		tag_name takes a string value"""
	for doc in collection_name.find({"toptags.tag.name":tag_name},{"_id":"1", "toptags.@attr.artist":"1"}):
		print doc
		print "\n"

def getArtistsForAnArrayOfTags(collection_name, tag_array):
	""" This method returns the names of artists that have the similar set of tags """
	for doc in collection_name.find({"toptags.tag.name": { '$all':tag_array}},{"_id":"1", "toptags.@attr.artist":"1"}):
		print doc

def getAllDistinctTags(collection_name):
	""" Test Method to get all distinct tags in the dataset"""
	for doc in collection_name.distinct("toptags.tag.name"):
		print doc		


""" To be fixed """

# def getSimilarArtistForaTagandCountValue(collection_name, tag_name,count_threshold):
# 	""" Method that would return a collection of artists that are similar based on the tag values,
# 	the results can be refined by increasing the count_threshold"""
# 	for doc in collection_name.find({'$and':{"toptags.tag.name":tag_name},{"toptags.tag.count":"100"}},{"_id":1}):
# 		pprint (doc)
	

# def getSimilarArtistsForAnArrayOfTagsAndCountValue(collection_name, tag_array, count_value):
# 	"""This method returns all artists that have a similar set of tags and also the tags that have a value greater than 
# 	a given count_value, a higher count value means higher fidelity of tags for the artists and thus the artists returned
# 	can be called similar or associated. Higher the count_value greater the artist association"""
# 	# here you have to specify that only for the tags in the array you have to check the count_value condition
# 	# Use $elemMatch
# 	"""
# 		collection_name.find( {
# 								"toptags.artist": {$all : [
# 														{"$elemMatch" : {"name":tag_array, "count": {'$gt': count_value}}},
														
# 														]}
# 			)
# 		}
# 	"""
# 	for doc in collection_name.find({"toptags.tag.name":{'$all':tag_array},{"toptags.tag.count"}},{"_id":"1", "toptags.@attr.artist":"1"}): 
# 		print "x"
# 		print doc

       #	for doc in collection_name.find({'$and': [{"toptags.tag.name":tag_array}, {"toptags.tag.count":{'$gte':count_value}}]},{"_id":"1","toptags.@attr.artist":"1"}):
 
def findMostFrequentTags(collection_name):
	"""This method returns most frequently occuring tags in the dataset and the number of times they appear
		as well as there total tag count."""
	# Use Map Reduce http://stackoverflow.com/questions/7408602/whats-the-best-way-to-find-the-most-frequently-occurring-value-in-mongodb
    # func_map = Code("function () {"
    # 	"this.toptags.tags.name"})
	pass

def findMostPopularTags(collection_name, weighted_threshold):
	"""This Method return the most popular tags and their confidence rating which is how many times
	these tags occured in the dataset and they also had a tag count above the weighted_threshold """
	pass

def computeSimilarityBetweenArtists(collection_name, artist_name_list):
    """ This method returns computed simila between artists passed as a list in the artist_name_list paramter """
    pass

def getSimilarArtist(collection_name, artist_name):
	""" This method return artist names similar to a given artist based on my similarity metric"""
	pass

def getSimilarTags(collection_name):
	"""Method to get similar tags from the dataset i.e tags that co-exist """
	pass


# Query to return common tags in each document
# def returnCommonTags(collection_name):
#   """ Method to return common tags amongst the documents in the collection"""
#     for doc in existing_test_collection.collection.distinct():
    	
# def getTagNamesAboveThreshold(collection_name, count_threshold):
# 	"""Method to return all tags above a certain tag_count threshold. Uses MongoDB aggregate query.
# 		count_threshold takes an integer value between 0 and 100 """
# 	collection_name.aggregate([{ $group : {_id: "toptags.tag.name":"pop", num_pop:{$sum:1}}}])

""" db.temp_tags_only1.aggregate([{$group: {_id:"$toptags.tag.name["pop"]", num:{$sum:1}}}]) """




"""Function Calls (Just toggle the comments to execute any function you want)"""

# listAllDocs(existing_test_collection)                     # List all documents in a collection.

# returnCommonTags(existing_test_collection)                # Returns common tags amongst documents

# getAllDistinctTags(existing_test_collection)				# Get all distinct tags in the dataset	

# getTagNamesAboveThreshold(existing_test_collection, 20)	# function that return tags are above a certain threshold

# listAllArtists(existing_test_collection)                  # method to list all artist names

getArtistsForTag(existing_test_collection, "pop")         # works

# getArtistsForAnArrayOfTags(existing_test_collection, ['pop'])

# getSimilarArtistsForAnArrayOfTagsAndCountValue(existing_test_collection, ['pop'], 0)

# getSimilarArtistForaTagandCountValue(existing_test_collection, "pop", 30)
