# script to get top tags per artist and store it in a file.

import urllib2
import json
import httplib
import bson
import time

main_url = "http://ws.audioscrobbler.com/2.0/" # base URL for REST call
#api_key = get from jSON 
post_count = 0 # debug counter to check whether the loop runs correctly
pre_count = 0
for_count = 0 

f = open('artists_list.txt','r').read().split('\n')# open the file with names of all artists and read it line by line

for line in f:
	current_artist = line
	for_count += 1
	current_artist = current_artist.replace(" ", "%20")# required to prevent bad request URLS
	current_artist.rstrip('\n')
	
	try:

		print "x " + current_artist + " %s " % (for_count) #debug artist name sent
		#time.sleep(3)# purposeful time to delay to prevent call abuse
		tag_request = None
		tag_data = None
		pre_count += 1
		request_string = main_url+'?method=artist.gettoptags&artist='+current_artist+'&api_key='+api_key+'&format=json'
		request_string = request_string.strip()
		tag_request = urllib2.urlopen(request_string)
		# print tag_request.response() # get response header
		print request_string
		print tag_request.getcode() # debug check to see which requesta re failing
		tag_data = json.load(tag_request)
		post_count += 1
		with open ('top_artists_tags_megadump','a') as output_file: #dummy file to store all values
			json.dump(tag_data, output_file)

	except httplib.BadStatusLine: # handle bad status repsonse froom last fm server
		pass
	

print for_count
print pre_count
print post_count

# check with 20% for spcae for calexico why only 317 entries.
# Just check one URL 1000 times and see if counter values are alright
# check status messages for each request as well to see whether its returning something or not.
# Possible encoding issue
# instead of iterating over test , iterate over original json object and pass values of "artist" key as  current client input