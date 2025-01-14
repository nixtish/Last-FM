# script to get top tags per artist and store it in a file.
# encoding=utf8
import urllib2
import json
import httplib
import bson
import time
import os
import sys  

# fix encoding issue
reload(sys)                  
sys.setdefaultencoding('Cp1252')

main_url = "http://ws.audioscrobbler.com/2.0/"                  # base URL for REST call
json_data = open(os.path.join(os.path.dirname(os.getcwd()),'lastfm_creds.json'))
data = json.load(json_data)
API_KEY =  data["creds"]["API Key"]                             # assign API KEY form JSON object

post_count = 0                                                  # debug counter to check whether the loop runs correctly
pre_count = 0
for_count = 0 

f = open('artists_list.txt','r').read().split('\n')             # open the file with names of all artists and read it line by line

for line in f:
	current_artist = line
	for_count += 1
	current_artist = current_artist.replace(" ", "%20")         # required to prevent bad request URLS
	current_artist.rstrip('\n')
	
	try:

		print "x " + current_artist + " %s " % (for_count)      # debug | artist name sent
		#time.sleep(3)                                          # purposeful time to delay to prevent call abuse
		tag_request = None
		tag_data = None
		pre_count += 1
		request_string = main_url+'?method=artist.gettoptags&artist='+current_artist+'&api_key='+API_KEY+'&format=json'
		request_string = request_string.strip()
		tag_request = urllib2.urlopen(request_string)
		print request_string
		print tag_request.getcode()                             # debug check to see which requesta re failing
		tag_data = json.load(tag_request)
		post_count += 1
		with open ('top_artists_tags_megadump1','a') as output_file: # dummy file to store all values
			json.dump(tag_data, output_file)

	except httplib.BadStatusLine:                               # handle bad status repsonse exception from last fm server
		pass
	
f.close()
print for_count
print pre_count
print post_count

# check with 20% for spcae for calexico why only 317 entries.
# Just check one URL 1000 times and see if counter values are alright
# check status messages for each request as well to see whether its returning something or not.
# Possible encoding issue
# instead of iterating over test , iterate over original json object and pass values of "artist" key as  current client input