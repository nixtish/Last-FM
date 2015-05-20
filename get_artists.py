import requests
import json
import csv
import urllib2
import pymongo

# A function would look like this.

main_url = "http://ws.audioscrobbler.com/2.0/"

#api_key = # get from JSON
		

response = urllib2.urlopen(main_url+ '?method=chart.gettopartists&api_key='+api_key+'&format=json')
data = json.load(response)   
print data

# clean json object
# for element in data: 
#         del element['mbid'] 

# for i in xrange(len(data)):
#     if data[i]["ename"] == "mbid":
#         obj.pop(i)
#         break

#write json object to file 

# with open('data_wo_mbid.json', 'w') as outfile:
#     json.dump(data, outfile)  


