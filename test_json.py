""" Script to load credentials from a JSON object outside the current working directory to prevent 
		checking in credentials"""

import json
import os
json_data = open(os.path.join(os.path.dirname(os.getcwd()),'lastfm_creds.json'))
data = json.load(json_data)

API_KEY =  data["creds"]["API Key"]
Secret =  data["creds"]["Secret"]

print "%r" % (API_KEY)
print "%r" % (Secret)	

