from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import json
from datetime import datetime
import urllib
import os
import sys


dir = sys.argv[1]
users = {}
chunksize = 1000

def pullImages(users):
  for id in users.keys():
    print id
    img_url = users[id]
    ext = img_url.split('/')[-1].split('.')[1]
    if os.path.isfile(dir + '/' + id + '.' + ext) == False:
      try:
        urllib.urlretrieve(img_url, dir + '/' + id + '.' + ext)
      except IOError:
        print 'Error...moving on'
    else:
      print 'image exists'

def addUsers(response):
  for i in response["hits"]["hits"]:
    user = i["_id"]
    if not user in users:
      users[user] = i["fields"]["images.thumbnail.url"][0]
    


es = Elasticsearch(['http://10.1.94.103:9200/'])
query={"size":chunksize,"fields":["_id","images.thumbnail.url"], "query" : {"match_all" : {}}}
scanResp= es.search(index="instagram_remap", doc_type=dir, body=query, search_type="scan", scroll="10m")  
scrollId= scanResp['_scroll_id']
response= es.scroll(scroll_id=scrollId, scroll= "10m")
addUsers(response)
count = len(response["hits"]["hits"])
scrollId = response['_scroll_id']
print count
while response["hits"]["hits"]:
  response= es.scroll(scroll_id=scrollId, scroll= "10m")
  addUsers(response)
  scrollId = response['_scroll_id']
  count += len(response["hits"]["hits"])
  print count
  
pullImages(users)
