from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import json
from datetime import datetime
import urllib
import os

users = {}
chunksize = 1000

def addUsers(response):
  for i in response["hits"]["hits"]:
    user = i["_id"]
    lat = i["fields"]["location.latitude"][0]
    lon = i["fields"]["location.longitude"][0]
    #42.334,-71.103,42.389,-70.989408
    if lat >= 42.334 - .005 and lon >= -71.103 - .005 and lat <= 42.389 + .005 and lon <= -70.989 + .005:
      pass
      #print 'Good'
    else:
      print 'Bad'
      es.delete(index="instagram_remap",doc_type="boston",id=user)
      print 'Done'
      #badfile.write(id+"\n")
    


es = Elasticsearch(['http://10.1.94.103:9200/'])
query={"size":chunksize,"fields":["_id","location.latitude", "location.longitude"], "query" : {"match_all" : {}}}
scanResp= es.search(index="instagram_remap", doc_type="boston", body=query, search_type="scan", scroll="10m")  
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
  
#deleteImages(users)
