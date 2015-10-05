from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import json
from datetime import datetime
import urllib
import os

dir = '../socialsandbox/isil/'
users = {}
chunksize = 1000
badfile = open('bad_baltimore','w')
allfile = open('all.csv','w')
def deleteImages(users):
  howmany = 0 
  for id in users.keys():
    users[id]["geoloc"]["lat"] = users[id]["location"]["latitude"]
    users[id]["geoloc"]["lon"] = users[id]["location"]["longitude"]
    print 'yep'
    howmany += 1
    es.index(index="instagram_remap",doc_type="baltimore",id=id,body=users[id])
  print howmany

def addUsers(response):
  for i in response["hits"]["hits"]:
    lat = i['_source']["location"]["latitude"]
    lon = i['_source']["location"]["longitude"]
    madelat = i['_source']["geoloc"]["lat"]
    madelon = i['_source']["geoloc"]["lon"]
    if lat == madelon and lon == madelat:
      users[i['_id']] = i['_source']


es = Elasticsearch(['http://10.1.94.103:9200/'])
query={"size":chunksize,"query" : {"match_all" : {}}}
scanResp= es.search(index="instagram_remap", doc_type="baltimore", body=query, search_type="scan", scroll="10m")  
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
  
deleteImages(users)
