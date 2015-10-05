from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import json
from datetime import datetime
import urllib
import os
import Geohash

chunksize = 10
output = open('waitwhat_images.tsv','w')

def addUsers(response):
  for i in response["hits"]["hits"]:
    text = ''
    lat = round(i['_source']["location"]["latitude"],3)
    lon = round(i['_source']["location"]["longitude"],3)
    ghash = Geohash.encode(lat,lon)
    link = i['_source']["link"]
    id = i['_id']
    print id
    datetime = i['_source']["created_time"]
    words = []
    if i['_source']['caption']:
      text = i['_source']['caption']['text'].encode('ascii','ignore').replace('\n',' ').replace('\t',' ')
      #words = i['_source']['caption']['text'].encode('ascii','ignore').replace('\n',' ').replace('\t',' ').lower().split()
    username = i['_source']['user']['username']
    output.write('\t'.join((id,str(lat),str(lon),ghash,link,str(datetime),username,text,i['_source']['images']['standard_resolution']['url'])) + '\n')

es = Elasticsearch(['http://10.1.94.103:9200/'])
query={"size":chunksize,"query" : {"match_all" : {}}}
scanResp= es.search(index="instagram_remap", doc_type="waitwhat", body=query, search_type="scan", scroll="10m")  
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
  
