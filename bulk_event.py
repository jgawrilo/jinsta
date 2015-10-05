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
output = open('ww_events.tsv','w')

es = Elasticsearch(['http://10.1.94.103:9200/'])

def indexEvents(x):
    
    geoh = x['geohash']
    lat = Geohash.decode_exactly(geoh)[0]
    lon = Geohash.decode_exactly(geoh)[1]
    for e in x['data']:
        rec = {}
        rec['geoloc'] = {'lat':lat, 'lon': lon}
        rec['geohash'] = geoh
        rec['tags'] = []
        rec['count'] = e['count']
        rec['datetime'] = e['event']
        rec['images'] = e['likes']
        print e
        for tag in e['tags'].keys():
            rec['tags'].append({"name":tag, "count":e['tags'][tag]})
	es.index(index='instagram_events_j_final',doc_type='dc',body=rec)        

def addUsers(response):
  for i in response["hits"]["hits"]:
    print id
    indexEvents(i['_source'])

query={"size":chunksize,"query" : {"match_all" : {}}}
scanResp= es.search(index="instagram_events_hres", doc_type="dc", body=query, search_type="scan", scroll="10m")  
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
  
