from elasticsearch import Elasticsearch
import os
import json

es = Elasticsearch(["http://10.1.94.103:9200/"])

for f in os.listdir('baltimore'):
  lines = open('baltimore/' + f).readlines()
  #print "".join((lines))
  es.bulk(body="".join((lines)))
