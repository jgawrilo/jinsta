from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import os
import json
from datetime import datetime
import urllib
import os
import cv2
import sys


faceCascade = cv2.CascadeClassifier("/home/ubuntu/FaceDetect-master/haarcascade_frontalface_default.xml")

def numFaces(imagePath): # Create the haar cascade
  # Read the image
  image = cv2.imread(imagePath)

  # Detect faces in the image
  try:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    faces = faceCascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(30, 30),
        flags = cv2.cv.CV_HAAR_SCALE_IMAGE
    )
  except:
    print 'cv2 error'
    return 0
  return len(faces)

chunksize = 10
output = open('faces.tsv','w')

def addUsers(response):
  for i in response["hits"]["hits"]:
    image = i['_source']["images"]["standard_resolution"]["url"]
    id = i["_id"]
    link = i['_source']["link"]
    print image
    ext = image.split('/')[-1].split('.')[1]
    try:
      if not os.path.isfile("waitwhat/" + id + "." + ext):
        urllib.urlretrieve(image, "waitwhat/" + id + "." + ext)
        faces = numFaces("waitwhat/" + id + "." + ext)
        output.write('\t'.join((str(id),str(image),str(link),str(faces))) + '\n')
      else:
        print 'already done..moving on'
    except:
      print 'other error...moving on'

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
  
