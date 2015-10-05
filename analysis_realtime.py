from elasticsearch import Elasticsearch

import tangelo
import cherrypy
import json
from datetime import datetime, timedelta
import numpy
from PIL import Image
import cv2
import argparse
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer




lat = None
lon = None

kafka =  KafkaClient("memex-kafka01:9092")
producer = SimpleProducer(kafka)
#GET /search/<fields>/<arg>/<arg>/...

def similarness(image1,image2):
    """
Return the correlation distance be1tween the histograms. This is 'normalized' so that
1 is a perfect match while -1 is a complete mismatch and 0 is no match.
"""
    # Open and resize images to 200x200
    i1 = Image.open(image1).resize((200,200))
    i2 = Image.open(image2).resize((200,200))

    # Get histogram and seperate into RGB channels
    i1hist = numpy.array(i1.histogram()).astype('float32')
    i1r, i1b, i1g = i1hist[0:256], i1hist[256:256*2], i1hist[256*2:]
    # Re bin the histogram from 256 bins to 48 for each channel
    i1rh = numpy.array([sum(i1r[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    i1bh = numpy.array([sum(i1b[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    i1gh = numpy.array([sum(i1g[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    # Combine all the channels back into one array
    i1histbin = numpy.ravel([i1rh, i1bh, i1gh]).astype('float32')

    # Same steps for the second image
    i2hist = numpy.array(i2.histogram()).astype('float32')
    i2r, i2b, i2g = i2hist[0:256], i2hist[256:256*2], i2hist[256*2:]
    i2rh = numpy.array([sum(i2r[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    i2bh = numpy.array([sum(i2b[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    i2gh = numpy.array([sum(i2g[i*16:16*(i+1)]) for i in range(16)]).astype('float32')
    i2histbin = numpy.ravel([i2rh, i2bh, i2gh]).astype('float32')

    return cv2.compareHist(i1histbin, i2histbin, 0)

def time(*args,**kwargs):
    global lat
    global lon
    return lat, lon


def search():
    now = datetime.now()
    five_hours_ago = now - timedelta(minutes=60)
    min_lat = 39.236
    max_lat = 39.373
    doc_type = "baltimore"
    min_lon = -76.706
    max_lon = -76.528
    es = Elasticsearch(['http://10.1.94.103:9200/'])
    res = es.search(index='instagram_remap', doc_type=doc_type, body={"sort" : [{ "created_time" : {"order" : "asc"}}], \
        "size":8000, "fields":["images.thumbnail.url","user.username","comments.data.username","likes.*","link","id","location.latitude","location.longitude", "created_time"], \
        "query": {"bool": {"must": [{ "range": { "location.latitude": { \
        "gte": min_lat, "lte": max_lat, "boost": 2 } } }, { "range": { "location.longitude": { \
        "gte": min_lon, "lte": max_lon, "boost": 2} } }, { "range": { "created_time": { \
        "gte": five_hours_ago.isoformat(), "boost": 2} } }] } },"partial_fields": {"part": {"include": "likes.data.username","include": "comments.data.from.username"}}})
    print len(res['hits']['hits'])
    producer.send_messages("instagram_event", str(len(res['hits']['hits'])))
    return
    start = None
    end = None
    for i in res['hits']['hits']:
        c = float(i['fields']['created_time'][0])
        dt = datetime.fromtimestamp(c)
        if not start:
            start = datetime(dt.year,dt.month,dt.day,dt.hour)
        end = datetime(dt.year,dt.month,dt.day,dt.hour)
        #return i
        #i1 = Image.open(str('/vagrant/insta/baltimore_images/' + i['fields']['id'][0] + '.jpg'))
        rounded = datetime(dt.year,dt.month,dt.day,dt.hour)
        creator = i["fields"]["user.username"][0]
        if "comments" in i["fields"]["part"][0]:
            for j in i["fields"]["part"][0]["comments"]["data"]:
                addEdge(nodes,edges,mygraph,j["from"]["username"],creator,"L")
        if "likes" in i["fields"]["part"][0]:
            for j in i["fields"]["part"][0]["likes"]["data"]:
                addEdge(nodes,edges,mygraph,j["username"],creator,"C")

        if m.get(rounded) == None:
            m[rounded] = 0
        m[rounded] += 1

    populateLinks(nodes,edges,mygraph)
    res['hourtimeseries'] = []
    res['graph'] = mygraph

    while start < end:
        if m.get(start) == None:
            res['hourtimeseries'].append({'x':int(start.strftime('%s')), 'y':0})
        else:
            res['hourtimeseries'].append({'x':int(start.strftime('%s')), 'y':m[start]})
        start = start + timedelta(hours=1)
    return res

search()
