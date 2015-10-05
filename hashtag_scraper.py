import urllib2
import json
import time
import os
import datetime
from pattern.en import sentiment

def analyze(name):
	di = 'outputs/'
	output = open(di + name + '_posts.txt','w')
	hashout = open(di + name + '_hashtags.txt','w')
	#output.write('\t'.join(("id","tags","datetime","username","latitude","longitude","link","image_link","caption")) + '\n')
	for f in os.listdir(name):
		data = json.loads(open(name + '/' + f).read())
		for row in data['data']:
			output.write('\t'.join(getValues(row) + [name]).encode("ascii","ignore") + '\n')
			hashTags(hashout,row)

def hashTags(hashout,row):
	iid = 'inst_' + row['id']
	for tag in row['tags']:
		hashout.write('\t'.join((iid,tag.lower().encode('ascii','ignore'))) + '\n')
	

def getValues(j):
	latitude,longitude = '',''
	caption = ""

	iid = 'inst_' + j['id']
	if 'caption' in j and j['caption'] != None:
		#print j['caption']
		caption = j['caption']['text'].replace('\n',' ').replace('\t',' ')
	if 'location' in j and not j['location'] == None:
		if 'latitude' in j['location']:
			latitude = j['location']['latitude']
		if 'longitude' in j['location']:
			longitude = j['location']['longitude']
			#print 'here'
	return [j['user']['username'],
	iid,
	j['link'],
	datetime.datetime.fromtimestamp(float(j['created_time'])).isoformat(),
	str(sentiment(caption)[0]),
	str(False),
	"",
	caption,
	str(latitude),
	str(longitude)
	]

	'''
	','.join(j['tags']).encode("ascii","ignore"),
	
	
	str(latitude),
	str(longitude),
	
	j['images']['low_resolution']['url'],
	caption.replace('\n','  ').replace('\t','  ')
	]
	'''

def getData(name):
	
	#os.mkdir(name)
	response = urllib2.urlopen('https://api.instagram.com/v1/tags/' + name + '/media/recent?count=100&access_token=39050578.2974fce.9a9xace71fcxx93x4856b883a51b3f7ce746')
	call = json.loads(response.read())
	i = 0
	total = 0
	total += len(call['data'])
	open(name + '/' + str(i) + '.json','w').write(json.dumps(call))
	print name, total
	i += 1

	while 'next_url' in call['pagination']:
		time.sleep(.5)
		try:
			response = urllib2.urlopen(call['pagination']['next_url'])
			call = json.loads(response.read())
			total += len(call['data'])
			open(name + '/' + str(i) + '.json','w').write(json.dumps(call))
			print name, total
			i += 1
		except urllib2.URLError:
			print 'moving on...'


# need 'smartwatch','dronestrike','supercomputer','electronics', 'wearabletech','virtualreality','oculusrift','darpadrc','darparoboticschallenge','drcfinals',

names = ['baltimoreriots','freddiegray','blacklivesmatter','baltimoreprotest','baltimoreuprising']

# iot, wearables, wearabletechnology, hackathon, internetofthings, diyelectronics, machinelearning

if __name__ == '__main__':
	#getData(name)
	
	#names = ['smartwatch','supercomputer']
	for name in names:
		analyze(name)
	
	
	#for name in names:
	#	getData(name)
	
