from datetime import datetime
import time
import MySQLdb 
import datetime
import config
import datetime
import sys
db = MySQLdb.connect(config.host,config.user,config.passwd,"images")
#import urllib2
counter = 0
import os,json
cursor = db.cursor(MySQLdb.cursors.DictCursor)
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = None

#print es.indices.create(index='test', ignore=400)


def getData(table,number,limit):
	sql = "SELECT * FROM %s LIMIT %s,%s"%(table,number,limit)
	cursor.execute(sql)
	data = cursor.fetchall()
	return data
def url_convert(url):
	a=url.replace("http://","").replace("https://","")
	b=a.split("/",1)
	host=b[0]
	c=b[0].split(".")[::-1]
	id=".".join(c)+":http/"+b[1]
	return host,id
def fil(item):
	return str(filter(lambda x:ord(x)>31 and ord(x)<128 ,item))
	
def source_creator(dic):
	for key,value in dic.items():
		if not value :
			value=""
	idhost=url_convert(dic["image"])
	source={
		"host":"thinkstockphotos.in",
		"lastModified":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"tstamp":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"date": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"type":"img",
		"url":fil(dic["image"]),
		"id":fil( idhost[1]),
		"title":fil(dic["description"]),
		"description":"thinkstockphotos www.thinkstockphotos.in",
		"boost": "0"
		}
	return source
	
def insertElastic(data,i):
	print "Indexing Step1"
	print datetime.datetime.now()
	actions = []
	for dat in data:
		source=source_creator(dat)
		id= source["id"]
		action={
		'_index': 'images',
		'_type': 'img',
		'_id':id,
		'_source':source
		    }
		#print json.dumps(action)
		actions.append(action)
	print "Indexing step2"
	print datetime.datetime.now()
	try:	
		print helpers.bulk(es,actions)
		print "Indexed "+str(i)+" to "+str(i+25000)	
		fil=open("counter.txt","w")
		fil.write(str(i+25000))
		fil.close()
		
	except Exception as e:
		print e
		print "failed "+str(i)+" to "+str(i+25000)
		fil=open("failed_new.txt","a")
		fil.write(str(i))
		fil.write("\n")
		fil.close()
	print datetime.datetime.now()

def main(table):
	fil=open("failed.txt","r").read().split("\n")
	for i in fil:
		i=int(i)
		print i
		data=getData(table,i,25000)
		print"MYSQL fetch"
		global es
		es = Elasticsearch('192.168.101.5:9200',timeout=60)
		if not data:
			break
		insertElastic(data,i)
		i+=25000
main("thinkstock")
	

