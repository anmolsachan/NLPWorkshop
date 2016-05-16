from datetime import datetime
import time
import MySQLdb 
import datetime
import config
import datetime
import sys
db = MySQLdb.connect(config.host,config.user,config.passwd,config.db)
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
	if len(b)<=1:
		b=a
	host=b[0]
	c=b[0].split(".")[::-1]
	id=".".join(c)+":http/"+b[1]
	return host,id
def fil(item):
	return str(filter(lambda x:ord(x)>31 and ord(x)<128 ,item))
	
def source_creator(dic):
	idhost=url_convert(dic["url"])
	if not dic["description"]:
		dic["description"]=""
	if not dic["h123"]:
		dic["h123"]=""
	if not dic["body"]:
		dic["body"]==""
	if not dic["meta"]:
		dic["meta"]=""
	content=dic["description"]+" "+dic["h123"]+" "+str(dic["body"]) 
	
	source={
		"host":fil(idhost[0]),
		"lastModified":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"digest":"",
		"tstamp":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"date": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"type":"text/html ",
		"url":fil(dic["url"]),
		"anchor":"",
		"id":fil( idhost[1]),
		"title":fil(dic["title"]),
		"meta_description": fil(dic["description"]),
		"meta_keywords":fil(dic["meta"]),
		"boost": "0",
		"cache" :"content",
		"content":fil(content ),
		"content_length":len(content)
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
		'_index': 'nutch',
		'_type': 'doc',
		'_id':id,
		'_source':source
		    }
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
		fil=open("failed_25000.txt","a")
		errors=open("errors.txt","a") 
		fil.write(str(i))
		errors.write(str(actions))
		errors.write("\n")
		errors.write(str(e))
		errors.write("\n")
		fil.write("\n")
		fil.close()
		errors.close()
	print datetime.datetime.now()

def main(table):
	i=17870400
	
	while True:
		data=getData(table,i,25000)
		print"MYSQL fetch"
		global es
		es = Elasticsearch('192.168.101.5:9200',timeout=30)
		if not data:
			break
		insertElastic(data,i)
		i+=25000

#print url_convert(url)
main("newIndex2")
	

