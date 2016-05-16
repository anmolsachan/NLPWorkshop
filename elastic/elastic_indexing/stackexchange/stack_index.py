# -*- coding: utf-8 -*-
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
	sql = "SELECT * FROM `%s` LIMIT %s,%s"%(table,number,limit)
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
	if dic["description"]=="None":
		dic["description"]=""
	if dic["h123"]=="None":
		dic["h123"]=""
	if dic["body"]=="None":
		dic["body"]=""
	if dic["description"]=="N":
		dic["description"]=""
	if dic["h123"]=="N":
		dic["h123"]=""
	if dic["body"]=="N":
		dic["body"]=""
	content=dic["description"]+" "+dic["h123"]+" "+str(dic["body"]) 
	source={
		"host":idhost[0],
		"lastModified":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"digest":"",
		"tstamp":datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"date": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]+"Z",
		"type":"text/html ",
		"url":dic["url"],
		"anchor":"",
		"id": idhost[1],
		"title":dic["title"],
		"meta_description": "",
		"meta_keywords":dic["meta"],
		"boost": "0",
		"cache" :"content",
		"content":content ,
		"content_length":len(content),
		"score_host":0.95
		}
	
	return source
	
def insertElastic(data,i):
	print "Indexing Step1"
	print datetime.datetime.now()
	actions = []
	for dat in data:
		source=source_creator(dat)
		id= str(source["id"])

		action={
		'_index': 'nutch',
		'_type': 'doc',
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
		print "failed "+str(i)+" to "+str(i+25000)
		fil=open("failed.txt","a")
		fil.write(str(i))
		fil.write("\n")
		fil.write(str(e))
		fil.write("\n")
		print e
		fil.close()
	print datetime.datetime.now()

def main(table):
	i=25000
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
main("index")
	

