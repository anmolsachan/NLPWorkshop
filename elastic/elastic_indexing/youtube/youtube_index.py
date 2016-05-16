from datetime import datetime
import time
import MySQLdb 
import datetime
import config
import datetime
import sys
db = MySQLdb.connect(config.host,config.user,config.passwd,"yt")
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
	return id,host
def fil(item):
	return str(filter(lambda x:ord(x)>31 and ord(x)<128 ,item))
	
def source_creator(dic):
	for key,value in dic.items():
		if not value :
			value=""
	idhost=url_convert(dic["link"])
	source={
		"id":fil(idhost[0]),
		"domain":fil(idhost[1]),
		"description":fil(dic["description"]),
		"img":fil(dic["img_link"]) ,
		"title":fil(dic["title"]),
		"url":fil(dic["link"])
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
		'_index': 'videos',
		'_type': 'video',
		'_id':id,
		'_source':source
		    }
		#print action
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
		#errors=open("errors.txt","a") 
		fil.write(str(i))
		#errors.write(str(actions))
		#errors.write("\n")
		#errors.write(str(e))
		#errors.write("\n")
		fil.write("\n")
		fil.close()
		#errors.close()
	print datetime.datetime.now()

def main(table):
	fi=open("failed.txt","r").read().split("\n")
	for i in fi:
		i=int(i)
		data=getData(table,i,25000)
		print"MYSQL fetch"
		global es
		es = Elasticsearch('192.168.101.5:9200',timeout=30)
		if not data:
			break
		insertElastic(data,i)
		i+=25000
		

main("main_links")
	
