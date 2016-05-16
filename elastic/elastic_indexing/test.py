from datetime import datetime
import MySQLdb 
import datetime
import config
import sys
db = MySQLdb.connect(config.host,config.user,config.passwd,config.db)
#import urllib2
counter = 0
import os,json
cursor = db.cursor(MySQLdb.cursors.DictCursor)
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = None
from elasticsearch import helpers

#print es.indices.create(index='test', ignore=400)



def getData(table,number,limit):
	sql = "SELECT * FROM %s LIMIT %s,%s"%(table,number,limit)
	cursor.execute(sql)
	data = cursor.fetchall()
	return data


def updateStatus(table,dicti):
	ids = []
	for dic in dicti:
		ids.append(str(dic["id"]))		
	
	sql = "UPDATE %s SET moved=1 WHERE `id` IN %s"%(table,str(tuple(ids)))
	cursor.execute(sql)
	db.commit()


def insertElastic(data):
	print datetime.datetime.now()
	print "Indexing Step1"
	actions = []
	for dat in data:
		id= dat["id"]
		dat.pop('id')
		action={
		'_index': 'video',
		'_type': 'doc',
		'_id':id,
		'_source':dat
		    }
		actions.append(action)
	print "Indexing step2"
	print datetime.datetime.now()
	print helpers.bulk(es,actions)
	print "Indexed"
	print datetime.datetime.now()
def main(table):
	i=0
	while True:
		print datetime.datetime.now()
		data=getData(table,i,100000)
		print datetime.datetime.now()
		print"MYSQL fetch"
		global es
		es = Elasticsearch('192.168.101.5:9200')
		if not data:
			break
		insertElastic(data)
		i+=100000
		#updateStatus(table,data)
	
main("videos")
	
