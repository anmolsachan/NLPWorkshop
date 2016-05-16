from datetime import datetime
import MySQLdb 
import config
db = MySQLdb.connect(config.host,config.user,config.passwd,config.db)
#import urllib2
counter = 0
import os,json
cursor = db.cursor(MySQLdb.cursors.DictCursor)
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch('192.168.101.5:9200')
#print es.indices.create(index='videos', ignore=400)



def getData(table,number,limit):
	sql = "SELECT * FROM %s WHERE moved!=1 LIMIT %s,%s"%(table,number,limit)
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
	global counter
	for dat in data:
		try:
			print counter
			dat.pop("moved")
			es = Elasticsearch("192.168.101.5:9200")
			es.index(index="videos", doc_type="doc", body=dat,timeout = 30)
			#os.system("""curl -XPOST "http://192.168.101.5:9200/videos/doc/%s" -d '%s' """%(str(counter),json.dumps(dat)))
			counter+=1
		except Exception as x:
			print x
		        pass

def main(table):
	while True:
		data=getData(table,0,100)
		if not data:
			break
		insertElastic(data)
		updateStatus(table,data)
#
def url_convert(url):
	a=url.split("")
	
main("videos")
	

