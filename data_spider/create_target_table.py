#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime


class DataHandle(object):
	def __init__(self):
		pass
	def Init(self, passwd, db):
		#self.conn = conn
		#self.sql = sql
		try:
			conn = MySQLdb.connect(host = '127.0.0.1'
 					, user = 'root'
					, passwd = passwd
		 			, db = db )
			return conn
		except Exception as e:
			print str(e)
			return -1
	
	def closeDB(self,conn):
		try:
			if conn:
		 		conn.close()
				print "conn close!"
		except Exception as e:
				print str(e)
	def CreateTable(self,conn,tablename):
		cur = conn.cursor()
		
		sqlselect = 'show tables;'
		cur.execute(sqlselect)
		tup = cur.fetchall()
		if (tablename,) not in tup:
			cur.execute("create table %s (id int unsigned auto_increment, title varchar(100) not null, score float(2), num int, link varchar(200) not null, time date, address varchar(50), other_release varchar(100), actors varchar(1000), primary key(id))default charset=utf8;" % tablename)

			#cur.execute('create table %s (id int(50) not null AUTO_INCREMENT, title varchar(100), score float(2), num int(8), link varchar(200), time date, address varchar(50), other_release varchar(100), actors varchar(1000), primary key(id));' % tablename)
		conn.commit()
		cur.close()
	def DataInsert(self, conn, table, oldtable):
		cur = conn.cursor()
		cur.execute("set names utf8")
		i = 0
		limit = 1
		links_seen = set()
		while 1:
			cur.execute("select * from %s " % oldtable + "limit %s, %s;" % (i,limit))#逐条获取数据
			row = cur.fetchall()
			i = i + 1
			#判断结果为空就结束循环
			if row:
				pass
			else:
				break
			#去重，如果重复则跳过此循环
			link = None
			link = ''.join(row[0][3]).strip()
			
			if link in links_seen:
				continue
			else:
				links_seen.add(link)
			
			title = None
			title = ''.join(row[0][0]).strip()
			
			#score = None
			s = ''.join(row[0][1]).strip()
			
			if self.HasNum(s):
				score = float(s)
			else:
				score = None
			
			
			n = ''.join(row[0][2]).strip()
			if n:
				#num = filter(lambda s: s in '0123456789', n) #只保留数字
				num = filter(str.isdigit,n)    #只保留数字，上面的也可以用
			
				num = int(num)
			else:
	 			num = None
				score = None

			other_release = None
			temp_time = ''.join(row[0][4]).strip()
			#如果数据中含有数字的话，那么将数字写入time中，将字符写入address中
			if self.HasNum(temp_time):
				
				#time = temp_time[:10]
				#address = temp_time[11:-1]
				
 				time = filter(lambda x: x in '0123456789-', temp_time)
			 	address = filter(lambda x: x not in '0123456789-()', temp_time)
			#如果数据中没有数字，则time为空，将括号里面的字符写入address，若无括号，address置空，数据写入other_release中
			else:
 			 	time = None
				if '()' in temp_time:
					address = filter(lambda x: x not in '()', temp_time)
				else:
					address = None
					other_release = temp_time
			
			#actors = ''.join(row[0][5]).strip()
			actor = row[0][5]
			#因为actor是一个里面含有一个列表的字符串，所以先将其转换为list
			actors = actor.replace(']','').replace('[','').replace("\'","").split(",")
			
			for element in actors:
				element = element.strip()

			for element in actors:
				if self.HasNum(element):
					if other_release:
				 		other_release = other_release + ';' + element #将含有数字（也就是日期）的字符串存入other_release
					else:
						other_release = element
					actors.remove(element)
			
			actor = ''.join(actors)
			
			#插入表中
			
			#cur.execute("insert into %s(title,score,num,link,time,address,other_release,actors) values (\'%s\',%s,%s,\'%s\',str_to_date(\'%s\','%%Y-%%m-%%d'),\'%s\',\'%s\',\"%s\");" % (table,title,score,num,link,time,address,other_release,actor))
			cur.execute("insert into " + table + " (title,score,num,link,time,address,other_release,actors) values (%s,%s,%s,%s,str_to_date(%s,'%%Y-%%m-%%d'),%s,%s,%s);", (title,score,num,link,time,address,other_release,actor))
			
			
			'''
			if num and time and other_release:
				cur.execute("insert into %s(title,score,num,link,time,address,other_release,actors) values (\'%s\',%s,%s,\'%s\',str_to_date(\'%s\','%%Y-%%m-%%d'),\'%s\',\'%s\',\"%s\");" % (table,title,score,num,link,time,address,other_release,actor))
			elif time and address and other_release:
				cur.execute("insert into %s(title,link,time,address,other_release,actors) values (\'%s\',%s,str_to_date(\'%s\','%%Y-%%m-%%d'),\'%s\',\'%s\',\"%s\");" % (table,title,link,time,address,other_release,actor))
			else address:
				cur.execute("insert into %s(title,num,link,other_release,actors) values (\'%s\',%s,\'%s\',\'%s\',\"%s\");" % (table,title,num,link,other_release,actor))
		'''
			conn.commit()
		cur.close()
		print "cur close!"
		self.closeDB(conn)

	def HasNum(self,S):
		return any(char.isdigit() for char in S)
		

if __name__ == "__main__":
	data = DataHandle()
	passwd = "186386"#密码
	db = "douban"					#数据库
	conn = data.Init(passwd, db)
	if conn != -1:
		table = "douban_movie"
		old = "douban_mov_bak"
		data.CreateTable(conn,table)#新表的名字
		data.DataInsert(conn,table, old)#新旧表的名字






