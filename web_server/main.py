#!/bin/python
#-*- coding: utf-8 -*-
import web
import hashlib
import lxml
import time
import os
import random
import pymysql

import reply
import receive

urls = (
	'/', 'Main',
)
class Main(object):
	def GET(self):
		try:
			data = web.input()
			if len(data) == 0:
				return "hello, this is handle view"
			signature = data.signature
			timestamp = data.timestamp
			nonce = data.nonce
			echostr = data.echostr
			token = "HelloChj" #按照公众平台官网\基本配置中信息填写

			list = [token, timestamp, nonce]
			list.sort()
			sha1 = hashlib.sha1()
			map(sha1.update, list)
			hashcode = sha1.hexdigest()
			print "handle/GET func: hashcode, signature: ", hashcode, signature
			if hashcode == signature:
				return echostr
			else:
				return ""
		except Exception, Argument:
			return Argument
	def update_user_info(self, user_name):
		self.db = pymysql.connect(host='localhost', port=3306, user='root', password='186386', db='douban', charset='utf8')
		self.cursor = self.db.cursor()
		cmd = 'select * from user_info where wx_id = "{}";'.format(user_name)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		if len(results) == 0:
			cmd = 'insert into user_info(wx_id, start_time) values("{}", "{}");'.format(user_name, int(time.time()))
			try:
				self.cursor.execute(cmd)
				self.db.commit()
			except:
				self.db.rollback()
	def parse_cmd(self, recv_content):
		recv_msg_buf = recv_content.split(' ')#格式标准化
		recv_msg = []
		for buf in recv_msg_buf:
			if buf != "":
				recv_msg.append(buf.strip())
		return recv_msg
	def evaluate(self, user_name, recv_msg):
		content = ""
		movie_name = recv_msg[1]
		#可能需要处理
		nice = float(recv_msg[2])
		if nice > 10:
			nice = 10
		elif nice < 0:
			nice = 0
		cmd = 'select id from user_info where wx_id = "{}";'.format(user_name)#记录到数据库
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		user_id = results[0][0]
		cmd = 'select id from douban_movie where title = "{}";'.format(movie_name)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		if len(results) == 0:
			content = "抱歉，电影名输入有误，请重新输入。"
		else:

			for row in results:
				movie_id = row[0]
				cmd = 'select liking from like_movie where user_id={} and movie_id={};'.format(user_id, movie_id)
				self.cursor.execute(cmd)
				results = self.cursor.fetchall()
				if len(results) == 0:
					cmd = 'insert into like_movie(user_id, movie_id, liking) values({}, {}, {});'.format(user_id, movie_id, nice)
					try:
						self.cursor.execute(cmd)
						self.db.commit()
						content = '评价成功，感谢您的支持。{}:{}分'.format(movie_name, nice)
					except:
						self.db.rollback()
						content	= '评价失败，请重新输入。{}:{}分'.format(movie_name, nice)
				else:
					cmd = 'update like_movie set liking={} where user_id={} and movie_id={};'.format(nice, user_id, movie_id)
					try:
						self.cursor.execute(cmd)
						self.db.commit()
						content = '更新评分成功。{}:{}分'.format(movie_name, nice)
					except:
						self.db.rollback()
						content	= '评价失败，请重新输入。{}:{}分'.format(movie_name, nice)
		return content
	def recommend(self, user_name, recv_msg):
		content = ""
		cmd = 'select id from user_info where wx_id="{}";'.format(user_name)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		if len(results) != 1:
			return content
		user_id = results[0][0]
		cmd = 'select * from like_movie where user_id={};'.format(user_id)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		line = {}
		if len(results):
			for row in results:
				movie_id = row[1]
				score = row[2] if row[2] != None else -1
				line[movie_id] = score
		
		cmd = 'select id from user_info where wx_id<>"{}";'.format(user_name)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		areas = {}
		for other_user in results:#遍历每一个用户
			cmd = 'select * from like_movie where user_id={};'.format(other_user[0])
			self.cursor.execute(cmd)
			results = self.cursor.fetchall()
			line_other = {}
			if len(results):
				for row in results:
					movie_id = row[1]
					score = row[2] if row[2] != None else -1
					line_other[movie_id] = score
			tup = self.compute(line, line_other)
			areas[other_user[0]] = tup
		neighbor_id = -1
		for (key, val) in areas.items():
			if val[0] == -1:
				del areas[key]
			elif neighbor_id == -1 and val[1] != 0:#首次赋值
				neighbor_id = key
			elif neighbor_id != -1 and val[1] != 0 and val[0] < areas[neighbor_id][0]:#更新
				neighbor_id = key
		print areas
		if neighbor_id == -1:
			return self.will(line)
			#return "抱歉，由于您的评价次数过少，系统暂时推算不出您的兴趣爱好。\n请多多评价，过段时间再试。"
		cmd = 'select * from like_movie where user_id={};'.format(neighbor_id)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		movies_id = []
		for row in results:
			if not line.has_key(row[1]):
				movies_id.append(row[1])
		if not len(movies_id):
			return self.will(line)
			#return "抱歉，由于您的评价次数过少，系统暂时推算不出您的兴趣爱好。\n请多多评价，过段时间再试。"
		for mov_id in movies_id:
			cmd = 'select * from douban_movie where id={};'.format(mov_id)
			self.cursor.execute(cmd)
			result = self.cursor.fetchone()
			title = result[1].encode("utf-8") if result[1] != None else ""
			score = result[2] if result[2] != None else 0
			num = result[3] if result[3] != None else 0
			link = result[4].encode("utf-8") if result[4] != None else ""
			date_time = result[5] if result[5] != None else ""
			address = result[6].encode("utf-8") if result[6] != None else ""
			other_address = result[7].encode("utf-8") if result[7] != None else ""
			actors = result[8].encode("utf-8") if result[8] != None else ""
			if score:
				content += '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n评分:{}\n{}\n\n'.format(title, date_time, address, other_address, actors, num, score, link)
			else:
				content += '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n{}\n\n'.format(title, date_time, address, other_address, actors, num, link)
		return content
	def compute(self, line, line_other):
		area=0.0
		common = dict.fromkeys([x for x in line if x in line_other])
		common_num = len(common)
		x = len(line) - common_num
		y = len(line_other) - common_num
		if 0 == common_num:
			area = -1
		else:
			for key in common:
				area += (line[key] - line_other[key])**2
			area = (area*x)/(common_num**2)
		return (area, y)#area越小越相近,-1为无交集,y为可推荐的数量
	def will(self, line):
		cmd = 'select count(id) from douban_movie;'
		self.cursor.execute(cmd)
		result = self.cursor.fetchone()
		movie_sum = result[0]
		movie_sum -= 100
		rand = random.randint(0, movie_sum)
		cmd = 'select * from douban_movie limit {},100;'.format(rand)
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		max_score_addr = 0
		max_score = 0.0
		index = 0
		for row in results:
			if row[2] != None and row[2] > max_score and not line.has_key(row[0]):
				max_score_addr = index
				max_score = row[2]
			index += 1
		title = results[max_score_addr][1].encode("utf-8") if results[max_score_addr][1] != None else ""
		score = results[max_score_addr][2] if results[max_score_addr][2] != None else 0
		num = results[max_score_addr][3] if results[max_score_addr][3] != None else 0
		link = results[max_score_addr][4].encode("utf-8") if results[max_score_addr][4] != None else ""
		date_time = results[max_score_addr][5] if results[max_score_addr][5] != None else ""
		address = results[max_score_addr][6].encode("utf-8") if results[max_score_addr][6] != None else ""
		other_address = results[max_score_addr][7].encode("utf-8") if results[max_score_addr][7] != None else ""
		actors = results[max_score_addr][8].encode("utf-8") if results[max_score_addr][8] != None else ""
		content = '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n评分:{}\n{}\n为提高您的推荐质量，请您多使用评价功能。另外，您评价过的电影不会再次推荐给您。\n'.format(title, date_time, address, other_address, actors, num, score, link)
		return content
	def search(self, user_name, recv_msg):
		return self.browse(user_name, recv_msg[1:])
	def browse(self, user_name, recv_msg):
		movie_name = recv_msg[0]
		content = ""
		cmd = 'select * from douban_movie where title like "{}";'.format(movie_name)#精准查找
		self.cursor.execute(cmd)
		results = self.cursor.fetchall()
		#print 'select "{}" have results num:{}.'.format(movie_name, len(results))
		if len(results):
			for row in results:
				title = row[1].encode("utf-8") if row[1] != None else ""
				score = row[2] if row[2] != None else 0
				num = row[3] if row[3] != None else 0
				link = row[4].encode("utf-8") if row[4] != None else ""
				date_time = row[5] if row[5] != None else ""
				address = row[6].encode("utf-8") if row[6] != None else ""
				other_address = row[7].encode("utf-8") if row[7] != None else ""
				actors = row[8].encode("utf-8") if row[8] != None else ""
				if score:
					content += '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n评分:{}\n{}\n\n'.format(title, date_time, address, other_address, actors, num, score, link)
				else:
					content += '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n{}\n\n'.format(title, date_time, address, other_address, actors, num, link)
			cmd = 'select id from user_info where wx_id = "{}";'.format(user_name)#更新查找记录
			self.cursor.execute(cmd)
			results = self.cursor.fetchall()
			user_id = results[0][0]
			cmd = 'select id from douban_movie where title = "{}";'.format(movie_name)
			self.cursor.execute(cmd)
			results = self.cursor.fetchall()
			for row in results:
				movie_id = row[0]
				cmd = 'insert into seek_movie(user_id, movie_id, seek_time) values({}, {}, {})'.format(user_id, movie_id, int(time.time()))
				try:
					self.cursor.execute(cmd)
					self.db.commit()
				except:
					self.db.rollback()
		else:#模糊查找
			cmd = 'select * from douban_movie where title like "%{}%" limit 5;'.format(movie_name)
			self.cursor.execute(cmd)
			results = self.cursor.fetchall()
			print 'like "{}" have results num:{}.'.format(movie_name, len(results))
			if len(results) == 0:
				content = "抱歉，暂时没有收录该影片。"#找不到
			else:
				content = "您在找的可能是：\n"#模糊匹配到
				for row in results:
					title = row[1].encode("utf-8") if row[1] != None else ""
					score = row[2] if row[2] != None else 0
					num = row[3] if row[3] != None else 0
					link = row[4].encode("utf-8") if row[4] != None else ""
					date_time = row[5] if row[5] != None else ""
					address = row[6].encode("utf-8") if row[6] != None else ""
					other_address = row[7].encode("utf-8") if row[7] != None else ""
					actors = row[8].encode("utf-8") if row[8] != None else ""
					content += '{}\n{}\n{}\n{}\n{}\n评价人数:{}\n评分:{}\n{}\n\n'.format(title, date_time, address, other_address, actors, num, score, link)
		return content

	def on_text(self, recMsg):
		content = ""
		user_name = recMsg.FromUserName
		recv_content = recMsg.Content
		self.update_user_info(user_name)#检查用户是否存在，不存在则创建
		
		recv_msg = self.parse_cmd(recv_content)#解析收到的命令
		exe_cmd = recv_msg[0]
		content = ""
		if exe_cmd == '评价':
			content = self.evaluate(user_name, recv_msg)
		elif exe_cmd == '推荐':
			content = self.recommend(user_name, recv_msg)
		elif exe_cmd == '搜索':
			content = self.search(user_name, recv_msg)
		elif exe_cmd == '怎么用':
			content = "您可以发送以下内容给我：\n搜索 无问西东\n评价 秦时明月 8.9\n推荐\n怎么用"
		else:
			content = self.browse(user_name, recv_msg)
		self.db.close()
		return content

	def on_image(self, recMsg):
		content = "感谢您的图片，但是我暂时不能识别图片。。。"
		return content

	def on_event(self, recMsg):
		#print recMsg.Event
		content = ""
		if recMsg.Event == "subscribe":
			db = pymysql.connect(host='localhost', port=3306, user='root', password='186386', db='douban', charset='utf8')
			cursor = db.cursor()
			cmd = 'select * from user_info where wx_id = "{}";'.format(recMsg.FromUserName)
			cursor.execute(cmd)
			results = cursor.fetchall()
			if len(results) == 0:
				cmd = 'insert into user_info(wx_id, start_time) values("{}", "{}");'.format(recMsg.FromUserName, int(time.time()))
				try:
					cursor.execute(cmd)
					db.commit()
				except:
					db.rollback()
			db.close()
			content = "感谢您的关注与支持，评价电影超过一定次数后，本平台将为您提供个性化推荐服务。您可以发送以下内容给我：\n搜索 无问西东\n评价 秦时明月 8.9\n推荐\n怎么用"
			return content
		elif recMsg.Event == "unsubscribe":
			return content
		else:
			return content

	def POST(self):
		try:
			webData = web.data()
			#print webData
			content = ""
			recMsg = receive.parse_xml(webData)
			toUser = recMsg.FromUserName
			fromUser = recMsg.ToUserName
			if isinstance(recMsg, receive.Msg) and recMsg.MsgType == 'text':#主要业务逻辑
				content = self.on_text(recMsg)
			elif isinstance(recMsg, receive.Msg) and recMsg.MsgType == 'image':
				content = self.on_image(recMsg)
			elif isinstance(recMsg, receive.Msg) and recMsg.MsgType == 'event':
				content = self.on_event(recMsg)
			else:
				print "暂不支持"
				return "success"

			if content == "":
				return "success"
			replyMsg = reply.TextMsg(toUser, fromUser, content)
			data = replyMsg.send()
			return data
		except Exception, Argment:
			print "fail, but I pretend to be success."
			return "success"

if __name__ == '__main__':
	app = web.application(urls, globals())
	app.run()
