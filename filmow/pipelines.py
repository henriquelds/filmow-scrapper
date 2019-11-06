# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3

class FilmowPipeline(object):

	def __init__(self):
		self.create_connection()
		self.create_tables()

	def create_connection(self):
		self.conn = sqlite3.connect("filmow.db")
		self.curr = self.conn.cursor()

	def create_tables(self):
		#comment these 3 lines while running in production
		self.curr.execute(""" DROP TABLE IF EXISTS "ratings" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "users" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "movies" """)	

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movies" ( "movie_tag"	TEXT NOT NULL UNIQUE,"year"	INTEGER,"genres"	TEXT,"title"	TEXT NOT NULL,PRIMARY KEY("movie_tag"))""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "users" ("username"	TEXT NOT NULL UNIQUE,"name"	TEXT,"id"	INTEGER PRIMARY KEY AUTOINCREMENT)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "ratings" ("username"	TEXT NOT NULL,"rating"	REAL NOT NULL,"movie_tag"	TEXT NOT NULL,PRIMARY KEY("username","movie_tag"),FOREIGN KEY("username") REFERENCES "users"("username"))""")

	def process_item(self, item, spider):
		if item['ratings']:
			self.store_user(item['username'], item['name'])
			print("User {username} : {rats} ratings ".format(username=item['username'], rats=len(item['ratings'] )))
			for rat in item['ratings']:
				self.store_rating(item['username'],rat[0], rat[1])
		else:
			print("User {n} : 0 ratings (not to be added)".format(n=item['username']))
		return item
    #end process_item

	def store_user(self, username, name):
		try:
			self.curr.execute(""" INSERT INTO users(username, name) VALUES (?,?)""", (username,name))
		except sqlite3.IntegrityError as e:
			print("User already in the DB")
		finally:
			self.conn.commit()
	#end store_user

	def store_rating(self, username, movie_tag, rat):
		try:
			self.curr.execute(""" INSERT INTO ratings(username, movie_tag, rating) VALUES (?,?,?)""", (username,movie_tag,float(rat)))
		except sqlite3.IntegrityError as e:
			print("Rating already in the DB", (username,movie_tag,float(rat)))
		finally:
			self.conn.commit()