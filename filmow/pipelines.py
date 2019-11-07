# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2 as pg
from filmow.string_iterator import StringIteratorIO, clean_csv_value

class FilmowPipeline(object):

	def __init__(self):
		self.create_connection()
		self.create_tables()

	def create_connection(self):
		self.conn = pg.connect(user = "filmow_scrapper", password="filmow", host="127.0.0.1", port="5432", database="filmow")
		#database filmow user filmow_scrapper psswd filmow
		self.curr = self.conn.cursor()

	def create_tables(self):
		#comment these 3 lines while running in production
		self.curr.execute(""" DROP TABLE IF EXISTS "ratings" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "users" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "movies" """)	

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movies" ( "movie_tag"	VARCHAR(20) NOT NULL UNIQUE,\
			"year"	INTEGER,"genres"	TEXT,\
			"title" VARCHAR(100) NOT NULL,\
			PRIMARY KEY("movie_tag"))""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "users" ("username"	VARCHAR(50) NOT NULL UNIQUE,\
			"name"	VARCHAR(50),\
			"id" SERIAL PRIMARY KEY)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "ratings" ("username"	VARCHAR(50) NOT NULL,\
			"movie_tag"	VARCHAR(20) NOT NULL,\
			"rating"	REAL NOT NULL,\
			PRIMARY KEY("username","movie_tag"),\
			CONSTRAINT username_fkey FOREIGN KEY (username) REFERENCES users (username) MATCH SIMPLE \
      		ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.conn.commit()

	def process_item(self, item, spider):
		if item['ratings']:
			self.store_user(item['username'], item['name'])
			print("User {username} : {rats} ratings ".format(username=item['username'], rats=len(item['ratings'] )))
			self.store_rating(item['ratings'])
			#for rat in item['ratings']:
			#	self.store_rating(item['username'],rat[0], rat[1])
		else:
			print("User {n} : 0 ratings (not to be added)".format(n=item['username']))
		return item
    #end process_item

	def store_user(self, username, name):
		try:
			self.curr.execute(""" INSERT INTO users(username, name) VALUES (%s,%s)""", (username,name))
		except pg.IntegrityError as e:
			print("User already in the DB")
		finally:
			self.conn.commit()
	#end store_user

	def store_rating(self, ratings):
		try:
			f = self.create_ratings_iterator(ratings)
			self.curr.copy_from(f, 'ratings', columns=('username', 'movie_tag', 'rating'), sep="|")
			#self.curr.execute(""" INSERT INTO ratings(username, movie_tag, rating) VALUES (?,?,?)""", (username,movie_tag,float(rat)))
		except pg.IntegrityError as e:
			print("Rating already in the DB")
		finally:
			self.conn.commit()

	def create_ratings_iterator(self, ratings):
		return StringIteratorIO( ('|'.join(map(clean_csv_value, (rat[0],rat[1],rat[2]))) + '\n' for rat in ratings ) )