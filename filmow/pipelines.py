# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2 as pg
import pycountry as pc
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
		self.curr.execute(""" DROP TABLE IF EXISTS "movie_to_genres" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "movie_to_countries" """)	
		self.curr.execute(""" DROP TABLE IF EXISTS "ratings" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "users" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "movies" """)
		self.curr.execute(""" DROP TABLE IF EXISTS "genres" """)
		

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movies" ( "movie_tag"	NUMERIC NOT NULL UNIQUE,\
			"year"	INTEGER,\
			"genres"	TEXT,\
			"title" VARCHAR(100) NOT NULL,\
			"runtime" INTEGER,\
			PRIMARY KEY("movie_tag"))""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "users" ("username"	VARCHAR(50) NOT NULL UNIQUE,\
			"name"	VARCHAR(50),\
			"age" INTEGER,\
			"city" VARCHAR(30),\
			"seen_count" INTEGER CHECK( seen_count > 0),\
			"id" SERIAL PRIMARY KEY)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "ratings" ("user_id"	INTEGER NOT NULL,\
			"movie_tag"	NUMERIC NOT NULL,\
			"rating"	REAL NOT NULL,\
			PRIMARY KEY("user_id","movie_tag"),\
			CONSTRAINT userid_fkey FOREIGN KEY (user_id) REFERENCES users (id) MATCH SIMPLE \
      		ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "genres" ("id"	SERIAL PRIMARY KEY,\
			"genre"	VARCHAR(30) UNIQUE)""")

		genres = open("filmow/genres.txt").read().splitlines()
		for gen in genres:
			self.curr.execute(""" INSERT INTO genres(genre) VALUES (%s)""", (gen,))
		#end for

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movie_to_genres" ("movie_tag"	NUMERIC NOT NULL,\
			"genre_id"	INTEGER NOT NULL,\
			PRIMARY KEY("movie_tag","genre_id"),\
			CONSTRAINT movietag_fk FOREIGN KEY (movie_tag) REFERENCES movies (movie_tag) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION,\
			CONSTRAINT genreid_fk FOREIGN KEY (genre_id) REFERENCES genres (id) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movie_to_countries" ("movie_tag"	NUMERIC NOT NULL,\
			"Country"	VARCHAR(50) NOT NULL,\
			PRIMARY KEY("movie_tag","Country"),\
			CONSTRAINT movietag_fk FOREIGN KEY (movie_tag) REFERENCES movies (movie_tag) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.conn.commit()

	def process_item(self, item, spider):
		if isinstance(item, UserItem):
			if item['ratings']:
				user_id = self.store_user(item['username'], item['name'], item['age'], item['city'], item['seen_count'])
				print("User {username} {user_id} : {rats} ratings ".format(username=item['username'], user_id=user_id,rats=len(item['ratings'] )))
				ratings = [(user_id,r[1],r[2]) for r in item['ratings']]

				self.store_ratings(ratings)
				#for rat in item['ratings']:
				#	self.store_rating(item['username'],rat[0], rat[1])
			else:
				print("User {n} : 0 ratings".format(n=item['username']))
			#end if
		elif isinstance(item, MovieItem):
			pass
		return item
    #end process_item

	def store_user(self, username, name, age, city, seen_count):
		id = -1
		try:
			self.curr.execute(""" INSERT INTO users(username, name, age, city, seen_count) VALUES (%s,%s,%s,%s,%s) RETURNING id""", (username,name,age,city,seen_count))
			id = self.curr.fetchone()[0]
		except pg.IntegrityError as e:
			print("User already in the DB")
			self.curr.execute(""" SELECT id FROM users WHERE username LIKE %s;""", (username,))
			id = self.curr.fetchone()[0]
		finally:
			self.conn.commit()
		return id
	#end store_user

	def store_ratings(self, ratings):
		try:
			f = self.create_ratings_iterator(ratings)
			self.curr.copy_from(f, 'ratings', columns=('user_id', 'movie_tag', 'rating'), sep="|")
			#self.curr.execute(""" INSERT INTO ratings(username, movie_tag, rating) VALUES (?,?,?)""", (username,movie_tag,float(rat)))
		except pg.IntegrityError as e:
			print("Rating already in the DB")
		finally:
			self.conn.commit()

	def create_ratings_iterator(self, ratings):
		return StringIteratorIO( ('|'.join(map(clean_csv_value, (rat[0],rat[1],rat[2]))) + '\n' for rat in ratings ) )