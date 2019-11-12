# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2 as pg
from filmow.string_iterator import StringIteratorIO, clean_csv_value
from filmow.items import MovieItem,UserItem
from scrapy.exceptions import DropItem
import sys

class FilmowPipeline(object):

	def __init__(self, mode):
		self.create_connection()
		self.create_tables(mode)

	@classmethod
	def from_crawler(cls, crawler):
		return cls(mode=crawler.settings.get('MODE'))

	def create_connection(self):
		self.conn = pg.connect(user = "filmow_scrapper", password="filmow", host="127.0.0.1", port="5432", database="filmow")
		#database filmow user filmow_scrapper psswd filmow
		self.curr = self.conn.cursor()

	def create_tables(self, mode):
		if mode == "movies-start":
			self.curr.execute(""" DROP TABLE IF EXISTS "movie_to_genres" """)
			self.curr.execute(""" DROP TABLE IF EXISTS "movie_to_countries" """)	
			self.curr.execute(""" DROP TABLE IF EXISTS "movie_to_directors" """)
			self.curr.execute(""" DROP TABLE IF EXISTS "movies" """)
			self.curr.execute(""" DROP TABLE IF EXISTS "genres" """)
		elif mode == "users-start":	
			self.curr.execute(""" DROP TABLE IF EXISTS "ratings" """)
			self.curr.execute(""" DROP TABLE IF EXISTS "users" """)
		

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movies" ( "movie_tag"	NUMERIC NOT NULL UNIQUE,\
			"year"	INTEGER,\
			"title" VARCHAR(100) NOT NULL,\
			"runtime" INTEGER,\
			"page" INTEGER NOT NULL,\
			"movie_id" SERIAL,\
			PRIMARY KEY("movie_tag"))""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "users" ("username"	VARCHAR(50) NOT NULL UNIQUE,\
			"name"	VARCHAR(50),\
			"age" INTEGER,\
			"city" VARCHAR(30),\
			"seen_count" INTEGER CHECK( seen_count > 0),\
			"page" INTEGER NOT NULL,\
			"id" SERIAL PRIMARY KEY)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "ratings" ("user_id"	INTEGER NOT NULL,\
			"movie_tag"	NUMERIC NOT NULL,\
			"rating"	REAL NOT NULL,\
			PRIMARY KEY("user_id","movie_tag"),\
			CONSTRAINT userid_fkey FOREIGN KEY (user_id) REFERENCES users (id) MATCH SIMPLE \
      		ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "genres" ("id"	SERIAL PRIMARY KEY,\
			"genre"	VARCHAR(30) UNIQUE)""")

		if mode == "movies-start":
			genres = open("filmow/genres.txt").read().splitlines()
			for gen in genres:
				try:
					self.curr.execute(""" INSERT INTO genres(genre) VALUES (%s)""", (gen,))
				except pg.errors.UniqueViolation as e:
					pass
			#end for
		#end if

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movie_to_genres" ("movie_tag"	NUMERIC NOT NULL,\
			"genre_id"	INTEGER NOT NULL,\
			PRIMARY KEY("movie_tag","genre_id"),\
			CONSTRAINT movietag_fk FOREIGN KEY (movie_tag) REFERENCES movies (movie_tag) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION,\
			CONSTRAINT genreid_fk FOREIGN KEY (genre_id) REFERENCES genres (id) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movie_to_countries" ("movie_tag"	NUMERIC NOT NULL,\
			"country"	VARCHAR(50) NOT NULL,\
			PRIMARY KEY("movie_tag","country"),\
			CONSTRAINT movietag_fk FOREIGN KEY (movie_tag) REFERENCES movies (movie_tag) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.curr.execute(""" CREATE TABLE IF NOT EXISTS "movie_to_directors" ("movie_tag"	NUMERIC NOT NULL,\
			"director"	VARCHAR(50) NOT NULL,\
			PRIMARY KEY("movie_tag","director"),\
			CONSTRAINT movietag_fk FOREIGN KEY (movie_tag) REFERENCES movies (movie_tag) MATCH SIMPLE\
			ON UPDATE NO ACTION ON DELETE NO ACTION)""")

		self.conn.commit()

	def process_item(self, item, spider):
		if isinstance(item,UserItem):
			return self.handle_user(item)
		if isinstance(item, MovieItem):
			return self.handle_movie(item)
		return item
    #end process_item

	def handle_user(self, item):
		if item['ratings']:
			user_id = self.store_user(item)
			print("User {username}: {rats} ratings ".format(username=item['username'], rats=len(item['ratings'] )))
			ratings = [(user_id,t[1],r) for t,r in item['ratings'].items()]
			self.store_ratings(ratings)
		else:
			raise DropItem("User {n} : 0 ratings".format(n=item['username']))
		#end if
		return item
	#end handle_user

	def handle_movie(self, item):
		movie_tag = self.store_movie(item)
		print("Movie {tag}: {title}".format(tag=movie_tag,title=item['title']))
		# since there are few genres, directors and countries we chose to insert them inside a for-loop instead of bulk-insertion with curr.copy_from
		for director in item['directors']:
			self.curr.execute(""" INSERT INTO movie_to_directors(movie_tag,director) VALUES (%s,%s) """,\
			(movie_tag,director))
		#end for
		for country in item['countries']:
			self.curr.execute(""" INSERT INTO movie_to_countries(movie_tag,country) VALUES (%s,%s) """,\
			(movie_tag,country))
		#end for
		for genre in item['genres']:
			self.curr.execute(""" SELECT id FROM genres WHERE genre LIKE %s;""", (genre,))
			genre_id = self.curr.fetchone()[0]
			self.curr.execute(""" INSERT INTO movie_to_genres(movie_tag,genre_id) VALUES (%s,%s) """,\
			(movie_tag,genre_id))
		#end for
		self.conn.commit()
		return item
	#end handle_user

	def store_movie(self, movie):
		try:
			self.curr.execute(""" INSERT INTO movies(movie_tag, year, title, runtime,page) VALUES (%s,%s,%s,%s,%s)""",\
			 (movie['movie_tag'],movie['year'],movie['title'],movie['runtime'],movie['page']))
		except pg.IntegrityError as e:
			print("Movie already in the DB")
		finally:
			self.conn.commit()
		return movie['movie_tag']
	#end store_movie

	def store_user(self, user):
		id = -1
		try:
			self.curr.execute(""" INSERT INTO users(username, name, age, city, seen_count,page) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",\
			 (user['username'],user['name'],user['age'],user['city'],user['seen_count'],user['page']))
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
			f = self.create_string_iterator(ratings)
			self.curr.copy_from(f, 'ratings', columns=('user_id', 'movie_tag', 'rating'), sep="|")
			#self.curr.execute(""" INSERT INTO ratings(username, movie_tag, rating) VALUES (?,?,?)""", (username,movie_tag,float(rat)))
		except pg.IntegrityError as e:
			print("Rating already in the DB")
		finally:
			self.conn.commit()

	# l must be a list of tuples (3-uples)
	def create_string_iterator(self, l):
		return StringIteratorIO( ('|'.join(map(clean_csv_value, (i[0],i[1],i[2]))) + '\n' for i in l ) )