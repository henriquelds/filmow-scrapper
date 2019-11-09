# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class FilmowItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class UserItem(scrapy.Item):
	name = scrapy.Field()
	username = scrapy.Field()
	age = scrapy.Field()
	city = scrapy.Field()
	ratings = scrapy.Field()
	seen_count = scrapy.Field()
	page = scrapy.Field()
	def __repr__(self):
		return repr({"username" : self['username']})
	
class MovieItem(scrapy.Item):
	movie_tag = scrapy.Field()
	title = scrapy.Field()
	year = scrapy.Field()
	runtime = scrapy.Field()
	genres = scrapy.Field()
	directors = scrapy.Field()
	countries = scrapy.Field()
	page = scrapy.Field()