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
	ratings = scrapy.Field()
	
class RatingItem(scrapy.Item):
	username = scrapy.Field()
	movie_tag = scrapy.Field()
	rating = scrapy.Field()

class MovieItem(scrapy.Item):
	tag = scrapy.Field()
	title = scrapy.Field()
	year = scrapy.Field()
	genres = scrapy.Field()