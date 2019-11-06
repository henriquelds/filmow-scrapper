import scrapy
from urllib.parse import urljoin
from ..items import UserItem


class FilmowSpider(scrapy.Spider):
    name = "filmow"
    root = "https://filmow.com/"


    start_urls = [
        'https://filmow.com/usuarios/',
    ]

    def parse_aval_page(self, response):
        user = response.meta['user_item']
        
        for aval in response.css('li.span2.movie_list_item'):
            rating = aval.css('div.user-rating span::attr(title)').extract_first().split()[1]
            movie_tag = aval.css('span.wrapper a::attr(data-movie-pk)').extract_first()
            user['ratings'].append((movie_tag,rating))
        #end for
        
        next_page = response.css('#next-page::attr(href)').extract_first()
        if next_page is not None:
            yield response.follow(next_page, meta={'user_item' : user}, callback=self.parse_aval_page)
        else:
            yield user
    #end parse aval

    def parse_user(self, response):
        title = str(response.css('title::text').extract())
        username = title[title.find("(")+1:title.find(")")]
        name = response.xpath("//meta[@property='og:title']/@content").extract_first()
        user = UserItem()
        user['name'] = name
        user['username'] = username
        user['ratings'] = []

        aval_url = urljoin("https://filmow.com/usuario/", username+"/filmes/avaliacoes")
        yield scrapy.Request(aval_url, meta={'user_item' : user}, callback=self.parse_aval_page)
    #end parse_user

    def parse(self, response):
        for user in response.css('li.span1.people-list-item.users-list-item.tip-user a.name::attr(href)').extract():
            url = urljoin(self.root, user)
            yield scrapy.Request(url, callback=self.parse_user)
        next_page = response.css('#next-page::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            print("next page: ", next_page)
            #yield scrapy.Request(next_page, callback=self.parse)
    #end parse