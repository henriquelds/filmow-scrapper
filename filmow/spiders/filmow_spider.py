import scrapy
from urllib.parse import urljoin
from ..items import UserItem, MovieItem


class MovieSpider(scrapy.Spider):
    name = "filmow-movie"
    root = "https://filmow.com/"

    start_urls = [
        'https://filmow.com/filmes-todos/?pagina=1',
    ]

    def parse_movie(self, response):
        movie = MovieItem()
        tag = int(response.css('div.star-rating::attr(data-movie-pk)').extract_first())
        title = response.css('div.movie-title a::text').extract_first()

        table = response.css('table.table.table-striped')

        year = int(table.css('td::text').extract()[1].strip())

        directors = table.css('span[itemprop="name"]::text').extract()
        runtime = int(table.css('span.running_time::text').extract_first().replace("minutos","").strip())

        genres = table.css('div.btn-tags-list a[itemprop="genre"]::text').extract()

        info = table.css('td')[-1]
        countries = info.css('div a.btn-tag::text').extract()
        
        if genres in countries: #movies may not have countries indicated, then the last td is the genres
            countries = []
        #end if
        movie['movie_tag'] = tag
        movie['title'] = title
        movie['year'] = year
        movie['directors'] = directors
        movie['runtime'] = runtime
        movie['genres'] = genres
        movie['countries'] = countries
        movie['page'] = response.meta['page']

        yield movie

    def parse(self, response):
        title = response.css('title::text').extract_first() 
        pg_text = find_between(title, "Página", "|")
        cur_page = int(pg_text) if len(pg_text) > 0 else 1
        for movie in response.css('li.span2.movie_list_item a.cover.tip-movie::attr(href)').extract():
            desc = movie+"ficha-tecnica/"
            url = urljoin(self.root, desc)
            yield scrapy.Request(url, callback=self.parse_movie, meta={"page":cur_page})
        next_page = None#response.css('#next-page::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            #print("movies next page: ", next_page)
            yield scrapy.Request(next_page, callback=self.parse)
    #end parse

#end MovieSpider

class UserSpider(scrapy.Spider):
    name = "filmow-user"
    root = "https://filmow.com/"


    start_urls = [
        'https://filmow.com/usuarios/?pagina=1',
    ]

    def parse_aval_page(self, response):
        user = response.meta['user_item']

        for aval in response.css('li.span2.movie_list_item'):
            rating = aval.css('div.user-rating span::attr(title)').extract_first().split()[1]
            movie_tag = int(aval.css('span.wrapper a::attr(data-movie-pk)').extract_first()[1:])
            user['ratings'].append((user['username'],movie_tag,rating))
        #end for
        
        next_page = response.css('#next-page::attr(href)').extract_first()
        if next_page is not None:
            yield response.follow(next_page, meta={'user_item' : user}, callback=self.parse_aval_page)
        else:
            yield user
    #end parse aval

    def parse_user(self, response):
        title = str(response.css('title::text').extract())
        username = title[title.find("(")+1:title.find(")")].strip()
        name = response.xpath("//meta[@property='og:title']/@content").extract_first().strip()
        info = response.css('div.span9 div::text').extract_first()
        city, age = None, None
        seen_count = int(response.css('span.number::text').extract_first())
        if 'years' in info:
            index = info.find('years')
            age = int(info[:index-1].strip())
            start = info.rfind('years')+len('years')+1
            end = max(info.rfind('-'),info.rfind(','))
            if end != -1:
                city = info[start:end].strip()
        elif max(info.rfind(','),info.rfind('-')) > 0:
            end = max(info.rfind(','),info.rfind('-'))
            city = info[:end].strip()

        city = None if city is not None and len(city) < 1 else city
        age = None if age is not None and len(age) < 1 else age

        user = UserItem()
        user['name'] = name
        user['username'] = username
        user['age'] = age
        user['city'] = city
        user['ratings'] = []
        user['seen_count'] = seen_count
        user['page'] = response.meta['page']

        if seen_count > 0:
            aval_url = urljoin("https://filmow.com/usuario/", username+"/filmes/avaliacoes")
            yield scrapy.Request(aval_url, meta={'user_item' : user}, callback=self.parse_aval_page)
        else:
            yield user
    #end parse_user

    def parse(self, response):
        title = response.css('title::text').extract_first() 
        pg_text = find_between(title, "Página", "|")
        cur_page = int(pg_text) if len(pg_text) > 0 else 1
        for user in response.css('li.span1.people-list-item.users-list-item.tip-user a.name::attr(href)').extract():
            url = urljoin(self.root, user)
            yield scrapy.Request(url, callback=self.parse_user, meta={"page":cur_page})
        next_page = response.css('#next-page::attr(href)').extract_first()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            #print("users next page: ", next_page)
            yield scrapy.Request(next_page, callback=self.parse)
    #end parse

#end UserSpider

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""