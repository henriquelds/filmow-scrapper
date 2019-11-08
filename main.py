import scrapy
from twisted.internet import reactor, defer
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
from filmow.spiders.filmow_spider import UserSpider,MovieSpider

settings = get_project_settings()
runner = CrawlerRunner(settings)

@defer.inlineCallbacks
def crawl():
    #yield runner.crawl(UserSpider)
    yield runner.crawl(MovieSpider)
    reactor.stop()

crawl()
reactor.run()