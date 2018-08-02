# -*- coding: utf-8 -*-
import scrapy
import pdb


class CraweleraApiSpider(scrapy.Spider):
    name = 'crawlera_api'
    allowed_domains = ['fr.wikipedia.org']
    start_urls = 'https://fr.wikipedia.org'
    total_req = 0

    def start_requests(self):
        fd = open("countries.txt")
        urls = ["https://www.random.org/coins/?num=2&cur=60-eur.ireland-1euro"] * 30
        self.logger.error(str(len(urls)))
        fd.close()
        for i in range(len(urls)):
            request = scrapy.Request(urls[i], dont_filter=True)
            request.meta['id_req'] = i
            yield request

    def parse(self, response):
        if response.request.url == "https://www.random.org/robots.txt":
            return
      
        if response.status == 504:
            self.logger.error("ERREUR 504")
            pdb.set_trace()

        else:
            self.total_req += 1
            self.logger.info(
                "THE CRAWLER HAS WORKED, NB REQ = {0} AND ID = {1}".format(
                    str(self.total_req), response.request.meta["id_req"]))
