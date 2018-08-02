# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from w3lib.http import basic_auth_header
from scrapy.utils.project import get_project_settings
import requests
import pdb
from scrapy_heapproxies import BadProxy, HeapProxy


class CrawelraApiSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class HeapProxyRandom(HeapProxy):
    total_errors = {400: 0, 404: 0,
                    429: 0, 504: 0}
    total_fail = 0

    def is_response_banned(self, response, request):
        # pdb.set_trace()
        if super(HeapProxyRandom,
                 self).is_response_banned(response,
                                          request):
            return True

        if response.status in [404, 400, 504, 429]:
            self.total_errors[response.status] += 1

            if "id_req" in request.meta.keys():
                self.logger.error("Erreur {0} with request id {1} (a total of {2} errors {0})".format(
                    response.status, request.meta["id_req"], self.total_errors[response.status]))
            return True
        ret = response.xpath("//img[contains(@title, 'obverse')]")
        if ret:
            self.total_fail += 1
            self.logger.warning(
                "THERE IS AN ERROR, CHANGING PROXY, NB FAIL = {0} AND ID = {1}".format(
                    str(self.total_fail), request.meta["id_req"]))
            return True
        return False
