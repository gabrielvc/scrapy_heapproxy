import re
import base64
import logging
import datetime
import time
import heapq
import pdb
import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from twisted.internet import reactor
from twisted.internet.error import TCPTimedOutError, TimeoutError, ConnectionRefusedError, ConnectionDone
from weakref import WeakKeyDictionary
from .exceptions import BadProxy, EmptyHeap
from .crawlera_session import CrawleraHeap
import requests
from w3lib.http import basic_auth_header
import time


class Mode:
    PROXY_TIMEOUT = range(1)

class HeapProxy(object):
    requests = WeakKeyDictionary()

    def __init__(self, settings):
        super(HeapProxy, self).__init__()
        self.timeout = float(settings.get('PROXY_TIMEOUT'))
        self.api_key = str(settings.get("CRAWLERA_APIKEY"))
        self.nbr_proxies = int(settings.get("N_PROXIES"))
        self.list_debug = []
        self.logger = logging.getLogger(
            'scrapy.heapproxies')
        self.heap = CrawleraHeap(self.nbr_proxies,
                                 self.api_key,
                                 self.timeout,
                                 logger=self.logger)
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(
            ext.spider_closed, signal=signals.spider_closed)
        return ext

    @classmethod
    def spider_idle(cls, spider):
        if cls.requests.get(spider) is not None:
            if cls.requests.get(spider) == 0:
                return
            spider.log("delayed requests pending, not closing spider")
            spider.log("number of requests to do {0}".format(
                cls.requests.get(spider)))
            spider.log('Currently there are {0} scheduled requests and {1} inprogress requests'.
                       format(len(spider.crawler.engine.slot.scheduler),
                              len(spider.crawler.engine.slot.inprogress)))
            raise DontCloseSpider()

    def add_proxy(self, request, session):
        request = session.apply(request)
        request.meta["proxy_object"] = session
        return request

    def push_to_heap(self, session):
        self.heap.push(session)

    def process_request(self, request, spider):
        try:
            self.logger.info(
                "Currently doing request {}".format(request.meta["id_req"]))
        except:
            self.logger.warn("request {} has no id_req".format(request.url))
        self.logger.debug("Currently there are {0} available proxies, and {1} banned proxies".format(
            len(self.heap), len(self.heap.ban_proxies)))
        if 'proxy' not in request.meta:
            self.logger.info("The request has no proxy")
            # Brand New Request
            self.logger.debug('Picking proxies')
            try:
                session = self.heap.get()
            except EmptyHeap:
                # No proxy in queue
                last_time = self.heap.last_activity

                dt = self.timeout - (datetime.datetime.now() -
                                     last_time).total_seconds()
                self.logger.debug("recaling id (no proxy) {0} with dt {1}".format(
                    request.meta["id_req"], dt))
                dt = max([dt, 3])
                # Somebody is gonna be pushed into queue, must liberate thread
                # request.dont_filter = True
                self.logger.debug("REQUEST NUMBER {} ADD TOO REQUEST (1)".format(str(request.meta["id_req"])))
                self.requests.setdefault(spider, 0)
                self.requests[spider] += 1
                reactor.callLater(dt,
                                  self.schedule_request,
                                  request.copy(),
                                  spider)
                raise IgnoreRequest()

            diff = (datetime.datetime.now() -
                    session.last_activity).total_seconds()


            if diff < self.timeout:
                # Time out reached for all proxies, calling later

                self.logger.info(
                    "Timeout reached, waiting {} seconds".
                    format(self.timeout - diff))
                self.logger.debug("REQUEST NUMBER {} ADD TOO REQUEST (2)".format(str(request.meta["id_req"])))
                self.requests.setdefault(spider, 0)
                self.requests[spider] += 1
                request = self.add_proxy(request, session)
                request.dont_filter = True
                self.logger.info("recaling id (no proxy) {0} with dt {1}".format(request.meta["id_req"],
                                                                                 self.timeout - diff))
                reactor.callLater(self.timeout - diff,
                                  self.schedule_request,
                                  request.copy(),
                                  spider,
                                  session)
                raise IgnoreRequest()

            request = self.add_proxy(request, session)

            self.logger.debug('Using proxy <%s>, %d proxies left' % (
                session.id, len(self.heap)))
            return None

        else:
            if 'delayed_request' in request.meta:
                # Delayed request, must just repush to queue
                if request.headers["X-Crawlera-Session"] in self.crawlera_req.deleted_proxy:
                    self.logger.warning("The proxy used has been delete, the request is called later")
                    request.headers.pop("X-Crawlera-Session", None)
                    request.meta.pop("proxy")
                    request.meta.pop('delayed_request')
                    last_time = min([i for i in self.working_proxies.values()])

                    dt = self.timeout - (datetime.datetime.now() -
                                        last_time).total_seconds()
                    dt = max([dt, 0.001])
                    reactor.callLater(dt,
                                  self.schedule_request,
                                  request.copy(),
                                  spider)
                    raise IgnoreRequest()                    
                self.logger.debug('Dealing with delayed request')
                # request.meta.pop('delayed_request')
                return

            self.logger.debug(
                'Request has already the needed proxy, nothing to do')

    def is_response_banned(self, response, request):
        session = request.meta['proxy_object']
        return self.heap.is_ban(session)

    def process_response(self,
                         request,
                         response,
                         spider):
        session = request.meta['proxy_object']
        if not session.id:
            session.set_id(response.headers["X-Crawlera-Session"])
        is_ban = self.is_response_banned(response, request)
        if is_ban:
            request.meta['proxy_object'] = session
            request.headers["X-Crawlera-Session"] = session.id            
            self.logger.info("Request has been banned")
            return self.process_exception(request, BadProxy(), spider)
        self.heap.active_proxies.add(session)
        self.push_to_heap(session)
        return response

    def schedule_request(self, request, spider, session=None):
        spider.logger.debug('Currently there are {0} scheduled requests and {1} inprogress requests'.
                            format(len(spider.crawler.engine.slot.scheduler),
                                   len(spider.crawler.engine.slot.inprogress)))
        spider.logger.debug('Trying to ad request {0} to spider {1}'.
                            format(request, spider))
        if session is not None:
            request.meta['delayed_request'] = True
            request = self.add_proxy(request, session)

        spider.crawler.engine.schedule(request, spider)

        spider.logger.debug('Currently there are {0} scheduled requests and {1} inprogress requests'.
                            format(len(spider.crawler.engine.slot.scheduler),
                                   len(spider.crawler.engine.slot.inprogress)))
        self.requests[spider] -= 1

    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest):
            return None

        if any([isinstance(exception, i) for i in [BadProxy,
                                                   TCPTimedOutError,
                                                   ConnectionDone,
                                                   TimeoutError,
                                                   ConnectionRefusedError]]):
            proxy = request.meta.pop('proxy')
            proxy_id = request.headers["X-Crawlera-Session"]
            request.headers.pop("X-Crawlera-Session")
            session = request.meta.pop('proxy_object', None)
            if session is None:
                pdb.set_trace()
            self.heap.delete_session(session)

            request.meta.pop('delayed_request', None)
            self.logger.info('Removing failed proxy {0}, {1} proxies left and {2} banned'.format(
                session.id, len(self.heap), len(self.heap.ban_proxies)))
            if "redirect_urls" in request.meta:
                request.replace(url=request.meta['redirect_urls'][0])
            return request

    def spider_closed(self):
        self.heap.destroy()
