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
from .exceptions import BadProxy
import requests
from w3lib.http import basic_auth_header
import time

class Mode:
    PROXY_TIMEOUT = range(1)

class RequestCrawlera():
    proxies = []

    def __init__(self, api_key, url = "http://httpbin.org/ip"):
        self.url_init = url
        self.api_key = api_key
        self.crawlera_url = "http://proxy.crawlera.com:8010"
        self.proxies = []
        self.deleted_proxy = []
        
    def create_session(self, nb):
        headers = {
            "Proxy-Authorization" : basic_auth_header(self.api_key, ''),
            'X-Crawlera-Session' : 'create'
        }
        proxies = {"http": self.crawlera_url}

        for i in range(0, nb):
            res = requests.get(self.url_init, headers=headers, proxies = proxies)
            self.proxies.append(res.headers.get('X-Crawlera-Session', ''))

    def add_proxy(self, proxy_id, request):
        request.meta['proxy'] = self.crawlera_url
        request.headers['Proxy-Authorization'] = basic_auth_header(self.api_key, '')
        request.headers["X-Crawlera-Session"] = proxy_id
        return request

    def delete_and_create(self, proxy_id):
        self.delete_session(proxy_id)
        headers = {
            "Proxy-Authorization" : basic_auth_header(self.api_key, ''),
            'X-Crawlera-Session' : 'create'
        }
        proxies = {"http": self.crawlera_url}
        res = requests.get(self.url_init, headers=headers, proxies = proxies)
        proxy_id = res.headers.get('X-Crawlera-Session', '')
        self.proxies.append(proxy_id)
        return (proxy_id)

    def delete_session(self, id_proxy):
        if id_proxy != b'':
            self.deleted_proxy.append(id_proxy)
            headers = {"Authorization": basic_auth_header(self.api_key, '')}
            headers["X-Crawlera-Session"] = id_proxy
            try:
                requests.delete("http://proxy.crawlera.com:8010/sessions/" + str(id_proxy, "utf-8"),
                                headers=headers)
            except TypeError:
                requests.delete("http://proxy.crawlera.com:8010/sessions/" + id_proxy,
                                headers=headers)
        for i in self.proxies:
            try:
                if i == str(id_proxy, "utf-8"):
                    self.proxies.remove(i)
                    break
            except TypeError:
                if i == id_proxy:
                    self.proxies.remove(i)
                    break

    def delete_all_sessions(self):
        for i in self.proxies:
            self.delete_session(i)

class HeapProxy(object):
    requests = WeakKeyDictionary()

    def __init__(self, settings):
        super(HeapProxy, self).__init__()
        self.timeout = float(settings.get('PROXY_TIMEOUT'))
        self.restart_limit = int(settings.get("PROXY_RESTART_LIMIT"))
        self.api_key = str(settings.get("CRAWLERA_APIKEY"))

        self.logger = logging.getLogger('scrapy.heapproxies')
        self.crawlera_req = RequestCrawlera(self.api_key)
        self.working_proxies = {}
        self.proxies = {}

        self.proxy_list = []
        self.build_from_list()
        self.id_req = 0
        self.schduled_list = []
    def build_from_list(self):
        self.crawlera_req.create_session(10)        
        for i in self.crawlera_req.proxies:
            self.working_proxies[i] = datetime.datetime.now()
        self.logger.debug("Proxies being used {}".format(self.proxies))
        now = datetime.datetime.now()
        self.logger.debug("Building heap")
        self.proxies = [(now - datetime.timedelta(seconds=self.timeout + 10),
                         i,
                         self.working_proxies[i]) for i in self.working_proxies.keys()]
        heapq.heapify(self.proxies)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    @classmethod
    def spider_idle(cls, spider):
        if cls.requests.get(spider) is not None:
            spider.log("delayed requests pending, not closing spider")
            raise DontCloseSpider()

    def add_proxy(self, proxy, request):
        request = self.crawlera_req.add_proxy(proxy[1], request)
        return request

    def push_to_heap(self, proxy):
        if proxy[1] not in self.working_proxies.keys():
            return None
        now = datetime.datetime.now()
        self.working_proxies[proxy[1]] = now
        try:
            heapq.heappush(self.proxies,
                           (now,
                            proxy[1],
                            proxy[2]))
        except:
            pdb.set_trace()

    def process_request(self, request, spider):
        if "400" in request.meta:
            if "proxy" in request.meta:
                try:
                    request.meta.pop("id_req")
                    request.meta.pop("proxy")
                    request.headers.pop("X-Crawlera-Session")
                except:
                    pdb.set_trace()
        if 'id_req' not in request.meta:
            request.meta['id_req'] = self.id_req
            self.id_req += 1
        self.logger.info("PROCESS REQUEST : " + str(request.meta["id_req"]))
        if 'proxy' not in request.meta:
            self.logger.info("The request has no proxy")
            # Brand New Request
            if len(self.proxies) == 0:
                self.logger.info("The proxy list is empty, the request is schedule")
                # No proxy in queue
                last_time = min([i for i in self.working_proxies.values()])
    
                dt_now = datetime.datetime.now()
                dt_tmp = self.timeout - (dt_now - last_time).total_seconds()
                dt = max([dt_tmp, 3])
                if request.meta["id_req"] in self.schduled_list:
                    pdb.set_trace()
                # Somebody is gonna be pushed into queue, must liberate thread
                #request.dont_filter = True

                reactor.callLater(dt,
                                  self.schedule_request,
                                  request.copy(),
                                  spider)
                raise IgnoreRequest()

            self.logger.info('Get a proxy')
            proxy = heapq.heappop(self.proxies)
            now = datetime.datetime.now()
            diff = (now - proxy[0]).total_seconds()

            if diff < self.timeout:
                # Time out reached for all proxies, calling later

                self.logger.info(
                    "Timeout reached, waiting {} seconds".
                    format(self.timeout - diff))
                self.requests.setdefault(spider, 0)
                self.requests[spider] += 1
                request = self.add_proxy(proxy, request)
                request.dont_filter = True
                reactor.callLater(self.timeout - diff,
                                  self.schedule_request,
                                  request.copy(),
                                  spider,
                                  proxy)
                raise IgnoreRequest()

            request = self.add_proxy(proxy, request)
            self.push_to_heap(proxy)

            self.logger.info('Using proxy <%s>, %d proxies left' % (
                proxy[1], len(self.proxies)))

        elif 'bad_proxy' in request.meta:
            # User sent mesage (spider)
            self.logger.warning('Bad proxy detected')
            raise BadProxy

        elif str(request.headers["X-Crawlera-Session"], "utf-8") not in self.working_proxies.keys():
            # Request was scheduled with proxy but proxy was since signaled bad_proxy
            self.logger.warning("Request was scheduled with proxy but proxy was since signaled bad_proxy")
            raise BadProxy

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
                request.meta.pop('delayed_request')
                proxy = request.meta.pop('proxy_object')
                self.push_to_heap(proxy)

            self.logger.debug(
                'Request has already the needed proxy, nothing to do')

    def schedule_request(self, request, spider, proxy=None):
        if request.meta["id_req"] in self.schduled_list:
            self.logger.info("RESCUDULE THE REQUEST NUM " + str(request.meta["id_req"]))
        if len(spider.crawler.engine.slot.inprogress) > 300:
            pdb.set_trace()
        spider.logger.debug('Currently there are {0} scheduled requests and {1} inprogress requests'.
                            format(len(spider.crawler.engine.slot.scheduler),
                                   len(spider.crawler.engine.slot.inprogress)))
        spider.logger.debug('Trying to ad request {0} to spider {1}'.
                            format(request, spider))
        if proxy is not None:
            request.meta['delayed_request'] = True
            request.meta['proxy_object'] = proxy
        self.schduled_list.append(request.meta["id_req"])
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
            new_proxy_id = self.crawlera_req.delete_and_create(proxy_id)
            request.meta.pop('delayed_request', None)
            request.meta.pop('proxy_object', None)
            request.meta.pop('bad_proxy', None)
            self.working_proxies.pop(proxy_id, None)
            self.working_proxies[new_proxy_id] = datetime.datetime.now()
            try:
                for i in self.proxies:
                    if i[1] == proxy:
                        self.logger.debug(
                            "Proxy {} not responding, taking off".format(i[1]))
                        self.proxies.remove(i)
                        now = datetime.datetime.now()
                        self.proxies.append((now - datetime.timedelta(seconds=self.timeout + 10),
                            new_proxy_id,
                            self.working_proxies[new_proxy_id]))
                        break

                heapq.heapify(self.proxies)
            except KeyError:
                pass

            self.logger.info('Removing failed proxy <%s>, %d proxies left' % (
                proxy, len(self.working_proxies)))
            if 'redirect_urls' in request.meta:
                request.replace(url=request.meta['redirect_urls'][0])
            return request


    def spider_closed(self):
        self.crawlera_req.delete_all_sessions()
