import re
import base64
import logging
import datetime
import time
import heapq
import pdb
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from twisted.internet import reactor
from twisted.internet.error import TCPTimedOutError, TimeoutError, ConnectionRefusedError
from weakref import WeakKeyDictionary
from .exceptions import BadProxy


class Mode:
    PROXY_TIMEOUT = range(1)


class HeapProxy(object):
    requests = WeakKeyDictionary()

    def __init__(self, settings):
        super(HeapProxy, self).__init__()
        self.mode = settings.get('PROXY_MODE')
        self.proxy_list = settings.get('PROXY_LIST')
        if self.proxy_list is None:
            raise KeyError('PROXY_LIST setting is missing')
        self.timeout = float(settings.get('PROXY_TIMEOUT'))
        self.restart_limit = int(settings.get("PROXY_RESTART_LIMIT"))

        self.logger = logging.getLogger('scrapy.heapproxies')
        self.working_proxies = {}
        self.proxies = {}

        self.build_from_list()

    def build_from_list(self):
        self.logger.debug('Reading file {}'.format(self.proxy_list))
        fin = open(self.proxy_list)
        try:
            for line in fin.readlines():
                parts = re.match('(\w+://)([^:]+?:[^@]+?@)?(.+)', line.strip())
                if not parts:
                    continue

                if parts.group(2):
                    user_pass = parts.group(2)[:-1]
                else:
                    user_pass = ''

                self.working_proxies[parts.group(
                    1) + parts.group(3)] = (user_pass, datetime.datetime.now())
        finally:
            fin.close()

        self.logger.debug("Proxies being used {}".format(self.proxies))
        now = datetime.datetime.now()
        self.logger.debug("Building heap")
        self.proxies = [(now - datetime.timedelta(seconds=self.timeout + 10),
                         i,
                         self.working_proxies[i][0]) for i in self.working_proxies.keys()]
        heapq.heapify(self.proxies)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        return ext

    @classmethod
    def spider_idle(cls, spider):
        if cls.requests.get(spider):
            spider.log("delayed requests pending, not closing spider")
            raise DontCloseSpider()

    def add_proxy(self, proxy, request):
        self.logger.debug('Pushing back to heap')
        proxy_address = proxy[1]
        proxy_user_pass = proxy[2]

        if proxy_user_pass:
            request.meta['proxy'] = proxy_address
            basic_auth = 'Basic ' + \
                base64.b64encode(proxy_user_pass.encode()).decode()
            request.headers['Proxy-Authorization'] = basic_auth
        else:
            self.logger.debug(
                'Proxy user pass not found, adding proxy without password')
            request.meta['proxy'] = proxy_address
        return request

    def push_to_heap(self, proxy):
        if proxy[1] not in self.working_proxies.keys():
            return None
        now = datetime.datetime.now()
        self.working_proxies[proxy[1]] = (proxy[2], now)
        try:
            heapq.heappush(self.proxies,
                           (now,
                            proxy[1],
                            proxy[2]))
        except:
            pdb.set_trace()

    def process_request(self, request, spider):
        if 'proxy' not in request.meta:
            # Brand New Request
            if len(self.proxies) == 0:
                # No proxy in queue
                last_time = min([i[1] for i in self.working_proxies.values()])

                dt = self.timeout - (datetime.datetime.now() -
                                     last_time).total_seconds()
                dt = max([dt, 0.001])
                # Somebody is gonna be pushed into queue, must liberate thread
                request.dont_filter = True
                reactor.callLater(dt,
                                  self.schedule_request,
                                  request.copy(),
                                  spider)
                raise IgnoreRequest()

            self.logger.debug('Picking proxies')
            proxy = heapq.heappop(self.proxies)
            now = datetime.datetime.now()
            diff = (now - proxy[0]).total_seconds()

            if diff < self.timeout:
                # Time out reached for all proxies, calling later
                self.logger.debug(
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

            self.logger.debug('Using proxy <%s>, %d proxies left' % (
                proxy[1], len(self.proxies)))

        elif 'bad_proxy' in request.meta:
            # User sent mesage (spider)
            self.logger.debug('Bad proxy detected')
            raise BadProxy

        elif request.meta.get('proxy') not in self.working_proxies.keys():
            # Request was scheduled with proxy but proxy was since signaled bad_proxy
            raise BadProxy

        else:
            if 'delayed_request' in request.meta:
                # Delayed request, must just repush to queue
                self.logger.debug('Dealing with delayed request')
                request.meta.pop('delayed_request')
                proxy = request.meta.pop('proxy_object')
                self.push_to_heap(proxy)

            self.logger.debug(
                'Request has already the needed proxy, nothing to do')
            return

    def schedule_request(self, request, spider, proxy=None):
        spider.logger.debug('Currently there are {0} scheduled requests and {1} inprogress requests'.
                            format(len(spider.crawler.engine.slot.scheduler),
                                   len(spider.crawler.engine.slot.inprogress)))
        spider.logger.debug('Trying to ad request {0} to spider {1}'.
                            format(request, spider))
        if proxy is not None:
            request.meta['delayed_request'] = True
            request.meta['proxy_object'] = proxy
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
                                                   TimeoutError,
                                                   ConnectionRefusedError]]):
            proxy = request.meta.pop('proxy')
            request.meta.pop('delayed_request', None)
            request.meta.pop('proxy_object', None)
            request.meta.pop('bad_proxy', None)
            self.working_proxies.pop(proxy, None)
            try:
                for i in self.proxies:
                    if i[1] == proxy:
                        self.logger.debug(
                            "Proxy {} not responding, taking off".format(i[1]))
                        self.proxies.remove(i)
                        break

                heapq.heapify(self.proxies)
            except KeyError:
                pass

            self.logger.info('Removing failed proxy <%s>, %d proxies left' % (
                proxy, len(self.working_proxies)))
            if len(self.working_proxies) < self.restart_limit:
                self.logger.info("Restarting proxylist")
                self.build_from_list()
            if 'redirect_urls' in request.meta:
                request.replace(url=request.meta['redirect_urls'][0])
            return request
