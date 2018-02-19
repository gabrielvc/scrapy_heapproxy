import re
import random
import base64
import logging
import time
import datetime
import heapq
import pdb
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from twisted.internet import reactor
from weakref import WeakKeyDictionary


class Mode:
    PROXY_TIMEOUT = range(1)


class HeapProxy(object):
    requests = WeakKeyDictionary()
    
    def __init__(self, settings):
        super(HeapProxy, self).__init__()
        self.mode = settings.get('PROXY_MODE')
        self.proxy_list = settings.get('PROXY_LIST')
        self.timeout = float(settings.get('PROXY_TIMEOUT'))
        self.chosen_proxy = ''
        self.logger = logging.getLogger('scrapy.heapproxies')
        self.restart_limit = int(settings.get("PROXY_RESTART_LIMIT"))
      
        if self.proxy_list is None:
            raise KeyError('PROXY_LIST setting is missing')
        self.proxies = {}

        self.read_from_list()


    def read_from_list(self):
        self.proxies = {}
        self.logger.debug('Reading file {}'.format(self.proxy_list))
        fin = open(self.proxy_list)
        try:
            for line in fin.readlines():
                parts = re.match('(\w+://)([^:]+?:[^@]+?@)?(.+)', line.strip())
                if not parts:
                    continue

                    # Cut trailing @
                if parts.group(2):
                    user_pass = parts.group(2)[:-1]
                else:
                    user_pass = ''

                self.proxies[parts.group(1) + parts.group(3)] = user_pass
        finally:
            fin.close()
        self.logger.debug("Proxies being used {}".format(self.proxies))
        now = datetime.datetime.now()
        self.logger.debug("Building heap")
        self.proxies = [(now - datetime.timedelta(seconds = self.timeout + 10), i, self.proxies[i]) for i in self.proxies]
        heapq.heapify(self.proxies)

        self.logger.debug("Picking first proxy")
        first = heapq.heappop(self.proxies)
        self.chosen_proxy = first

        self.logger.debug("Repushing proxy to end of queue")
        heapq.heappush(self.proxies, (datetime.datetime.now(), first[1], first[2]))
        
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


    def add_proxy(self, first, request):
        self.chosen_proxy = first

        self.logger.debug('Pushing back to heap')
        heapq.heappush(self.proxies, (datetime.datetime.now(), first[1], first[2]))

        proxy_address = self.chosen_proxy[1]

        proxy_user_pass = self.chosen_proxy[2]

        if proxy_user_pass:
            request.meta['proxy'] = proxy_address
            basic_auth = 'Basic ' + base64.b64encode(proxy_user_pass.encode()).decode()
            request.headers['Proxy-Authorization'] = basic_auth
        else:
            self.logger.debug('Proxy user pass not found, adding proxy without password')
            request.meta['proxy'] = proxy_address
        return request

    def process_request(self, request, spider):
        # Don't overwrite with a random one (server-side state for IP)
        if 'proxy' in request.meta:
            if request.meta["exception"] is False:
                return
        request.meta["exception"] = False
        if len(self.proxies) == 0:
            raise ValueError('All proxies are unusable, cannot proceed')

        self.logger.debug('Picking proxies')
        first = heapq.heappop(self.proxies)
        now = datetime.datetime.now()
        diff = (now - first[0]).total_seconds()

        if diff < self.timeout:
            self.logger.info("Timeout reached, waiting {} seconds".format(self.timeout - diff))
            self.requests.setdefault(spider, 0)
            self.requests[spider] += 1
            request = self.add_proxy(first, request)
            reactor.callLater(self.timeout - diff, self.schedule_request, request.copy(),
                              spider)
            raise IgnoreRequest()
            
        request = self.add_proxy(first, request)
        
        self.logger.debug('Using proxy <%s>, %d proxies left' % (
                self.chosen_proxy[1], len(self.proxies)))


    def schedule_request(self, request, spider):
        spider.crawler.engine.schedule(request, spider)
        self.requests[spider] -= 1

        
    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest) or isinstance(exception, DontCloseSpider):
            return None
        
        if 'proxy' not in request.meta:
            return
        proxy = request.meta['proxy']
        try:
            for i in self.proxies:
                if i[1] == self.chosen_proxy[1]:
                    self.logger.info("Proxy {} not responding, taking off".format(i[1]))
                    self.proxies.remove(i)
                    break
                
            heapq.heapify(self.proxies)
        except KeyError:
            pass
        request.meta["exception"] = True
        self.logger.info('Removing failed proxy <%s>, %d proxies left' % (
            proxy, len(self.proxies)))
        if len(self.proxies) < self.restart_limit:
            self.logger.info("Restarting proxylist")
            self.read_from_list()
