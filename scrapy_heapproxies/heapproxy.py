
import re
import random
import base64
import logging
import time
import datetime
import heapq
import pdb

log = logging.getLogger('scrapy.heapproxies')


class Mode:
    PROXY_TIMEOUT = range(1)


class HeapProxy(object):
    def __init__(self, settings):
        self.mode = settings.get('PROXY_MODE')
        self.proxy_list = settings.get('PROXY_LIST')
        self.timeout = float(settings.get('PROXY_TIMEOUT'))
        self.chosen_proxy = ''

        if self.proxy_list is None:
            raise KeyError('PROXY_LIST setting is missing')
        self.proxies = {}
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
            
        now = datetime.datetime.now()
        self.proxies = [(now - datetime.timedelta(seconds = self.timeout), i, self.proxies[i]) for i in self.proxies]
        heapq.heapify(self.proxies)
        first = heapq.heappop(self.proxies)
        self.chosen_proxy = first
        heapq.heappush(self.proxies, (datetime.datetime.now(), first[1], first[2]))
        
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        # Don't overwrite with a random one (server-side state for IP)
        if 'proxy' in request.meta:
            if request.meta["exception"] is False:
                return
        request.meta["exception"] = False
        if len(self.proxies) == 0:
            raise ValueError('All proxies are unusable, cannot proceed')

        first = heapq.heappop(self.proxies)
        now = datetime.datetime.now()
        diff = (now - first[0]).total_seconds()

        if diff < self.timeout:
            log.info("Timeout reached, waiting {} seconds".format(self.timeout - diff))
            time.sleep(self.timeout - diff)
            
        self.chosen_proxy = first
        heapq.heappush(self.proxies, (datetime.datetime.now(), first[1], first[2]))

        proxy_address = self.chosen_proxy[1]

        proxy_user_pass = self.chosen_proxy[2]

        if proxy_user_pass:
            request.meta['proxy'] = proxy_address
            basic_auth = 'Basic ' + base64.b64encode(proxy_user_pass.encode()).decode()
            request.headers['Proxy-Authorization'] = basic_auth
        else:
            log.debug('Proxy user pass not found')
        log.debug('Using proxy <%s>, %d proxies left' % (
                proxy_address, len(self.proxies)))

    def process_exception(self, request, exception, spider):
        if 'proxy' not in request.meta:
            return
        proxy = request.meta['proxy']
        try:
            for i in self.proxies:
                if i[1] == self.chosen_proxy[i]:
                    log.info("Proxy {} not responding, taking off".format(i[1]))
                    self.proxies.remove(i)
                    break
                
            heapq.heapify(self.proxies)
        except KeyError:
            pass
        request.meta["exception"] = True
        log.info('Removing failed proxy <%s>, %d proxies left' % (
            proxy, len(self.proxies)))
