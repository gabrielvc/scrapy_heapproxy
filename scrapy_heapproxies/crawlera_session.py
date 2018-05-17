import datetime
import requests
from w3lib.http import basic_auth_header
import heapq
import logging
from .exceptions import EmptyHeap
import pdb


class CrawleraHeap:

    def __init__(self,
                 size,
                 api_key,
                 timeout,
                 url="http://httpbin.org/ip",
                 crawlera_url="http://proxy.crawlera.com:8010",
                 logger=logging.getLogger('Crawlera Heap'),
                 **kwargs):
        self.logger = logger
        self.url = url
        self.crawlera_url = crawlera_url
        self.api_key = api_key
        self.proxies = [CrawleraSession(api_key=self.api_key,
                                        url=self.url,
                                        crawlera_url=self.crawlera_url,
                                        logger=self.logger,
                                        **kwargs) for i in range(size)]
        self.ban_proxies = set()
        heapq.heapify(self.proxies)
        self.last_activity = None

    def get(self):
        if not len(self):
            raise EmptyHeap

        current_session = heapq.heappop(self.proxies)
        if self.is_ban(current_session):
            return self.get()

        return current_session

    def push(self, crawlera_session):
        self.last_activity = datetime.datetime.now()
        crawlera_session.update()
        if crawlera_session in self.ban_proxies:
            return None
        heapq.heappush(self.proxies, crawlera_session)

    def is_ban(self, crawlera_session):
        return crawlera_session in self.ban_proxies

    def delete_session(self, crawlera_session):
        crawlera_session.delete()
        self.ban_proxies.add(crawlera_session)
        push_ok = None
        while push_ok is not None:
            push_ok = self.push(CrawleraSession(api_key=self.api_key,
                                                url=self.url,
                                                crawlera_url=self.crawlera_url))

    def destroy(self):
        self.logger.info(
            "Destroying all the {} on the available proxies".format(len(self)))
        for i in self.proxies:
            i.delete()

    def __len__(self):
        return len(self.proxies)


class CrawleraSession:

    def __init__(self,
                 api_key,
                 url="http://httpbin.org/ip",
                 crawlera_url="http://proxy.crawlera.com:8010",
                 logger=logging.getLogger('CrawleraSession')):

        self.crawlera_url = crawlera_url
        self.api_key = api_key
        self.logger = logger
        self.ask_god(url, crawlera_url)
        self.last_activity = datetime.datetime.now()
        self.status = "available"

    def ask_god(self, url, crawlera_url):
        id = ''
        while not id:
            self.logger.debug("Asking crawlera for a session")
            headers = {
                "Proxy-Authorization": basic_auth_header(self.api_key, ''),
                'X-Crawlera-Session': 'create'
            }
            proxies = {"http": crawlera_url}
            res = requests.get(url, headers=headers,
                              proxies=proxies)
            id = res.headers["X-Crawlera-Session"]
            self.logger.debug("God gave us {}".format(id))
        self.id = id

    def apply(self, request):
        if all([i in request.meta.keys() for i in ['proxy',
                                                   'Proxy-Authorization',
                                                   "X-Crawlera-Session"]]):
            self.logger.debug("Request already has the proxy info")
            return request

        self.logger.debug("Adding proxy info")
        request.meta['proxy'] = self.crawlera_url
        request.headers['Proxy-Authorization'] = basic_auth_header(
            self.api_key, '')
        request.headers["X-Crawlera-Session"] = self.id
        return request

    def update(self):
        self.last_activity = datetime.datetime.now()
        self.status = "available"

    def delete(self):
        self.logger.debug("Deleting proxy {}".format(self.api_key))
        headers = {"Authorization": basic_auth_header(self.api_key, '')}
        headers["X-Crawlera-Session"] = self.id
        requests.delete("http://proxy.crawlera.com:8010/sessions/{}".format(self.id),
                        headers=headers)
        self.last_activity = datetime.datetime.fromtimestamp(0)

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.id == other.id
        return False

    def __gt__(self, other):
        return self.last_activity > other.last_activity

    def __repr__(self):
        return "<CrawleraSession(id={0}, status={1}, last_activity={2})>".format(self.id,
                                                                                 self.status,
                                                                                 self.last_activity.strftime('%H:%M'))
