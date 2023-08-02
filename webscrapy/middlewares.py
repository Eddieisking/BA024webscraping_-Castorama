# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals, Request
import random
from webscrapy.settings import USER_AGENT_LIST
from scrapy.exceptions import IgnoreRequest, NotConfigured
import datetime

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


def get_cookies_dict():
    cookies_str = 'session-id=262-5810110-4651945; ubid-acbuk=257-6978771-9482927; lc-acbuk=en_GB; ' \
                  'av-timezone=Europe/London; ' \
                  'session-token=DOVZpTftMkUoO5VyBKks7lmDekubDW0tOJ/Y8RFlRCDcQNMY9anb57YIE61uvnNfAPAU66EyGAc8DqO' \
                  '/ZMK3goltzXQk74bkMIZ3blfypkZD9i4g5AO2dfs6WUwoHB4od8kwqIOj6ES9kJW0weMmjYApvtAIk6gWp5CjVLJ47uP0nRCUiTs8G5JIMsqjwxfHtGnWSqgR2I+lwltG3l0/liTbKeUAkXFyLxSQr9zz0ZafpcrzIKJceuTtwMyRk8sD; x-acbuk="shubALQENXQDItWhyI3ywt5EDiCNBbXUzZxfVHH9lWROOPcIA1q?BbuwaQt63DTE"; at-acbuk=Atza|IwEBIFEhoQBEgsl13zRH7tbqZZsLkFdSErJL6Jpi6Ifp9c_RyZ1tOhe7IfKF6nnJIh4hUThrCCZEVx6oYY0G15ffaTbjgdGYLF135v9-KfnBvEYFIpug_gBh3M1fdnB6lZZD8XTNQz-jsWttVgTtHuEzksP1pNtNsThE4BRKD4DtFSgu34T2Q5eImhkKWwlSTfSYcE1XeAhMnuxcQBcuS-TRVIc5NXki7U0ydlVIKkGvCyZKRg; sess-at-acbuk="+dmR+g8Ip+sCWW69DkUyjUV8hffpBaRX/184ZeqVZPk="; sst-acbuk=Sst1|PQH59LJBGdPDpiPYay9TQjKgCTur4B_PjtkbLg3tmdmH6cMrIfsn-19grn3avly8z5lHqWBXUDrgslHmXWeLxndLGO-_TnF9FBKniR-hLl-W7O7kH0vpUajOjSAEJZUvTND9zvrxQP375bqjQyzEiGM-nVmoeQ4l9pv2EelRBehMRHyMjebmhyz5kVmW5-wZo3ui_7ZIoOIGw_nUBzx-s64hnjZ_Lpl9lN1fSst5eCzgt3nK74MqKlowJkghFOAoHFjU5nsP2vmzyq6GEgxUVOG9MIABXnggOQRzXyw9wJwNWtc; session-id-time=2082787201l; i18n-prefs=GBP; csm-hit=tb:XWX730Q202Q0YG44CV43+s-XWX730Q202Q0YG44CV43|1686670523540&t:1686670523540&adb:adblk_no '
    cookies_dict = {}
    for item in cookies_str.split('; '):
        key, value = item.split('=', maxsplit=1)
        cookies_dict[key] = value
    return cookies_dict


COOKIES = get_cookies_dict()


class WebscrapySpiderMiddleware:
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

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

class WebscrapyDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request: Request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        # request.cookies = COOKIES
        # request.meta = {'proxy': 'socks5://127.0.0.1:10808'}
        ua = random.choice(USER_AGENT_LIST)
        request.headers['User-Agent'] = ua

        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


"""In the following code, just need to change self.current_proxy to use new proxy"""
class RotateProxyMiddleware:
    def __init__(self):
        self.current_proxy = "http://storm-stst123_area-FR:123123@proxy.stormip.cn:1000"
        self.max_retries = 3

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        request.meta['proxy'] = self.current_proxy
        # print('current_proxy')
        # print(self.current_proxy)
        current_time = datetime.datetime.now()
        print(current_time)

    def process_response(self, request, response, spider):
        if response.status == 403:
            retries = request.meta.get('retry_times', 0)
            if retries < 3:
                new_request = request.copy()
                new_request.dont_filter = True  # Disable duplicate request filtering
                new_request.meta['retry_times'] = retries + 1
                return new_request
            else:
                self.save_unable_to_access(request.url)
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest):
            # Handle IgnoreRequest exceptions
            if getattr(exception, 'response', None) is not None:
                return self.process_response(request, exception.response, spider)
            else:
                # IgnoreRequest without a response, re-raise the exception
                raise exception
        elif isinstance(exception, NotConfigured):
            # NotConfigured exception, re-raise it
            raise exception
        else:
            # Handle other exceptions
            retries = request.meta.get('retry_times', 0)
            if retries < self.max_retries:
                new_request = request.copy()
                new_request.dont_filter = True  # Disable duplicate request filtering
                new_request.meta['retry_times'] = retries + 1
                return new_request
            else:
                self.save_unable_to_access(request.url)

    def save_unable_to_access(self, url):
        with open("unable to access.txt", "a") as file:
            file.write(f"Unable to access: {url}\n")
