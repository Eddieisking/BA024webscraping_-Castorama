"""
Project: Web scraping for customer reviews
Author: Hào Cui
Date: 06/16/2023
"""
import json
import re

import scrapy
from scrapy import Request

from webscrapy.items import WebscrapyItem


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.castorama.fr", "api.bazaarvoice.com"]
    headers = {}  #

    def start_requests(self):
        # keywords = ['Stanley', 'Black+Decker', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Facom', 'MAC Tools', 'Vidmar', 'Lista', 'Irwin Tools', 'Lenox', 'Proto', 'CribMaster', 'Powers Fasteners', 'cub-cadet', 'hustler', 'troy-bilt', 'rover', 'BigDog Mower', 'MTD']
        exist_keywords = ['dewalt', 'Stanley', 'Black+Decker', 'Facom']
        # company = 'Stanley Black and Decker'

        # from search words to generate product_urls
        for keyword in exist_keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.castorama.fr/search?term={keyword}'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
                # meta={'proxy':'socks5://127.0.0.1:10110'},
                # headers=self.headers
            )

    def parse(self, response, **kwargs):

        # Extract the pages of product_urls
        page = response.xpath('//p[@data-test-id="search-options-total-results"]/text()')[0].extract()
        page_number = int(''.join(filter(str.isdigit, page)))
        pages = (page_number // 24) + 1

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        product_urls = [f'https://www.castorama.fr/search?page={page}&term={keyword}' for page
                        in range(1, pages+1)]

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse)

    def product_parse(self, response: Request, **kwargs):

        product_list = response.xpath('//*[@id="content"]//main//ul/li')

        for product in product_list:
            product_href = product.xpath('.//div[@data-test-id="product-panel"]/a/@href')[0].extract()
            product_detailed_url = f'https://www.castorama.fr{product_href}'
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse)

    def product_detailed_parse(self, response, **kwargs):

        product_id = response.xpath('.//*[@id="product-details"]//td[@data-test-id="product-ean-spec"]/text()')[
            0].extract()
        product_name = response.xpath('//*[@id="product-title"]/text()')[0].extract()

        # Product reviews url
        product_detailed_href = f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action' \
                                f'=REVIEWS_N_STATS&filter' \
                                f'=productid%3Aeq%3A{product_id}&filter=contentlocale%3Aeq%3Ade*%2Cen*%2Ces*%2Cit' \
                                f'*%2Cpt*%2Cro*%2Cfr_FR' \
                                f'%2Cfr_FR&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale%3Aeq%3Ade' \
                                f'*%2Cen*%2Ces*%2Cit' \
                                f'*%2Cpt*%2Cro*%2Cfr_FR%2Cfr_FR&include=authors%2Cproducts&filteredstats=reviews' \
                                f'&Stats=Reviews&limit=8' \
                                f'&offset=0&sort=submissiontime%3Adesc&passkey' \
                                f'=cad9K7m2kxo5wBH0ObPjr6uk0EFHk2o06sOp4UMIhBNBM&apiversion' \
                                f'=5.5&displaycode=5678-fr_fr '

        if product_name:
            yield Request(url=product_detailed_href, callback=self.review_parse, meta={'product_name': product_name})

    def review_parse(self, response: Request, **kwargs):
        product_name = response.meta['product_name']

        datas = json.loads(response.body)

        if datas:
            limit_number = datas.get('Limit')
            offset_number = datas.get('Offset') + limit_number
            total_number = datas.get('TotalResults')

            for i in range(0, limit_number):
                try:
                    item = WebscrapyItem()
                    item['review_id'] = datas.get('Results')[i].get('Id')
                    item['product_name'] = product_name
                    item['customer_name'] = datas.get('Results')[i].get('UserNickname') if datas.get('Results')[i].get('UserNickname') else 'Anonymous'
                    item['customer_rating'] = datas.get('Results')[i].get('Rating')
                    item['customer_date'] = datas.get('Results')[i].get('SubmissionTime')
                    item['customer_review'] = datas.get('Results')[i].get('ReviewText')
                    item['customer_support'] = datas.get('Results')[i].get('TotalPositiveFeedbackCount')
                    item['customer_disagree'] = datas.get('Results')[i].get('TotalNegativeFeedbackCount')

                    yield item
                except Exception as e:
                    break

            if offset_number < total_number:
                next_page = re.sub(r'offset=\d+', f'offset={offset_number}', response.url)
                yield Request(url=next_page, callback=self.review_parse, meta={'product_name': product_name})
            else:
                pass

        else:
            pass
