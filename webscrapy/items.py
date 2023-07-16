# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WebscrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    review_id = scrapy.Field()
    product_name = scrapy.Field()
    customer_name = scrapy.Field()
    customer_rating = scrapy.Field()
    customer_date = scrapy.Field()
    customer_review = scrapy.Field()
    customer_support = scrapy.Field()
    customer_disagree = scrapy.Field()
    pass
