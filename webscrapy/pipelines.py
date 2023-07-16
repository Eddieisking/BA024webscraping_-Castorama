# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import openpyxl
import pymysql
import re
import googletrans
from datetime import datetime
from googletrans import Translator
from pymysql import Error

"""
    review_id = scrapy.Field()
    product_name = scrapy.Field()
    customer_name = scrapy.Field()
    customer_rating = scrapy.Field()
    customer_date = scrapy.Field()
    customer_review = scrapy.Field()
    customer_support = scrapy.Field()
"""

# Pipeline for Excel
class ExcelPipeline:

    def __init__(self):
        self.wb = openpyxl.Workbook()
        self.ws = self.wb.active
        self.ws.title = 'customer reviews'
        self.ws.append(('review_id','product_name','customer_name', 'customer_rating', 'customer_date', 'customer_review', 'customer_support'))

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        self.wb.save('castorama.xlsx')

    def process_item(self, item, spider):
        review_id = item.get('review_id', '')
        product_name = item.get('product_name', '')
        customer_name = item.get('customer_name', '')
        customer_rating = item.get('customer_rating', '')
        customer_date = item.get('customer_date', '')
        customer_review = item.get('customer_review', '')
        customer_support = item.get('customer_support', '')
        self.ws.append((review_id, product_name, customer_name, customer_rating, customer_date, customer_review, customer_support))
        return item

"""
    review_id = scrapy.Field()
    product_name = scrapy.Field()
    customer_name = scrapy.Field()
    customer_rating = scrapy.Field()
    customer_date = scrapy.Field()
    customer_review = scrapy.Field()
    customer_support = scrapy.Field()
"""
# Pipeline for sql
def remove_unappealing_characters(text):
    # Remove emojis
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Remove non-printable characters
    text = re.sub(r'[^\x20-\x7E]', '', text)

    return text

"""Translate a text from src to dest language"""

def translator(text: str, src: str):

    # print(googletrans.LANGUAGES)

    translator = Translator()
    result = translator.translate(text, src=src, dest='en')

    return result.text

def date(date:str):
    date_object = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z')
    date = date_object.date()
    return date

# class DatabasePipeline:
#
#     def __init__(self):
#         self.conn = pymysql.connect(user="fqmm26", password="boston27", host="myeusql.dur.ac.uk", database="Pfqmm26_amazon")
#         self.cursor = self.conn.cursor()
#         self.data = []
#
#     def close_spider(self, spider):
#         if len(self.data) > 0:
#             self.sql_write()
#         # self.cursor.close()
#         self.conn.close()
#
#     def process_item(self, item, spider):
#         product_name = item.get('product_name', '')
#         customer_name = item.get('customer_name', '')
#         customer_rating = item.get('customer_rating', '')
#         # Remove unloaded chars
#         customer_date_original = item.get('customer_date', '')
#         customer_date = remove_unappealing_characters(customer_date_original)
#         # Remove unloaded chars and cut
#         customer_review_original = item.get('customer_review', '')
#         customer_review = remove_unappealing_characters(' '.join(customer_review_original))
#         customer_support = item.get('customer_support', '')
#         self.data.append((product_name, customer_name, customer_rating, customer_date, customer_review, customer_support))
#
#         if len(self.data) == 10:
#             self.sql_write()
#             self.data.clear()
#
#         return item
#
#     def sql_write(self):
#         self.cursor.executemany(
#             "insert into customer_review (product_name, customer_name, customer_rating, customer_date, customer_review, customer_support) values(%s, %s, %s, %s, %s, %s)",
#             self.data
#         )
#         self.conn.commit()

class DatabasePipeline:

    def __init__(self):
        self.conn = pymysql.connect(user="fqmm26", password="boston27", host="myeusql.dur.ac.uk", database="Pfqmm26_BA024")
        self.cursor = self.conn.cursor()
        self.data = []

    def close_spider(self, spider):
        if len(self.data) > 0:
            self.sql_write()
        # self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        try:
            self.cursor.execute("SELECT 1")  # Execute a simple query to check if the connection is alive
        except Error as e:
            print(f"Error: {e}")
            self.reconnect()
        review_id = item.get('review_id', '')
        product_name = item.get('product_name', '')
        customer_name = item.get('customer_name', '')
        customer_rating = item.get('customer_rating', '')
        customer_date = date(item.get('customer_date', ''))
        customer_review = remove_unappealing_characters(item.get('customer_review', '')[0:1999])
        customer_support = item.get('customer_support', '')
        customer_disagree = item.get('customer_disagree', '')

        product_name_en = translator(product_name, src='fr')
        customer_review_en = translator(customer_review, src='fr')

        self.data.append((review_id, product_name, customer_name, customer_rating, customer_date, customer_review, customer_support, customer_disagree, product_name_en, customer_review_en))

        if len(self.data) == 10:
            self.sql_write()
            self.data.clear()

        return item

    def sql_write(self):
        self.cursor.executemany(
            "insert into castorama_fr (review_id, product_name, customer_name, customer_rating, customer_date, customer_review, customer_support, customer_disagree, product_name_en, customer_review_en) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            self.data
        )
        self.conn.commit()

    def reconnect(self):
        try:
            self.conn.ping(reconnect=True)  # Ping the server to reconnect
            print("Reconnected to the database.")
        except Error as e:
            print(f"Error reconnecting to the database: {e}")


