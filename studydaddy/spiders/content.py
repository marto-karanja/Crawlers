import scrapy
import mysql.connector
import csv
import socket
import datetime
from scrapy.http import Request
from itemloaders.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from studydaddy.items import ContentItem

class StudyDaddyContent(scrapy.Spider):
    """Study Daddy connection harvestor"""
    name = "content"
    custom_settings = {
        'DOWNLOAD_DELAY' : '85',
        'AUTOTHROTTLE_ENABLED':'True',
        'AUTOTHROTTLE_START_DELAY':'20.0',
        'AUTOTHROTTLE_MAX_DELAY':'360.0',
        'AUTOTHROTTLE_TARGET_CONCURRENCY':'0.25',
        'AUTOTHROTTLE_DEBUG': 'True',
        'CLOSESPIDER_ITEMCOUNT':'10000',
        'HTTPCACHE_ENABLED': 'True',
        'ITEM_PIPELINES':{
            'studydaddy.pipelines.content_db.ContentWriter': 700,
            },
    }
    category = ['economics-homework-help']

    def start_requests(self):
        links = set()
        # open database connection and fetch links
        conn = mysql.connector.connect(user='kush', passwd='incorrect', db='crawls', host='localhost', charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        cursor.execute('SELECT link_no, link FROM studydaddy_links where category = %s and processed="False" order by link_no;',(self.category))
        rows = cursor.fetchall()
        conn.close()
        self.logger.info('%s urls fetched', len(rows))

        #iterate through the links
        for link_no,link in rows:
            link = 'https://studydaddy.com' + link
            yield scrapy.Request(link, meta={"link_no": link_no}, headers = {'referer': 'https://www.google.com/' }, callback = self.parse_content)

    def parse_content(self, response):
        
        loader = ItemLoader(item=ContentItem(), response = response)

        # extract content

        # extract content
        content =response.css(".question-description").css("p::text").extract()
        loader.add_value('content', content, MapCompose(str.strip), Join() )

        # extract title
        title = response.css(".active").css("h1::text").extract()
        loader.add_value('title', title)
        #add category
        loader.add_value('category', self.category)
        # add link no
        loader.add_value('link_no', response.meta['link_no'])
        #add housekeeping
        loader.add_value('project', self.settings.get('BOT_NAME'))
        loader.add_value('spider', self.name)
        loader.add_value('server', socket.gethostname())
        loader.add_value('date', datetime.datetime.now())

        yield loader.load_item()

        


        