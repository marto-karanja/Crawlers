import datetime
import csv
import os
import sys
import scrapy
import datetime

from scrapy import Request
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from studydaddy.items import LinkItem



class StudyDaddyLink(scrapy.Spider):
    name="links"

    custom_settings = {
        'DOWNLOAD_DELAY' : '85',
        'AUTOTHROTTLE_ENABLED':'True',
        'AUTOTHROTTLE_START_DELAY':'20.0',
        'AUTOTHROTTLE_MAX_DELAY':'360.0',
        'AUTOTHROTTLE_TARGET_CONCURRENCY':'0.25',
        'AUTOTHROTTLE_DEBUG': 'True',
        'HTTPCACHE_ENABLED': 'True',
        'ITEM_PIPELINES':{
            'studydaddy.pipelines.link_db.LinkSaver': 700,
            },
        
    }

    #=----------------------------------------------------------------
    def start_requests(self):
        """read from file"""
        urls = []
        with open("links.csv") as file:
            reader = csv.DictReader(file)
            for line in reader:
                urls.append(line['url'])
        self.category = urls[0].split("/")[3]
        self.category = self.category.split("-")[0]

        #yield urls
        for link in urls:
            yield Request(url=link, callback=self.parse)

    #---------------------------------------------------------------------

    def parse(self, response):
        """extract page links"""
        links = response.css(".accounting-item__link").css("::attr(href)").extract()
        #use item loader to save in database
        loader = ItemLoader(item=LinkItem(), response=response)
        loader.add_value('links', links)
        loader.add_value('referring_link', response.url)
        loader.add_value('project', self.settings.get('BOT_NAME'))
        loader.add_value('spider', self.name)
        loader.add_value('date', datetime.datetime.now())
        loader.add_value('category', self.category)

        yield loader.load_item()
        



