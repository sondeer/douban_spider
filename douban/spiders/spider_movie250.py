# -*- coding: utf-8 -*-
import scrapy

from douban.items import DoubanItem


class SpiderMovie250Spider(scrapy.Spider):
    name = 'spider_movie250'
    allowed_domains = ['movie.douban.com']
    start_urls = ['http://movie.douban.com/top250']

    def parse(self, response):
        # fp = open('1.html', 'wb+')
        # fp.write(response.body)
        # fp.close()
        # print(response.body)
        
        #定义一个列表
        item = []
        for info in response.xpath('//div[@class="item"]'):
            #print(info)

            item = DoubanItem()         #实例化item
            #item['rank'] = info.xpath('//div[@class="pic"]/em/text()').extract()  '// 为多级定位符，会逐步寻找
            item['rank'] = info.xpath('div[@class="pic"]/em/text()').extract()
            item['title'] = info.xpath('div[@class="pic"]/a/img/@alt').extract()
            item['link'] = info.xpath('div[@class="pic"]/a/@href').extract()
            item['rate'] = info.xpath('div[@class="info"]/div[@class="bd"]/div[@class="star"]/span/text()').extract()
            item['quote'] = info.xpath('div[@class="info"]/div[@class="bd"]/p[@class="quote"]/span/text()').extract()
            #print('get item info')
            print(item)
            yield item

        #翻页
        next_page = response.xpath('//span[@class="next"]/a/@href')
        #print(next_page)
        if next_page:
            url = response.urljoin(next_page[0].extract())
            #print(url)
            
            #request对象生成后会传递给下载器，下载器处理这个request后返回response对象，然后返回给本spider
            #request对象中callback  = self.parse
            yield scrapy.Request(url,self.parse)
