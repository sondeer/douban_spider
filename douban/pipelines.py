# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json

import pymongo as pymongo
import pymysql
from scrapy.exporters import JsonItemExporter, CsvItemExporter
from twisted.enterprise import adbapi



from douban.items import DoubanItem


class DoubanPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonWithEncodingPipeline(object):
    def __init__(self):
        self.file = codecs.open('douban.json','w',encoding='utf-8')

    def process_item(self,item,spider):
        line = json.dumps(dict(item),ensure_ascii=False) + "\n"
        print(line)
        self.file.write(line)
        return item

    def spider_closed(self,spider):
        self.file.close()



# class JsonSaverPipeline(object):
#
#     def __init__(self):
#         self.file = open("result_douban.json", "wb")
#         self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
#         self.exporter.start_exporting()
#
#     def process_item(self, item, spider):
#         if isinstance(item, ShopItem):
#             self.exporter.export_item(item)
#
#         return item
#
#     def close_spider(self, spider):
#         self.exporter.finish_exporting()
#         self.file.close()


def dbHandle():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        passwd='123456',
        db = 'testdb',
        charset='utf8',
        use_unicode=False

    )
    print('have process connect dbHandle')
    return conn


class MysqlSavePipeline(object):

    def __init__(self):
        print('save data to mysql')
        #pass

    def process_item(self,item,spider):
        dbObject = dbHandle()
        cursor = dbObject.cursor()
        print('have insert into the data to mysql')
        sql= 'insert into top250(title_name,rank,rate,link,quote) values (%s,%s,%s,%s,%s)'
        try:
            cursor.execute(sql,(item['title'][0],
                                item['rank'][0],
                                item['rate'][0],
                                item['link'][0],
                                item['quote'][0]
                                )
                            )
            print(item['title'][0])
            dbObject.commit()
        except Exception as e:
            print(e)            #抛出异常
            dbObject.rollback()

        print(item['title'][0])
    # def process_item(self,item,spider):
    #     host = settings['MYSQL_HOSTS']
    #     user = settings['MYSQL_USER']
    #     passwd = settings['MYSQL_PASSWORD']
    #     db = settings['MYSQL_DB']
    #     c = settings['CHARSET']
    #     port = settings['MYSQL_PORT']
    #
    #     con = pymysql.connect(ho)


'''
#由于Python3不支持mysqldb库，故下述功能不适用Python3.5
class MysqlSavePipeline(object):

    #数据库参数
    def __init__(self):
        dbargs = dict(
            host = '127.0.0.1',
            db = 'test',
            user = 'root',
            passwd = '123456',
            cursorclass = MySQLdb.cursors.DictCursor,
            charset = 'utf8',
            use_unicode = True
        )
        self.dbpool = adbapi.ConnectionPool('MySQLdb',**dbargs)

    def process_item(self,item,spider):
        res = self.dbpool.runInteraction(self.insert_into_table, item)
        return item
        # 插入的表，此表需要事先建好

    def insert_into_table(self, conn, item):
        conn.execute('insert into douban(rank, title, rate,qute,link) values(%s,%s,%s,%s,%s)', (
            item['rank'][0],
            item['title'][0],
            # item['star'][0],
            item['rate'][0],
            item['quote'][0],
            item['link'][0])
                     )

    def close_spider(self,spider):
        #由于数据库操作不是文件操作，不需要关闭处理
        pass
'''

class CsvSaverPipeline(object):

    def __init__(self):
        self.file = open("result_douban.csv", "wb")
        self.exporter = CsvItemExporter(self.file, encoding="utf-8")
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        if isinstance(item, DoubanItem):
            self.exporter.export_item(item)

        return item

    def close_spider(self, spider):
        pass

class DbSaverPipeline(object):

    def __init__(self):
        client = pymongo.MongoClient(host="localhost")
        self.db = client["douban_top250"]
        self.title = self.db["title"]
        self.rank = self.db["rank"]

    def process_item(self, item, spider):

        if isinstance(item, DoubanItem):
            data = dict(item)
            # try:
            #     result = self.spider_movie250.replace_one({"id": data.get("id")}, data, upsert=True)
            # except Exception as ex:
            #     print(ex)


        return item

    def close_spider(self, spider):
        pass
