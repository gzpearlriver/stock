#!/usr/bin/env python
# -*- coding: utf-8 -*-

from stockbank import *

engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@120.25.237.31:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)

#engine = sqlalchemy.create_engine("mysql+mysqlconnector://stock:stock@stock.riverriver.xyz:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
#使用mysqlconnector会出现
#engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@stock.riverriver.xyz:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()

#stockdata = sqlalchemy.Table('stockdata', metadata, autoload=True, autoload_with=engine)
mylist = sqlalchemy.Table('stocklist', metadata, autoload=True, autoload_with=engine)


stockdata_process(conn_mysql, mylist, op='routine_update')


conn_mysql.close()
