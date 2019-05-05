#!/usr/bin/python
# -*- coding: UTF-8 -*-
#allow chinese character in program
import sys
reload(sys)
sys.setdefaultencoding('utf8')


import pandas as pd
import sqlite3 as lite
import tushare as ts
import pymysql
from sqlalchemy import *


def migrate_stocklist():

    sql = "select * from stocklist"
    oldlist = pd.read_sql_query(sql, conn_lite)
    print(len(oldlist))
    print(oldlist.columns)
    
    miglist = oldlist[[u'code',u'name',u'industry',u'area']]
    #miglist = oldlist['code']
    
    miglist['new']=False
    #print(miglist.head())
    
    miglist.to_sql('stocklist', con=conn_mysql, if_exists='replace')

def migrate_finance():

    sql = "select * from stock limit 1"
    oldfin = pd.read_sql_query(sql, conn_lite)
    print(oldfin)
    print(oldfin.columns)
    
    '''stmt = text('delete from finance;')
    print(str(stmt))
    conn_mysql.execute(stmt)
    
    oldfin.to_sql('finance', con=conn_mysql, if_exists='append') '''

    
db_file = '/code/stock/stock.db'

conn_lite=lite.connect(db_file)
#conn_lite.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
#allow chinese character in database for python2
cur_lite = conn_lite.cursor()




engine = create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=0&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = MetaData(engine)
conn_mysql = engine.connect()


migrate_finance()    