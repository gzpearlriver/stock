import numpy as np
import pandas as pd

import urllib.request
#for python3

import os
import datetime
import time
import sqlalchemy


def del_nullcode():
    sql = sqlalchemy.text("select count(*) as alldata from stockdata ")
    result = conn_mysql.execute(sql).fetchone()
    print('before delete null:',result['alldata'])
    
    del_null = sqlalchemy.text('delete from stockdata where code is null')
    #del_old = del_old.bindparams(x=code)
    conn_mysql.execute(del_null)
    
    sql = sqlalchemy.text("select count(*) as alldata from stockdata ")
    result = conn_mysql.execute(sql).fetchone()
    print('after delete null:',result['alldata'])



def alt_table():
    print("1) Drop table stockdata_old and Alter table stockdata rename to stockdata_old.")
    print("2) Drop table stockdata only.")
    choice = input("enter your choice")
    if choice =='1' :
        try:
            conn_mysql.execute('drop table stockdata_old;')
        except:
            print('dropping fail. no such table stockdata_old')
        
        try:
            conn_mysql.execute('alter table stockdata rename to stockdata_old;')
            print('succed to rename stockdata to stockdata_old .')
        except:
            print('fail to rename stockdata to stockdata_old .')

    elif choice =='2' :
        try:
            conn_mysql.execute('drop table stockdata;')
            print('succed to drop table stockdata .')
        except:
            print('dropping fail. no such table stockdata.')

def debtpay():
    sql = "select code, name, avg(保守速动比率) as 保守速动比率 , avg(速动比率) as 速动比率, avg(流动比率) as 流动比率 from stockdata where seq=%d or seq=%d or seq=%d group by code,name " % (seq_today, seq_today-1, seq_today-2)
    print(sql)
    
    stock = pd.read_sql_query(sql, conn_mysql)
    #print(stocklist)
    print(stock.columns)
    print(stock.dtypes)
    print(stock.head())
    stock = stock.sort_values(by=['保守速动比率'], ascending=True)
    print(stock)
    print(type(stock))
    #stock.to_csv('stock.csv',encoding='utf_8_sig')
    bad_shortdebtpay = stock[stock['保守速动比率'] > 4]
    good_shortdebtpay = stock[stock['保守速动比率'] < 0.5 ]
    
    bad_shortdebtpay.to_sql('bad_shortdebtpay', con=conn_mysql, if_exists='replace')
    good_shortdebtpay.to_sql('good_shortdebtpay', con=conn_mysql, if_exists='replace')

def roeroa():
    sql = "select code, name, `资产总计(万元)` as 资产总计, `所有者权益(或股东权益)合计(万元)` as 所有者权益,  `ROA(年化)` as ROA , `ROE(年化)` as ROE , `毛利率(年化)` , 毛利率, `经营现金流净额比利润(年化)` from stockdata where seq=%d and `所有者权益(或股东权益)合计(万元)` >0 " % seq_today
    print(sql)
    stock = pd.read_sql_query(sql, conn_mysql)
    #print(stocklist)
    print(stock.columns)
    print(stock.dtypes)
    print(stock.head())
    stock = stock.sort_values(by=['ROA'], ascending=True)
    print(stock)
    print(type(stock))
    #stock.to_csv('stock.csv',encoding='utf_8_sig')
    '''bad_shortdebtpay = stock[stock['保守速动比率'] > 4]
    good_shortdebtpay = stock[stock['保守速动比率'] < 0.5 ]
    
    bad_shortdebtpay.to_sql('bad_shortdebtpay', con=conn_mysql, if_exists='replace')
    good_shortdebtpay.to_sql('good_shortdebtpay', con=conn_mysql, if_exists='replace')'''

def survey():
   stmt = sqlalchemy.text("select count(*) as stockdata_count from (select distinct code from stockdata) temp_a ")
   result = conn_mysql.execute(stmt).fetchone()
   stockdata_count = result['stockdata_count']
   print("stockdata_count",stockdata_count)
   
   stmt = sqlalchemy.text("select count(*) as stocklist_count from stocklist ")
   result = conn_mysql.execute(stmt).fetchone()
   stocklist_count = result['stocklist_count']
   print("stocklist_count",stocklist_count)

engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()
stockdata = sqlalchemy.Table('stockdata', metadata, autoload=True, autoload_with=engine)


today = datetime.date.today()
year = today.year
month = today.month
seq_today = year * 4 + month // 4
print("seq_today=",seq_today)   

#survey()
#roeroa()
alt_table()