import numpy as np
import pandas as pd

import urllib.request
#for python3


import os
import datetime
import time
import sqlalchemy

import sys
#print(sys.getdefaultencoding())

import tushare as ts


def update_database(code,name,new_data,table,op='addnewonly'):
    #op='newcoverold', replace by new df
    #op='addnewonly', old df remain, add new line
    #new_data必须带字段'seq'，在to_sql前本函数负责把它变成index
    print("update database, op=",op)
    
    try:
        #尝试获取旧数据，存入old_data
        sql = "select * from %s where code = %s ; " % (table,code)
        print(sql)
        old_data = pd.read_sql_query(sql, conn_mysql)
    except:
        #database无此股票数据，直接插入后返回
        new_data = new_data.set_index(new_data['seq'])
        del new_data['seq']
        new_data.to_sql(table, con=conn_mysql, if_exists='append')
        print("new talbe! save data of %s %s to table %s db! %d lines" % (code,name,table,len(new_data)))
        return 0, len(new_data)

    #print(old_data.index)
    old_seq = set(old_data['seq'])
    print(new_data.index)
    new_seq = set(new_data['seq'])
    print('old_seq',old_seq)
    print('new_seq',new_seq)
    print('new-old',new_seq - old_seq)
    print('old-new',old_seq - new_seq)
    
    if op == 'newcoverold':
        old_data = old_data[old_data['seq'].isin(old_seq - new_seq)]
        kdata = pd.concat([old_data, new_data],sort=False)
        #print(kdata)
        
        #seq作为index保留在df，写入数据库成为seq列，读入又重新成为df的seq列
        kdata = kdata.set_index(kdata['seq'])
        del kdata['seq']
        
        del_old = sqlalchemy.text('delete from stockdata where code = :x')
        del_old = del_old.bindparams(x=code)
        conn_mysql.execute(del_old)
        print('delete old data of %s %s'% (code, name))
        
        kdata.to_sql(table, con=conn_mysql, if_exists='append')
        print("replace with new data of %s %s to table %s in db! %d lines" % (code,name,table,len(kdata)))
        
    elif op == 'addnewonly':
        new_data = new_data[new_data['seq'].isin(new_seq - old_seq)]
        
        #seq作为index保留在df，写入数据库成为seq列，读入又重新成为df的seq列
        new_data = new_data.set_index(new_data['seq'])
        del new_data['seq']
        
        #print(new_data)
        new_data.to_sql(table, con=conn_mysql, if_exists='append')
        print("save only new data of %s %s to table %s db! %d lines" % (code,name,table,len(new_data)))
        
    return 0, max(old_seq | new_seq)
    
    


def update_kmonth():

    table = 'kmonth'

    sql = "select code, max(seq_season) from kmonth group by code;"
    stock_already_exist =  pd.read_sql_query(sql, conn_mysql)
    print(stock_already_exist)
    
    sql = "select * from stocklist order by code"
    stocklist = pd.read_sql_query(sql, conn_mysql)
    
    for index, row in stocklist.iterrows():
        code = row['code']
        name = row['name']
     
        #从tushare获取某个股票的月K线数据
        print("\n\n\n Now I am reading kmonth of %s %s" % (code,name))
        
        kdata = ts.get_k_data(code, ktype='M', index=False, start='', end='', autype=None)
        if len(kdata) > 0:
            kdata['date'] = kdata['date'].str.replace('/','-')
            tmp = kdata['date'].str.split('-', expand=True)
            #tmp = tmp.iloc[1:,:] 原用于去第一行，现删去
            #print(tmp)
            kdata['年'] = tmp[0].astype('int32')
            kdata['月'] = tmp[1].astype('int32')
            kdata['季'] = (tmp[1].astype('int32')-1) // 3 + 1  #1-3月为第1季，而不是第0季。
            kdata['code'] = code
            kdata['name'] = name
            kdata['seq'] = kdata['年'] * 12 + kdata['月']
            kdata['seq_season'] = kdata['年'] * 4 + kdata['季']
            #print(kdata)
            #插入到kdata表中
            update_database(code,name,kdata,table,op='addnewonly')
    
def update_kmonth_old():
    dest_table = 'kmonth_new'
    
    sql = "select * from stocklist order by code"
    stocklist = pd.read_sql_query(sql, conn_mysql)
    
    for index, row in stocklist.iterrows():
        code = row['code']
        name = row['name']
        stock_seq = row['seq']
        lastread163 = row['lastread163']
        print("\n\n\n Now I am updating kmonth of %s %s" % (code,name))

    
        sql = "select * from kmonth where code = %s ;" % code
        kmonth = pd.read_sql_query(sql, conn_mysql)
    
        kmonth['seq_season'] = kmonth['年'] * 4 + kmonth['季']
        kmonth.to_sql(dest_table, con=conn_mysql, if_exists='append')
    #alter table kmonth rename to kmonth_old;
    #alter table kmonth_new rename to kmonth;

 
engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@stock.riverriver.xyz:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()
#stockdata = sqlalchemy.Table('stockdata', metadata, autoload=True, autoload_with=engine)
#mylist = sqlalchemy.Table('stocklist', metadata, autoload=True, autoload_with=engine)



today = datetime.date.today()
year = today.year
month = today.month
seq_today = year * 12 + month
print("seq_today=",seq_today)

update_kmonth()
#query = "CREATE TABLE kmonth_ext as select k.*, s.`ROE(年化)`, s.`ROA(年化)`, s.每股净资产, s.毛利率 , s.`经营现金流净额比利润(年化)` , s.`母公司利润比例(年化)` , s.资产负债率 , s.基本每股收益年化 , s.营业收入YOY , s.营业成本YOY , s.营业利润YOY , s.净利润YOY , k.close / s.基本每股收益年化 PE,  k.close/(s.基本每股收益年化*(1+s.净利润YOY)) PEG, k.close/s.每股净资产 PB,  k.close*s.`实收资本(或股本)(万元)` 总市值 FROM kmonth k LEFT OUTER JOIN stockdata s ON (k.code=s.code and k.seq_season=s.seq);"
#conn_mysql.execute(query)  

