
import numpy as np
import pandas as pd

from datetime import date,timedelta
import time
import sqlalchemy

import sys
#print(sys.getdefaultencoding())

import tushare as ts


engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=1&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()
#stockdata = sqlalchemy.Table('stockdata', metadata, autoload=True, autoload_with=engine)


sql = "select * from stocklist"
oldlist = pd.read_sql_query(sql, conn_mysql)
print(oldlist.columns)
print(oldlist.iloc[0:3,:])
del oldlist['index']

tslist = ts.get_stock_basics()
tslist = tslist.reset_index()
newlist = pd.DataFrame()
newlist['name'] = tslist['name']
newlist['code'] = tslist['code']
newlist['industry'] = tslist['industry']
newlist['area'] = tslist['area']
newlist['name'] = newlist['name'].str.replace('*','')
newlist['name'] = newlist['name'].str.replace(' ','')


print(newlist.columns)
print(newlist.iloc[0:3,:])


oldcode = oldlist['code'].values.tolist()
newcode = newlist['code'].values.tolist()
#print(newcode)
#print(oldcode)

delta_n_o = set(newcode) - set(oldcode)
print("new - old")
print(delta_n_o)

delta_o_n = set(oldcode) - set(newcode)
print("old - new")
print(delta_o_n)

same_n_o = set(oldcode) & set(newcode)
print(len(same_n_o))

new_stock = newlist[newlist['code'].isin(delta_n_o)]
new_stock['new'] = True
new_stock['trading'] = True
new_stock['lastread163'] = date(2019,1,1)
new_stock['seq'] = 0

old_stock = oldlist[oldlist['code'].isin(same_n_o)]
old_stock['new'] = False
old_stock['trading'] = True

off_stock = oldlist[oldlist['code'].isin(delta_o_n)]
off_stock['new'] = False
off_stock['trading'] = False

stocklist = pd.concat([new_stock, old_stock, off_stock], ignore_index=True, sort=True)


print(stocklist.columns,len(stocklist))
print(stocklist.iloc[0:3,:])

stocklist.to_sql('stocklist', con=conn_mysql, if_exists='replace')
'''
try:
    cur.execute('drop index stocklist_old;')
except:
    print('drop index stocklist_old fail')
    
try:
    cur.execute('drop table stocklist_old;')
except:
    print('drop table stocklist_old fail')
    
try:
    cur.execute('alter table stocklist rename to stocklist_old;')
except:
    print('alter table stocklist rename fail')
    
try:
    cur.execute('drop index stocklist;')
except:
    print('drop index stocklist fail')
    
try:
    newlist.to_sql('stocklist', con=conn, if_exists='append')
except:
    print('insert stocklist fail')'''
    
conn_mysql.close()