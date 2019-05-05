import numpy as np
import pandas as pd

import urllib.request
#for python3

import os
from datetime import date,timedelta
import time
import sqlalchemy

import sys
#print(sys.getdefaultencoding())



url_template = (
"http://quotes.money.163.com/service/zcfzb_%s.html",
"http://quotes.money.163.com/service/lrb_%s.html",
"http://quotes.money.163.com/service/xjllb_%s.html",
"http://quotes.money.163.com/service/cwbbzy_%s.html")

file_template= (
"/stockdata/163/zcfzb_%s.csv" ,
"/stockdata/163/lrb_%s.csv",
"/stockdata/163/xjllb_%s.csv",
"/stockdata/163/cwbbzy_%s.csv")






def read163data(stock_string):
    errcode = 1
    for i in range(0, 4, 1):
        #stock_string = str(stock).zfill(6)
        url = url_template[i] % (stock_string)
        filename = file_template[i] % (stock_string)
        print(url, filename)

        #if os.path.exists(filename) and os.path.isfile(filename):
        #    continue

        for j in range(0, 10):
            try:
            #if True:
                response = urllib.request.urlopen(url)
                #for python3 
                #response = urllib.urlopen(url)
                #response = urllib.request(url)
                
                if response.getcode() == 200:
                    print('http sucess!')
                    break

            except:
                print(j,' time http error encounted!')
                time.sleep(10)

        else:
            break
        # got file!
        print(response.getcode())
        body = response.read()

        file = open(filename, "wb")
        file.write(body)
        file.close()
        time.sleep(1)
        errcode = 0
    return errcode

engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=0&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()


sql = "select * from stocklist limit 1"
stocklist = pd.read_sql_query(sql, conn_mysql)
#print(stocklist)
#print(stocklist.columns)
#print(stocklist.dtypes)

#k=0
for index, row in stocklist.iterrows():
    code = row['code'].decode()
    name = row['name'].decode()
    print("read163now",code,name)
    errcode = read163data(code)
    #print(type(code),type(name))
    '''if errcode == 0:
        k = k + 1
        if k%20 == 0:
            print("pausing.")
            time.sleep(20)'''

