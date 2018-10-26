import numpy as np
import pandas as pd
import urllib
import pathlib
import time
import sqlite3 as lite

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






def read163data(stock):
    errcode = 1
    for i in range(0, 4, 1):
        stock_string = str(stock).zfill(6)
        url = url_template[i] % (stock_string)
        filename = file_template[i] % (stock_string)
        print(url, filename)

        path = pathlib.Path(filename)
        if path.is_file():
            continue

        for j in range(0, 10):
            try:
                #response = urllib.request.urlopen(url)
                response = urllib.urlopen(url)
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

db_file = '/code/stock/stock.db'

conn=lite.connect(db_file)
conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
#allow chinese character in database
cur = conn.cursor()

sql = "select * from stocklist"
stocklist = pd.read_sql_query(sql, conn)
    
k=0
for stock in stocklist.code:
    errcode = read163data(stock)
    if errcode == 0:
        k = k + 1
        if k%20 == 0:
            print("pausing.")
            time.sleep(20)

