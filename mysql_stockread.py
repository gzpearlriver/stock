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

input_dir = "/stockdata/163/"
output_dir = "/stockdata/163bigtable/"

input_cwbbzy_template1 = input_dir + "cwbbzy_%s.csv"
input_lrb_template1 = input_dir + "lrb_%s.csv"
input_zcfzb_template1 = input_dir + "zcfzb_%s.csv"
input_xjllb_template1 = input_dir + "xjllb_%s.csv"



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


def read_df(filename):
    #读入163的CSV，转换成DataFrame
    df = pd.read_csv(filename, encoding='GBK',header=0)
    #print(df.iloc[:,0],df.iloc[:,-1])
    head = df.iloc[:, 0].str.strip()
    # 提取列名
    df = df.iloc[:, 1:-1].T
    # 删除第一行和最后一行，然后转置
    df.columns = head
    # 把列名补回

    df = df.sort_index(ascending=False)
    return df


def cal_season_data(stock_data,latest_season, oldest_season, new_column, old_column):
    print("processing ", new_column, old_column)
	
    for s in range(latest_season, oldest_season, -1):
        if stock_data.loc[s, '季'] == 1:
            # season ==1
            stock_data.loc[s, new_column] = stock_data.loc[s, old_column]
        else:
            # season == 2 3 4
            if s not in stock_data['seq']:
                print("this seasonal data does not exits!")
                break
            #print(s, stock_data.loc[s ,'年'], stock_data.loc[s, '季'] , stock_data.loc[s, old_column], stock_data.loc[s - 1, old_column])
            stock_data.loc[s, new_column] = float(stock_data.loc[s, old_column]) - float(stock_data.loc[s - 1, old_column])
    return stock_data


def cal_year_data(stock_data,latest_season, oldest_season, new_column, old_column):
    print("processing ", new_column, old_column)
    for s in range(latest_season, oldest_season + 4, -1):
        stock_data.loc[s, new_column] = float(stock_data.loc[s, old_column])
        for i in range(1, 4, 1):
            # print(stock_data.loc[s, new_column] , stock_data.loc[s - i, old_column])
            stock_data.loc[s, new_column] += float(stock_data.loc[s - i, old_column])
    return stock_data

    
def cal_rate_data(stock_data,latest_season, oldest_season, new_column, dividend, divisor):
    print("processing ", new_column, dividend, divisor)
    for s in range(latest_season, oldest_season + 4, -1):
        try:
            #mydividend = float(stock_data.loc[s, dividend])
            #mydivisor = float(stock_data.loc[s, divisor]))
            stock_data.loc[s, new_column] = float(stock_data.loc[s, dividend]) / float(stock_data.loc[s, divisor]) 
        except:
            print(new_column, stock_data.loc[s, dividend], stock_data.loc[s , divisor], "divided by 0 or not number")
            stock_data.loc[s, new_column] = 0		
    return stock_data


def cal_growth_data(stock_data,latest_season, oldest_season, new_column, old_column):
    print("caculating growth_data ", new_column, old_column)
    for s in range(latest_season, oldest_season + 8, -1):
        try:
            stock_data.loc[s, new_column] = float(stock_data.loc[s, old_column]) / float(stock_data.loc[s -4 , old_column])   - 1
        except:
            print(stock_data.loc[s, old_column] , stock_data.loc[s -4 , old_column], "divided by 0 or not number")
            stock_data.loc[s, new_column] = 0		
    return stock_data

    
def stock_process(stock_string,stock_name):
    print("processing data", stock_string, stock_name)
    #处理从163下载的原始文件，计算三个表的补充指标
    lrb_filename = input_lrb_template1 % (stock_string)
    zcfzb_filename = input_zcfzb_template1 % (stock_string)
    xjllb_filename = input_xjllb_template1 % (stock_string)

    
    lrb = read_df(lrb_filename)
    lrb = lrb.replace("\s*--\s*", 0, regex=True)
    #relpace character --
    lrb = lrb.astype('float')
    lrb = lrb.sort_index(ascending=True)
    #print(lrb)

    
    xjllb = read_df(xjllb_filename)
    xjllb = xjllb.replace("\s*--\s*", 0, regex=True)
    #relpace character --
    xjllb = xjllb.astype('float')
    xjllb = xjllb.sort_index(ascending=True)
    #print(xjllb)
        
    zcfzb = read_df(zcfzb_filename)
    zcfzb = zcfzb.replace("\s*--\s*", 0, regex=True)
    zcfzb = zcfzb.astype('float')
    zcfzb['code'] = stock_string  #.encode('utf-8')
    zcfzb['name'] = stock_name  #.encode('utf-8')
    zcfzb = zcfzb.sort_index(ascending=True) 
    #print(zcfzb)
    
    return zcfzb,lrb,xjllb
    
   

    
def formula_cal(zcfzb,lrb,xjllb):
    #提取三个表的重要数据，合成综合表
    

    xjllb['现金流量表的财务费用(万元)'] = xjllb['财务费用(万元)']
    del xjllb['财务费用(万元)']
    xjllb['现金流量表的净利润(万元)'] = xjllb['净利润(万元)']
    del xjllb['净利润(万元)']
    xjllb['现金流量表的少数股东损益(万元)'] = xjllb['少数股东损益(万元)']
    del xjllb['少数股东损益(万元)']

    print(lrb.count())
    print(xjllb.count())
    print(zcfzb.count())

    stock_data = zcfzb.copy()
    stock_data = stock_data.join(lrb)
    stock_data = stock_data.join(xjllb)
    #以上代码用于三表合一
    print(stock_data)
    
    
    '''stock_data[u'毛利率'] = stock_data[u'营业利润(万元)'] / stock_data[u'营业总收入(万元)']
    stock_data[u'资产负债率'] = stock_data[u'负债合计(万元)'] / stock_data[u'资产总计(万元)']
    stock_data[u'产权比率'] = stock_data[u'负债合计(万元)'] / stock_data[u'所有者权益或股东权益合计(万元)']
    stock_data[u'有形净值债务率'] = stock_data[u'负债合计(万元)'] / (stock_data[u'所有者权益或股东权益合计(万元)'] - stock_data[u'无形资产(万元)'])
    stock_data[u'流动比率'] = stock_data[u'流动资产合计(万元)'] / stock_data[u'流动负债合计(万元)'] 
    stock_data[u'速动比率'] = (stock_data[u'流动资产合计(万元)'] - stock_data[u'存货(万元)'] ) / stock_data[u'流动负债合计(万元)'] 
    stock_data[u'保守速动比率'] = (stock_data[u'货币资金(万元)'] + stock_data[u'交易性金融资产(万元)'] + stock_data[u'应收票据(万元)'] + stock_data[u'应收账款(万元)']) / stock_data[u'流动负债合计(万元)'] 
    stock_data[u'本公司账户类现金与总资产比值'] = (stock_data[u'货币资金(万元)'] + stock_data[u'交易性金融资产(万元)'] + stock_data[u'衍生金融资产(万元)'] + stock_data[u'其他流动资产(万元)']) / stock_data[u'资产总计(万元)'] 
    stock_data[u'非本公司账户类现金与总资产比值'] = (stock_data[u'应收票据(万元)'] + stock_data[u'应收账款(万元)'] + stock_data[u'预付款项(万元)'] + stock_data[u'其他应收款(万元)']) / stock_data[u'资产总计(万元)'] 

    
   

    print(stock_data[u'营业收入(万元)'])
    
    # derive year and month, write index
    stock_data = stock_data[stock_data[u'营业收入(万元)'].notnull() ]
    #处理原始文件中的异常：存在行，但逗号之间都为空值
    #print(stock_data)
    stock_data['date'] = stock_data.index
    stock_data['date'] = stock_data['date'].str.replace('/','-')
    tmp = stock_data['date'].str.split('-', expand=True)
    #tmp = tmp.iloc[1:,:] 原用于去第一行，现删去
    #print(tmp)
    stock_data[u'年'] = tmp[0].astype('int32')
    stock_data[u'月'] = tmp[1].astype('int32')
    stock_data[u'季'] = tmp[1].astype('int32') // 3
	
	#删除多余的
    stock_data = stock_data[ stock_data[u'月'] % 3 ==0 ] 
    
    stock_data['seq'] = stock_data[u'年'].astype('int32') * 4 + stock_data[u'季'].astype('int32')
    stock_data = stock_data.set_index(stock_data['seq'])
    #print(stock_data['seq'])
	
    # get the idnex of oldest seasonal data
    for i in range(stock_data['seq'].max(), stock_data['seq'].min() - 1 , -1):
        #print (i)
        if (i in stock_data.index):
            oldest_season = i
        else :
            break
    latest_season = stock_data['seq'].max()
    print ( "oldest_season:", oldest_season, "  latest_season:", latest_season)

    if oldest_season >= latest_season:
        #无法形成单季数据，结束！
        #stock_data.to_csv(bigdata_filename)
        return empty
	
    # 计算单季数据
    for indicators in season_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ",new_column , 'by ', old_column)
        stock_data = cal_season_data(stock_data,latest_season, oldest_season, new_column, old_column)

    if oldest_season +4 >= latest_season:
        #无法形成年化数据，结束！
        #stock_data.to_csv(bigdata_filename)
        return empty

    # 计算年化数据
    for indicators in year_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ",new_column , 'by ', old_column)
        stock_data = cal_year_data(stock_data, latest_season, oldest_season, new_column, old_column)
    
    # 计算比率数据
    for indicators in rate_data:
        new_column = indicators[0]
        dividend = indicators[1]
        divisor = indicators[2]
        print ("creating ", new_column, 'by ', dividend, divisor)
        stock_data = cal_rate_data(stock_data,latest_season, oldest_season, new_column, dividend, divisor)

    if oldest_season +8 >= latest_season:
        #无法形成增长数据
        #stock_data.to_csv(bigdata_filename)
        return empty
        
    # 计算增长数据
    for indicators in growth_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ", new_column , 'by ', old_column)
        stock_data = cal_growth_data(stock_data,latest_season, oldest_season, new_column, old_column)
    

    
    del stock_data['seq']
    print(list(stock_data.columns))

    stock_data.to_sql('stock', con=conn, if_exists='append')
    print("save to db!")'''
 
 
engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=0&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()


sql = "select * from stocklist limit 1"
stocklist = pd.read_sql_query(sql, conn_mysql)
#print(stocklist)
#print(stocklist.columns)
#print(stocklist.dtypes)


for index, row in stocklist.iterrows():
    code = row['code'].decode()
    name = row['name'].decode()
    '''print("read 163 data now", code, name)
    errcode = read163data(code)
    print("result of reading 163 data :  %d", errcode)
    
    if errcode !=0:
        print("fail to read 163 data")
        break'''

    zcfzb,lrb,xjllb = stock_process(code,name)
    
    formula_cal(zcfzb,lrb,xjllb)
