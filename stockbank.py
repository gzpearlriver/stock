#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
pd.set_option('display.max_columns', None)

import urllib.request
#for python3

import os
import datetime
import time
import sqlalchemy

import sys
#print(sys.getdefaultencoding())

import tushare as ts


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

#自定义指标
col_data  = (('工业利润自定义', (['营业总收入(万元)']), (['营业成本(万元)','利息支出(万元)','研发费用(万元)','其他业务成本(万元)','营业税金及附加(万元)','销售费用(万元)','管理费用(万元)','财务费用(万元)','资产减值损失(万元)'])),)


#季度化指标
season_data = (('营业总收入单季', '营业总收入(万元)'),
               ('营业总成本单季', '营业总成本(万元)'),
               ('利润总额单季', '利润总额(万元)'),
               ('净利润单季', '净利润(万元)'),
               ('工业利润自定义单季', '工业利润自定义'),
               ('归属于母公司所有者的净利润单季', '归属于母公司所有者的净利润(万元)'),
               ('少数股东损益单季', '少数股东损益(万元)'),
               ('经营活动产生的现金流量净额单季', '经营活动产生的现金流量净额(万元)'),
               ('投资活动产生的现金流量净额单季', '投资活动产生的现金流量净额(万元)'),
               ('筹资活动产生的现金流量净额单季', '筹资活动产生的现金流量净额(万元)'),
               ('经营活动现金流出小计单季','经营活动现金流出小计(万元)'),
               ('经营活动现金流入小计单季','经营活动现金流入小计(万元)'),
               ('营业税金及附加单季', '营业税金及附加(万元)'),
               ('所得税费用单季', '所得税费用(万元)'),
               ('基本每股收益单季','基本每股收益'))

               
#年化指标			   
year_data = (('营业总收入年化', '营业总收入单季'),
             ('营业总成本年化', '营业总成本单季'),
             ('利润总额年化', '利润总额单季'),
             ('净利润年化', '净利润单季'),
             ('工业利润自定义年化', '工业利润自定义单季'),
             ('归属于母公司所有者的净利润年化', '归属于母公司所有者的净利润单季'),
             ('少数股东损益年化', '少数股东损益单季'),
             ('经营活动产生的现金流量净额年化', '经营活动产生的现金流量净额单季'),
             ('投资活动产生的现金流量净额年化', '投资活动产生的现金流量净额单季'),
             ('筹资活动产生的现金流量净额年化', '筹资活动产生的现金流量净额单季'),
             ('经营活动现金流出小计年化','经营活动现金流出小计单季'),
             ('经营活动现金流入小计年化','经营活动现金流入小计单季'),
             ('营业税金及附加年化', '营业税金及附加单季'),
             ('所得税费用年化', '所得税费用单季'),
             ('基本每股收益年化','基本每股收益单季'))


#比率指标 
#格式：new_column = （dividend_add_list - dividend_minus_list) / ( divisor_add_list -divisor_minus_list)
rate_data = (('ROE(年化)', (['净利润年化']), (), (['所有者权益(或股东权益)合计(万元)']),() ),
             ('ROA(年化)', (['净利润年化']), (), (['资产总计(万元)']),() ),
             ('CFOE(年化)', (['经营活动产生的现金流量净额年化']), (), (['所有者权益(或股东权益)合计(万元)']),() ),
             ('CFOA(年化)', (['经营活动产生的现金流量净额年化']), (), (['资产总计(万元)']),() ),
             ('CFOD(年化)', (['经营活动产生的现金流量净额年化']), (), (['负债合计(万元)']),() ),
             ('PFOA(年化)', (['现金流量净额年化']), (), (['资产总计(万元)']),() ),
     
             ('毛利率', (['利润总额(万元)']), (), (['营业总收入(万元)']), () ),
             #准备增加毛利率单季             
             ('毛利率(年化)', (['利润总额年化']), (), (['营业总收入年化']), () ),
             ('经营现金流净额比利润(年化)',(['经营活动产生的现金流量净额年化']), (), (['净利润年化']), () ),
             ('母公司利润比例(年化)', (['归属于母公司所有者的净利润年化']), (), (['净利润年化']), () ),
           
             ('每股净资产',(['归属于母公司股东权益合计(万元)']), (), (['实收资本(或股本)(万元)']), () ),
             
             ('资产负债率', (['负债合计(万元)']), (), (['资产总计(万元)']), () ),
             ('本公司账户类现金与总资产比值', (['货币资金(万元)', '交易性金融资产(万元)', '衍生金融资产(万元)', '其他流动资产(万元)']) , () ,(['资产总计(万元)']), () ), 
             ('非本公司账户类现金与总资产比值', (['应收票据(万元)','应收账款(万元)','预付款项(万元)', '其他应收款(万元)']), () , (['资产总计(万元)']), () ),
             ('流动资产与总资产比值', (['流动资产合计(万元)']), () , (['资产总计(万元)']), () ) ,
             ('存货与总资产比值', (['存货(万元)']), () , (['资产总计(万元)']), () ) ,
             ('货币与总资产比值', (['货币资金(万元)']), () , (['资产总计(万元)']), () ) ,
             
             ('现金流入比营业收入(年化)',(['经营活动现金流入小计年化']), (), (['营业总收入年化']), () ),
             ('经营现金流入与总负债比值',(['经营活动现金流入小计年化']), (), (['负债合计(万元)']), () ),
             ('经营现金流量净额与流动负债比值',(['经营活动产生的现金流量净额年化']), (), (['流动负债合计(万元)']), () ),
             
             ('产权比率',(['负债合计(万元)']), (), (['所有者权益(或股东权益)合计(万元)']), () ),
             ('有形净值债务率', (['负债合计(万元)']), (),  (['所有者权益(或股东权益)合计(万元)']), (['无形资产(万元)'])),
             ('流动比率' ,(['流动资产合计(万元)']), () ,(['流动负债合计(万元)']), () ), 
             ('速动比率', (['流动资产合计(万元)']), (['存货(万元)']), (['流动负债合计(万元)']), () ), 
             ('保守速动比率', (['货币资金(万元)', '交易性金融资产(万元)', '应收票据(万元)', '应收账款(万元)']), () ,(['流动负债合计(万元)']), () ))


       
#ROE 净资产收益率=净利润*2/（本年期初净资产+本年期末净资产）  
#营业利润和净利润有误导成分，工业利润（自制指标）和现金流更加有意义           

#增长率指标
growth_data = (('营业总收入YOY', '营业总收入年化'),
               ('营业总成本YOY', '营业总成本年化'),
               ('利润总额YOY', '利润总额年化'),
               ('净利润YOY', '净利润年化'),
               ('经营活动产生的现金流量净额YOY', '经营活动产生的现金流量净额年化'),
               ('所有者权益或股东权益合计YOY', '所有者权益(或股东权益)合计(万元)'),
               ('资产总计YOY', '资产总计(万元)'),
               ('负债合计YOY', '负债合计(万元)'))

def read163data(code):
    errcode = 1
    for i in range(0, 4, 1):
        #code = str(stock).zfill(6)
        url = url_template[i] % (code)
        filename = file_template[i] % (code)
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

def cal_col_data(stock_data,latest_season, oldest_season, new_column, addends, subtrahends):
    print("processing ", new_column, addends, subtrahends)
    for s in range(latest_season, oldest_season , -1):
        result = 0
        for col in addends:


            result = result + stock_data.loc[s, col]
            #print(result)
        for col in subtrahends:


            result = result - stock_data.loc[s, col]
            #print(s,col,result)
        print("seq,column name,result",s,new_column,result)
        stock_data.loc[s, new_column] = result
    return stock_data
    
    
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
            #print(stock_data.loc[s, new_column] , stock_data.loc[s - i, old_column])
            stock_data.loc[s, new_column] += float(stock_data.loc[s - i, old_column])
    return stock_data

    
def cal_rate_data(stock_data,latest_season, oldest_season, new_column, dividend_add, dividend_minus, divisor_add, divisor_minus):
    print("processing ", new_column, dividend_add, dividend_minus, divisor_add, divisor_minus)
    for s in range(latest_season, oldest_season + 4, -1):
        try:
            #分子
            mydividend = 0
            for col in dividend_add:
                mydividend = mydividend + stock_data.loc[s, col]
            for col in dividend_minus:
                mydividend = mydividend - stock_data.loc[s, col]
            #分母
            mydivisor = 0
            for col in divisor_add:
                mydivisor = mydivisor + stock_data.loc[s, col]
            for col in divisor_minus:
                mydivisor = mydivisor - stock_data.loc[s, col]
                
            stock_data.loc[s, new_column] = float(mydividend) / float(mydivisor)
            #mydividend = float(stock_data.loc[s, dividend])  old way
            #mydivisor = float(stock_data.loc[s, divisor])  old way
            #stock_data.loc[s, new_column] = float(mydividend) / float(mydivisor)
        except:
            print("divided by 0 or not number", new_column, dividend_add, dividend_minus, divisor_add, divisor_minus)
            stock_data.loc[s, new_column] = None		
    return stock_data


def cal_growth_data(stock_data,latest_season, oldest_season, new_column, old_column):
    print("caculating growth_data ", new_column, old_column)
    for s in range(latest_season, oldest_season + 8, -1):
        try:
            stock_data.loc[s, new_column] = float(stock_data.loc[s, old_column]) / float(stock_data.loc[s -4 , old_column])   - 1
        except:
            print(stock_data.loc[s, old_column] , stock_data.loc[s -4 , old_column], "divided by 0 or not number")
            stock_data.loc[s, new_column] = None		
    return stock_data

    
def read_163file(code,name):
    print("processing 163 data", code, name)
    #处理从163下载的原始文件，计算三个表的补充指标
    lrb_filename = input_lrb_template1 % (code)
    zcfzb_filename = input_zcfzb_template1 % (code)
    xjllb_filename = input_xjllb_template1 % (code)
    

    lrb = read_df(lrb_filename)
    lrb = lrb.replace("\s*--\s*", 0, regex=True)
    #relpace character -- with 0. it is better than nan.
    #lrb = lrb.replace("(", "（")
    #lrb = lrb.replace(")", "）")
    #避免缺失括号对计算的影响，特别sql的key error
    lrb = lrb.astype('float')
    lrb = lrb.sort_index(ascending=True)
    #print(lrb)
    

    xjllb = read_df(xjllb_filename)
    xjllb = xjllb.replace("\s*--\s*", 0, regex=True)
    #relpace character --
    #xjllb = xjllb.replace("\s*\(\s*", "（", regex=True)
    #xjllb = xjllb.replace("\s*\)\s*", "）", regex=True)
    #避免缺失括号对计算的影响，特别sql的key error
    xjllb = xjllb.astype('float')
    xjllb = xjllb.sort_index(ascending=True)
    #print(xjllb)
        
    zcfzb = read_df(zcfzb_filename)
    zcfzb = zcfzb.replace("\s*--\s*", 0, regex=True)
    #zcfzb = zcfzb.replace("(", "（")
    #zcfzb = zcfzb.replace(")", "）")
    zcfzb = zcfzb.astype('float')
    zcfzb['code'] = code
    zcfzb['name'] = name
    zcfzb = zcfzb.sort_index(ascending=True) 
    #print(zcfzb)
    

    #这三个指标重名，加表头区分
    xjllb['现金流量表的财务费用(万元)'] = xjllb['财务费用(万元)']
    del xjllb['财务费用(万元)']
    xjllb['现金流量表的净利润(万元)'] = xjllb['净利润(万元)']
    del xjllb['净利润(万元)']
    xjllb['现金流量表的少数股东损益(万元)'] = xjllb['少数股东损益(万元)']
    del xjllb['少数股东损益(万元)']
    
    #print(lrb.index)
    #print(xjllb.index)
    #print(zcfzb.index)
    #lrb_len = lrb.count()
    #xjllb_len = xjllb.count()
    #zcfzb_len = zcfzb.count()
    
    #三表合一
    stock_data = zcfzb.copy()
    stock_data = stock_data.join(lrb)
    stock_data = stock_data.join(xjllb)
    #print(stock_data)
    
    #print(lrb.columns)
    #print(xjllb.columns)
    #print(zcfzb.columns)
    print(stock_data.columns)
    print('len of stock_data',len(stock_data))
    
    #此列列名异常，补充右括号
    try:
        stock_data['向中央银行借款净增加额(万元)'] = stock_data['向中央银行借款净增加额(万元']
        del stock_data['向中央银行借款净增加额(万元']
    except:
        print("column 向中央银行借款净增加额(万元 not exit")
        
    #derive year and month, write index
    #stock_data = stock_data[stock_data['营业总收入(万元)'].notnull() ]
    
    #处理原始文件中的异常：存在行，但逗号之间都为空值
    stock_data['date'] = stock_data.index
    stock_data['date'] = stock_data['date'].str.replace('/','-')
    tmp = stock_data['date'].str.split('-', expand=True)
    #tmp = tmp.iloc[1:,:] 原用于去第一行，现删去
    #print(tmp)
    stock_data['年'] = tmp[0].astype('int32')
    stock_data['月'] = tmp[1].astype('int32')
    stock_data['季'] = tmp[1].astype('int32') // 3
   #季从1开始，4结束，3月=第一季，6月第二季，9月第三季，12月第4季
	
	#删除多余的
    stock_data = stock_data[ stock_data['月'] % 3 ==0 ] 
    
    stock_data['seq'] = stock_data['年'].astype('int32') * 4 + stock_data['季'].astype('int32')
    
    #print(stock_data['seq'])
	        
    return stock_data

def cal_indicator(stock_origin):
    stock_data = stock_origin.copy()
    #注意，要求index为seq，在数据库重新读入时一定要set_index一次
    stock_data = stock_data.set_index(stock_data['seq'])
    
    #print(stock_data.columns)
    #print(stock_data.index)
    #print(stock_data['seq'].max(), stock_data['seq'].min())
    
    # get the idnex of   !! oldest !!    seasonal data
    seq_set = set(stock_data['seq'])
    latest_season = stock_data['seq'].max()
    for i in range(latest_season, stock_data['seq'].min() - 1 , -1):
        #print (i)
        if (i in seq_set):
            oldest_season = i
        else:
            break
    print ( "oldest_season:", oldest_season, "  latest_season:", latest_season)
    
    if oldest_season >= latest_season:
        #无法形成单季数据，结束！
        #stock_data.to_csv(bigdata_filename)
        print("error 1: no single season data")
        return 1, "no single season data"
        
    # 计算新的基础列
    for indicators in col_data:
        new_column = indicators[0]
        addends = indicators[1]
        subtrahends = indicators[2]
        print ("creating ",new_column , 'by (',  addends, '-', subtrahends, ')' )
        stock_data = cal_col_data(stock_data, latest_season, oldest_season, new_column, addends, subtrahends)
    
    # 计算单季数据
    for indicators in season_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ",new_column , 'by ', old_column)
        stock_data = cal_season_data(stock_data,latest_season, oldest_season, new_column, old_column)
        
    if oldest_season +4 >= latest_season:
        #无法形成年化数据，结束！
        #stock_data.to_csv(bigdata_filename)
        print("error 2: no anual data")
        return 2, "no anual data"
        
    # 计算年化数据
    for indicators in year_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ",new_column , 'by ', old_column)
        stock_data = cal_year_data(stock_data, latest_season, oldest_season, new_column, old_column)
    
    # 计算比率数据
    for indicators in rate_data:
        new_column = indicators[0]
        dividend_add = indicators[1]
        dividend_minus = indicators[2]
        divisor_add = indicators[3]
        divisor_minus = indicators[4]
        print ("creating ", new_column, 'by (',  dividend_add, '-', dividend_minus, ') / (', divisor_add, '-', divisor_minus, ')')
        stock_data = cal_rate_data(stock_data,latest_season, oldest_season, new_column, dividend_add, dividend_minus, divisor_add, divisor_minus)
     
    if oldest_season +8 >= latest_season:
        #无法形成增长数据
        #stock_data.to_csv(bigdata_filename)
        print("error 3: no growth data")
        return 3 , stock_data
        
    # 计算增长数据
    for indicators in growth_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ", new_column , 'by ', old_column)
        stock_data = cal_growth_data(stock_data,latest_season, oldest_season, new_column, old_column)
    
    #汇总综合数据，暂时没有做过程
    stock_data['现金流量净额年化'] = stock_data['筹资活动产生的现金流量净额年化'] + stock_data['投资活动产生的现金流量净额年化'] +  stock_data['经营活动产生的现金流量净额年化']
    


    #stock_data = stock_data.mask(np.isinf(stock_data))
    #报错，isinf处理不了非数
    
    stock_data = stock_data.replace([np.inf, -np.inf], np.nan)
    #把无限大变长NaN
    stock_data = stock_data.where(stock_data.notnull(),None)
    #把NaN替换成None，以便插入数据库的时候不出错
    #可以处理nan，但处理不了inf
    #print(stock_data.isin([np.nan, np.inf, -np.inf]))
    #stock_data = stock_data.where(stock_data.isin([np.nan, np.inf, -np.inf, float('inf'), float('-inf')]),None)
    
    #stock_data = stock_data.where(np.isfinite(stock_data),None)
    #stock_data = stock_data.replace([np.inf, -np.inf], None)
    #把inf替换成None，以便插入数据库的时候不出错
    
    #print(8043, stock_data.loc[8043,'保守速动比率'])
    #print(type(stock_data.loc[8043,'保守速动比率']))
    #print(list(stock_data.columns))
    
    #filename='/stockdata/163/stockdata.csv'
    #stock_data.to_csv(filename,encoding='utf_8_sig')
    #存为临时表进行核对
    return 0, stock_data

def update_database(conn_mysql,code,name,new_data,table,op='addnewonly'):
    #op='newcoverold', replace by new df
    #op='addnewonly', old df remain, add new line
    print("update database, op=",op)
    
    try:
        sql = "select * from %s where code = %s ; " % (table,code)
        print(sql)
        old_data = pd.read_sql_query(sql, conn_mysql)
    except:
        del new_data['seq']
        new_data.to_sql(table, con=conn_mysql, if_exists='append')
        print("new talbe! save data of %s %s to table %s db! %d lines" % (code,name,table,len(new_data)))
        return 0, len(new_data)

    #print(old_data.index)
    old_seq = set(old_data['seq'])
    print(new_data.index)
    new_seq = set(new_data.index)
    print('old_seq',old_seq)
    print('new_seq',new_seq)
    print('new-old',new_seq - old_seq)
    print('old-new',old_seq - new_seq)
    
    if op == 'newcoverold':
        old_data = old_data[old_data['seq'].isin(old_seq - new_seq)]
        stock_data = pd.concat([old_data, new_data],sort=False)
        #print(stock_data)
        
        #seq作为index保留在df，写入数据库成为seq列，读入又重新成为df的seq列
        stock_data = stock_data.set_index(stock_data['seq'])
        del stock_data['seq']
        
        del_old = sqlalchemy.text('delete from stockdata where code = :x')
        del_old = del_old.bindparams(x=code)
        conn_mysql.execute(del_old)
        print('delete old data of %s %s'% (code, name))
        
        stock_data.to_sql(table, con=conn_mysql, if_exists='append')
        print("replace with new data of %s %s to table %s in db! %d lines" % (code,name,table,len(stock_data)))
        
    elif op == 'addnewonly':
        new_data = new_data[new_data['seq'].isin(new_seq - old_seq)]
        
        #seq作为index保留在df，写入数据库成为seq列，读入又重新成为df的seq列
        new_data = new_data.set_index(new_data['seq'])
        del new_data['seq']
        
        #print(new_data)
        new_data.to_sql(table, con=conn_mysql, if_exists='append')
        print("save only new data of %s %s to table %s db! %d lines" % (code,name,table,len(new_data)))
        
    return 0, max(old_seq | new_seq)

def stockdata_process(conn_mysql, mylist,  op='routine_update'):
    #op = create , means no stockdata at all, maybe new columns will be intruduced
    #op = rebuild , stockdata is already there, but most data are missed
    #op = force_update , means stockdata is already there and update every stock
    
    #日常更新 
    #op = routine_update , means stockdata is already there and just needs update 
    
    #更改列，增加指标。需要首先把表改为stockdata_old，然后重新计算指标，插回stockdata中
    #op = change_col , means create stockdata from stockdata_old
    
    today = datetime.date.today()
    year = today.year
    month = today.month
    seq_today = year * 4 + month // 4
    print("seq_today=",seq_today)
    
    if op == 'rebuild' or op == 'change_col_phase2':
        try:
            stmt= sqlalchemy.text('delete from distinct_code;')
            conn_mysql.execute(stmt)
            print('delete from distinct_code;  success!')
        except:
            print('deleteing failed!')
            
        try:
            stmt= sqlalchemy.text('insert into distinct_code  select distinct code as dcode from stockdata;')
            conn_mysql.execute(stmt)
            print('updating table distinct_code succeded! ')
        except:
            print('fail to update distinct_code!')   
        sql = "select * from stocklist a where not exists ( select dcode from distinct_code b  where b.dcode = a.code ) order by code"
        #very important solution for "not in"
        #这个sql的目标是把stocklist中减去已经存在stockdata的股票代码，只针对它们进行计算。
    else:
        #other case, get full list of stocks from table stocklist 
        sql = "select * from stocklist order by code "
    
    print(sql)
    print(conn_mysql)
    print("read stocklist from db!")
    stocklist = pd.read_sql_query(sql, conn_mysql)
    #print(stocklist)
    #print(stocklist.columns)
    #print(stocklist.dtypes)
    
    for index, row in stocklist.iterrows():
        code = row['code']
        name = row['name']
        stock_seq = row['seq']
        lastread163 = row['lastread163']
        #print(type(today),type(lastread163),type(datetime.timedelta(days=7)))
        print("\n\n\n Now I am processing %s %s" % (code,name))
        
            
        if op == 'routine_update' and stock_seq < seq_today and (today >  (lastread163 + datetime.timedelta(days=7))):
                print("read 163 data now", code, name)
                errcode = read163data(code)
        
        elif op == 'force_update' and  today ==  lastread163 :
            #今天已经查询过163了
            print("already update recently. skip!")
            continue
            
        elif op == 'force_update' and  today >  lastread163:
            #今天还没查询过163，继续强制更新
            path = file_template[3] % (code)
            print(path)
            if not os.path.exists(path):
                continue
                print("read 163 data now", code, name)
                errcode = read163data(code)
            else:
                print("163 file already exist", code, name)
                errcode = 0
        
        elif op == 'change_col_phase1' or op == 'change_col_phase2' :        
            print("change columns %s %s." %(code,name))
            #改变列，不需要去查询163
            #data is at database, table is called stockdata_old
        
        else:
            print("other cases. skip")
            continue
        
        

        #the update part
        if op == 'routine_update' and stock_seq < seq_today and (today >  (lastread163 + datetime.timedelta(days=7))):
        
            data = read_163file(code,name)
            errcode, data = cal_indicator(data)
            print("calculate indicators error: " , errcode)

            if errcode == 0 or errcode == 3:
                #all right or without growth data
                #只更新全对或仅仅缺增长数据的股票，把新指标数据插入stockdata
                errcode, max_seq = update_database(conn_mysql,code,name,data,'stockdata',op='addnewonly')
                print("update database error: " , errcode)
                
            #计算和更新共享errcode，两个过程都是0，才更新stocklist的时间
            #更新stocklist的时间，目的是减少短时间多少查询163
            print("stock process result:  ",errcode)
            if errcode == 0:
                #get 163 data successfully, update date, seq , new_stock_or_not
                s = mylist.update().where(mylist.c.code == code).values(lastread163=today,seq=int(max_seq),new=False)
            else:
                s = mylist.update().where(mylist.c.code == code).values(lastread163=today)
            print(s)
            conn_mysql.execute(s)
            
        elif op == 'force_update' :
            data = read_163file(code,name)
            errcode, data = cal_indicator(data)
            if errcode == 0:
                errcode, max_seq = update_database(conn_mysql,code,name,data,'stockdata',op='addnewonly')
                print("force update database result:  ",errcode, max_seq)
                s = mylist.update().where(mylist.c.code == code).values(lastread163=today,seq=int(max_seq),new=False)
            else:
                print("cal error " ,errcode)
                s = mylist.update().where(mylist.c.code == code).values(lastread163=today)
            conn_mysql.execute(s)

        elif op == 'change_col_phase1' or op == 'change_col_phase2':
            #) and (today >  (lastread163 + datetime.timedelta(days=2))):
            #列更新的特别之处是数据来源为 stockdata_old！！！！  而stockdata是从空表开始建立
            #phase1是正常从空表开始建立，phase2是phase1异常中断后的接力，略过所有已经重建的股票。
            
            sql = "select * from stockdata_old where code = %s" % code
            print(sql)
            data = pd.read_sql_query(sql, conn_mysql)
            #重要！数据库内seq成为列，变回df后必须index为seq
            data = data.set_index(data['seq'])
            #print(len(data))
            #print(data.columns)
            
            #旧数据库没有，从163读
            if len(data) == 0:
                #use data from stockdata_old as many as possible. read form csv file otherwise.
                #旧数据库中没有，需要从163查询
                errcode = read163data(code)
                if errcode == 0:
                    print("read data from 163 file", code, name)
                    data = read_163file(code,name)
                else:
                    #163也没有，放弃
                    return
                    




            #重要一步，重新计算各种指标，实现列改变的目标
            errcode, data = cal_indicator(data)
            print("calculate indicators error: " , errcode)

            if errcode == 0 or errcode == 3:
                #all right or without growth data
                #只更新全对或仅仅缺增长数据的股票，把新指标数据插入stockdata
                errcode, max_seq = update_database(conn_mysql,code,name,data,'stockdata',op='newcoverold')
                print("update database error: " , errcode)
                if errcode == 0:
                    s = mylist.update().where(mylist.c.code == code).values(lastread163=today)
                    print(s)
                    conn_mysql.execute(s)



def change_col_prepare(conn_mysql):
    
    try:
        conn_mysql.execute('drop table stockdata_old;')
    except:
        print('drop table stockdata_old fail')
        
    try:
        conn_mysql.execute('alter table stockdata rename to stockdata_old;')
    except:
        print('alter table stockdata rename fail')

        
def analyze_market(conn_mysql, seq):
    
    '''    today = datetime.date.today()
    year = today.year
    month = today.month
    seq_today = year * 4 + month // 4
    print("seq_today=",seq_today)'''
    
    sql = "select * from stockdata where seq=%s ;\n"  %seq
    print(sql)
    data = pd.read_sql_query(sql, conn_mysql)


    

    return data

    
def drop_column(conn_mysql,columns=[]):
    for col in columns:
        try:
            print('I am going to drop column %s on table %s . Are you sure?' %(col,table_name))
            key = input('Yes/No?')
            if key in ('y','Y','yes','YES'):
                stmt= sqlalchemy.text('alter table stockdata drop column %s' %str(col))
                conn_mysql.execute(stmt)
                print('drop column %s on table %s succeded! ' %(col,table_name))
        except:
            print('fail to update distinct_code!')  


def get_stockdata(conn_mysql, code):

    sql = "select * from stockdata where code = %s ;\n"  %code
    print(sql)
    data = pd.read_sql_query(sql, conn_mysql)
    print(len(data))
    print(data.columns)
    return data
    

def update_kmonth(conn_mysql):

    today = datetime.date.today()
    year = today.year
    month = today.month
    seq_today = year * 12 + month
    print("seq_today=",seq_today)

    table = 'kmonth'

    #获得每个股票当前最新的月线日期
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
            kdata = kdata.set_index('seq',drop=False)
            #print(kdata)
            #插入到kdata表中
            update_database(conn_mysql,code,name,kdata,table,op='addnewonly')

