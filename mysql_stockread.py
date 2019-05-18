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

#季度化指标
season_data = (('营业收入单季', '营业收入(万元)'),
               ('营业成本单季', '营业成本(万元)'),
               ('营业利润单季', '营业利润(万元)'),
               ('净利润单季', '净利润(万元)'),
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
year_data = (('营业收入年化', '营业收入单季'),
             ('营业成本年化', '营业成本单季'),
             ('营业利润年化', '营业利润单季'),
             ('净利润年化', '净利润单季'),
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
rate_data = (('ROE', '净利润年化', '所有者权益(或股东权益)合计(万元)'),
             ('ROA', '净利润年化', '资产总计(万元)'),
             ('毛利率', '营业利润年化', '营业收入年化'),
             ('经营现金流净额比利润','经营活动产生的现金流量净额年化','净利润年化'),
             ('母公司利润比例', '归属于母公司所有者的净利润年化', '净利润年化'),
             ('资产负债率', '负债合计(万元)', '资产总计(万元)'),
             ('每股净资产','归属于母公司股东权益合计(万元)','实收资本(或股本)(万元)'),
             ('现金流入比营业收入','经营活动现金流入小计年化','营业收入年化'))


#增长率指标
growth_data = (('营业收入YOY', '营业收入年化'),
               ('营业成本YOY', '营业成本年化'),
               ('营业利润YOY', '营业利润年化'),
               ('净利润YOY', '净利润年化'),
               ('经营活动产生的现金流量净额YOY', '经营活动产生的现金流量净额年化'),
               ('所有者权益或股东权益合计YOY', '所有者权益(或股东权益)合计(万元)'),
               ('资产总计YOY', '资产总计(万元)'),
               ('负债合计YOY', '负债合计(万元)'))

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
    print(xjllb.columns)
        
    zcfzb = read_df(zcfzb_filename)
    zcfzb = zcfzb.replace("\s*--\s*", 0, regex=True)
    #zcfzb = zcfzb.replace("(", "（")
    #zcfzb = zcfzb.replace(")", "）")
    
    zcfzb = zcfzb.astype('float')
    zcfzb['code'] = stock_string
    zcfzb['name'] = stock_name
    zcfzb = zcfzb.sort_index(ascending=True) 
    #print(zcfzb)
    

    #提取三个表的重要数据，合成综合表
    xjllb['现金流量表的财务费用(万元)'] = xjllb['财务费用(万元)']
    del xjllb['财务费用(万元)']
    xjllb['现金流量表的净利润(万元)'] = xjllb['净利润(万元)']
    del xjllb['净利润(万元)']
    xjllb['现金流量表的少数股东损益(万元)'] = xjllb['少数股东损益(万元)']
    del xjllb['少数股东损益(万元)']

    #print(lrb.index)
    #print(xjllb.index)
    #print(zcfzb.index)
    lrb_len = lrb.count()
    xjllb_len = xjllb.count()
    zcfzb_len = zcfzb.count()

    stock_data = zcfzb.copy()
    stock_data = stock_data.join(lrb)
    stock_data = stock_data.join(xjllb)
    #以上代码用于三表合一
    #print(stock_data)
    
    #print(lrb.columns)
    #print(xjllb.columns)
    #print(zcfzb.columns)
    print(stock_data.columns)
    
    try:
        stock_data['向中央银行借款净增加额(万元)'] = stock_data['向中央银行借款净增加额(万元']
        del stock_data['向中央银行借款净增加额(万元']
    except:
        print("column 向中央银行借款净增加额(万元 not exit")
    
    stock_data['毛利率'] = stock_data['营业利润(万元)'] / stock_data['营业总收入(万元)']
    stock_data['资产负债率'] = stock_data['负债合计(万元)'] / stock_data['资产总计(万元)']
    stock_data['产权比率'] = stock_data['负债合计(万元)'] / stock_data['所有者权益(或股东权益)合计(万元)']
    stock_data['有形净值债务率'] = stock_data['负债合计(万元)'] / (stock_data['所有者权益(或股东权益)合计(万元)'] - stock_data['无形资产(万元)'])
    stock_data['流动比率'] = stock_data['流动资产合计(万元)'] / stock_data['流动负债合计(万元)'] 
    stock_data['速动比率'] = (stock_data['流动资产合计(万元)'] - stock_data['存货(万元)'] ) / stock_data['流动负债合计(万元)'] 
    stock_data['保守速动比率'] = (stock_data['货币资金(万元)'] + stock_data['交易性金融资产(万元)'] + stock_data['应收票据(万元)'] + stock_data['应收账款(万元)']) / stock_data['流动负债合计(万元)'] 
    stock_data['本公司账户类现金与总资产比值'] = (stock_data['货币资金(万元)'] + stock_data['交易性金融资产(万元)'] + stock_data['衍生金融资产(万元)'] + stock_data['其他流动资产(万元)']) / stock_data['资产总计(万元)'] 
    stock_data['非本公司账户类现金与总资产比值'] = (stock_data['应收票据(万元)'] + stock_data['应收账款(万元)'] + stock_data['预付款项(万元)'] + stock_data['其他应收款(万元)']) / stock_data['资产总计(万元)'] 

    
   

    #print(stock_data['营业收入(万元)'])
    
    # derive year and month, write index
    stock_data = stock_data[stock_data['营业收入(万元)'].notnull() ]
    #处理原始文件中的异常：存在行，但逗号之间都为空值
    #print(stock_data)
    stock_data['date'] = stock_data.index
    stock_data['date'] = stock_data['date'].str.replace('/','-')
    tmp = stock_data['date'].str.split('-', expand=True)
    #tmp = tmp.iloc[1:,:] 原用于去第一行，现删去
    #print(tmp)
    stock_data['年'] = tmp[0].astype('int32')
    stock_data['月'] = tmp[1].astype('int32')
    stock_data['季'] = tmp[1].astype('int32') // 3
	
	#删除多余的
    stock_data = stock_data[ stock_data['月'] % 3 ==0 ] 
    
    stock_data['seq'] = stock_data['年'].astype('int32') * 4 + stock_data['季'].astype('int32')
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
        return 1, "no single season data"
	
    # 计算单季数据
    for indicators in season_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ",new_column , 'by ', old_column)
        stock_data = cal_season_data(stock_data,latest_season, oldest_season, new_column, old_column)

    if oldest_season +4 >= latest_season:
        #无法形成年化数据，结束！
        #stock_data.to_csv(bigdata_filename)
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
        dividend = indicators[1]
        divisor = indicators[2]
        print ("creating ", new_column, 'by ', dividend, divisor)
        stock_data = cal_rate_data(stock_data,latest_season, oldest_season, new_column, dividend, divisor)

    if oldest_season +8 >= latest_season:
        #无法形成增长数据
        #stock_data.to_csv(bigdata_filename)
        return 3 , "no growth data"
        
    # 计算增长数据
    for indicators in growth_data:
        new_column = indicators[0]
        old_column = indicators[1]
        print ("creating ", new_column , 'by ', old_column)
        stock_data = cal_growth_data(stock_data,latest_season, oldest_season, new_column, old_column)
    

    max_seq = stock_data['seq'].max()
    del stock_data['seq']
    #seq作为index保留
    
    stock_data = stock_data.where(stock_data.notnull(),None)
    #stock_data = stock_data.where(stock_data.isinf(),None)
    #把NaN替换成None，以便插入数据库的时候不出错
    
    print(list(stock_data.columns))

    filename='/stockdata/163/stockdata.csv'
    stock_data.to_csv(filename,encoding='utf_8_sig')
    #存为临时表进行核对
    
    stmt = sqlalchemy.text("SELECT count(*) as data_count FROM stockdata WHERE code = :x ")
    stmt = stmt.bindparams(x=code)
    #print(str(stmt))
    result = conn_mysql.execute(stmt).fetchone()
    old_count = result['data_count']
    print("there are %s old data of %s in db" % (old_count,name))
    if old_count > 0:
        del_old = sqlalchemy.text('delete from stockdata where code = :x')
        del_old = del_old.bindparams(x=code)
        conn_mysql.execute(del_old)
        print('delete old data of %s %s'% (code, name))
        
    stock_data.to_sql('stockdata', con=conn_mysql, if_exists='append')
    print("save data of %s %s to db!" % (code,name))
    return 0, max_seq
 
 
engine = sqlalchemy.create_engine("mysql+pymysql://stock:stock@104.225.154.46:3306/stock?use_unicode=0&charset=utf8",encoding='utf-8',echo=False,max_overflow=5)
metadata = sqlalchemy.MetaData(engine)
conn_mysql = engine.connect()
stockdata = sqlalchemy.Table('stockdata', metadata, autoload=True, autoload_with=engine)


sql = "select * from stocklist "
stocklist = pd.read_sql_query(sql, conn_mysql)
print(stocklist)
print(stocklist.columns)
print(stocklist.dtypes)
today = datetime.date.today()
year = today.year
month = today.month
seq_today = year * 4 + month // 4
print("seq_today=",seq_today)

mylist = sqlalchemy.Table('stocklist', metadata, autoload=True, autoload_with=engine)

for index, row in stocklist.iterrows():
    code = row['code'].decode()
    name = row['name'].decode()
    stock_seq = row['seq']
    lastread163 = row['lastread163']
    print(type(today),type(lastread163),type(datetime.timedelta(days=7)))
    
    if (stock_seq < seq_today) and (today >  (lastread163 + datetime.timedelta(days=7))):
        print("read 163 data now", code, name)
        errcode = read163data(code)
        print("result of reading 163 data :  %d", errcode)
        
        if errcode != 0:
            print("fail to read 163 data of %s %s" %(code,name))
            continue
            #skip this stock
    
        errcode, max_seq = stock_process(code,name)
        print("stock_process  ",errcode, max_seq)
        if errcode == 0:
            #get 163 data successfully, update date, seq , new_stock_or_not
            s = mylist.update().where(mylist.c.code == code).values(lastread163=today,seq=int(max_seq),new=False)
        else:
            s = mylist.update().where(mylist.c.code == code).values(lastread163=today)
        print(s)
        conn_mysql.execute(s)

conn_mysql.close()