''' 处理从163下载的原始文件，导入数据库，计算各类指标'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import urllib
import pathlib
import time
import math
import gc
import sqlite3 as lite

from pylab import mpl

mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

#季度化指标
season_data = (('营业收入单季', '营业收入万元'),
               ('营业成本单季', '营业成本万元'),
               ('营业利润单季', '营业利润万元'),
               ('净利润单季', '净利润万元'),
               ('归属于母公司所有者的净利润单季', '归属于母公司所有者的净利润万元'),
               ('少数股东损益单季', '少数股东损益万元'),
               ('经营活动产生的现金流量净额单季', '经营活动产生的现金流量净额万元'),
               ('投资活动产生的现金流量净额单季', '投资活动产生的现金流量净额万元'),
               ('筹资活动产生的现金流量净额单季', '筹资活动产生的现金流量净额万元'),
               ('经营活动现金流出小计单季','经营活动现金流出小计万元'),
               ('经营活动现金流入小计单季','经营活动现金流入小计万元'),
               ('营业税金及附加单季', '营业税金及附加万元'),
               ('所得税费用单季', '所得税费用万元'),
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
rate_data = (('ROE', '净利润年化', '所有者权益或股东权益合计万元'),
             ('ROA', '净利润年化', '资产总计万元'),
             ('毛利率', '营业利润年化', '营业收入年化'),
             ('经营现金流净额比利润','经营活动产生的现金流量净额年化','净利润年化'),
             ('母公司利润比例', '归属于母公司所有者的净利润年化', '净利润年化'),
             ('资产负债率', '负债合计万元', '资产总计万元'),
             ('每股净资产','归属于母公司股东权益合计万元','实收资本或股本万元'),
             ('现金流入比营业收入','经营活动现金流入小计年化','营业收入年化'))


#增长率指标
growth_data = (('营业收入YOY', '营业收入年化'),
               ('营业成本YOY', '营业成本年化'),
               ('营业利润YOY', '营业利润年化'),
               ('净利润YOY', '净利润年化'),
               ('经营活动产生的现金流量净额YOY', '经营活动产生的现金流量净额年化'),
               ('所有者权益或股东权益合计YOY', '所有者权益或股东权益合计万元'),
               ('资产总计YOY', '资产总计万元'),
               ('负债合计YOY', '负债合计万元'))



chart1_items=('营业收入年化',
'营业利润年化',
'净利润年化',
'归属于母公司所有者的净利润年化',
'经营活动产生的现金流量净额年化',
'营业税金及附加年化')


chart2_items=('货币资金万元',
'期末现金及现金等价物余额万元',
'资产总计万元',
'负债合计万元',
'所有者权益或股东权益合计万元',
'存货万元')

chart3_items=('ROE',
'ROA',
'资产负债率',
'毛利率',
'母公司利润比例',
'经营现金流净额比利润',
'现金流入比营业收入')

chart4_items=('营业收入年化',
'经营活动现金流入小计年化',
'经营活动现金流出小计年化',
'经营活动产生的现金流量净额年化',
'投资活动产生的现金流量净额年化',
'筹资活动产生的现金流量净额年化')

#'购买商品、接受劳务支付的现金万元',
#'筹资活动现金流入小计万元',
#'筹资活动现金流出小计万元',
#'投资活动现金流入小计万元',
#'投资活动现金流出小计万元',

chart5_items=('营业收入YOY',
'净利润YOY',
'所有者权益或股东权益合计YOY',
'资产总计YOY',
'负债合计YOY')


#流动资产
chart_ldzc_items=('货币资金万元',
'结算备付金万元',
'拆出资金万元',
'交易性金融资产万元',
'衍生金融资产万元',
'应收票据万元',
'应收账款万元',
'预付款项万元',
'应收保费万元',
'应收分保账款万元',
'应收分保合同准备金万元',
'应收利息万元',
'应收股利万元',
'其他应收款万元',
'应收出口退税万元',
'应收补贴款万元',
'应收保证金万元',
'内部应收款万元',
'买入返售金融资产万元',
'存货万元',
'待摊费用万元',
'待处理流动资产损益万元',
'一年内到期的非流动资产万元',
'其他流动资产万元')

#非流动资产
chart_fldzc_items=('发放贷款及垫款万元',
'可供出售金融资产万元',
'持有至到期投资万元',
'长期应收款万元',
'长期股权投资万元',
'其他长期投资万元',
'投资性房地产万元',
'固定资产万元',
'在建工程万元',
'工程物资万元',
'固定资产清理万元',
'生产性生物资产万元',
'公益性生物资产万元',
'油气资产万元',
'无形资产万元',
'开发支出万元',
'商誉万元',
'长期待摊费用万元',
'股权分置流通权万元',
'递延所得税资产万元',
'其他非流动资产万元')

#非流动负债
chart_ldfz_items=('短期借款万元',
'向中央银行借款万元',
'吸收存款及同业存放万元',
'拆入资金万元',
'交易性金融负债万元',
'衍生金融负债万元',
'应付票据万元',
'应付账款万元',
'预收账款万元',
'卖出回购金融资产款万元',
'应付手续费及佣金万元',
'应付职工薪酬万元',
'应交税费万元',
'应付利息万元',
'应付股利万元',
'其他应交款万元',
'应付保证金万元',
'内部应付款万元',
'其他应付款万元',
'预提费用万元',
'预计流动负债万元',
'应付分保账款万元',
'保险合同准备金万元',
'代理买卖证券款万元',
'代理承销证券款万元',
'国际票证结算万元',
'国内票证结算万元',
'递延收益万元',
'应付短期债券万元',
'一年内到期的非流动负债万元',
'其他流动负债万元')

#非流动负债
chart_fldfz_items=('长期借款万元',
'应付债券万元',
'长期应付款万元',
'专项应付款万元',
'预计非流动负债万元',
'长期递延收益万元',
'递延所得税负债万元',
'其他非流动负债万元')

chart_crqr_items=('流动比率',
'速动比率',
'保守速动比率')
#'产权比率',
#'有形净值债务率'

chart_zzczb_items=('本公司账户类现金与总资产比值',
'非本公司账户类现金与总资产比值')

#所有收入
chart_earning_items=('营业收入万元',
'利息收入万元',
'已赚保费万元',
'手续费及佣金收入万元',
'房地产销售收入万元',
'其他业务收入万元',
'公允价值变动收益万元',
'投资收益万元',
'对联营企业和合营企业的投资收益万元',
'汇兑收益万元',
'期货损益万元',
'托管收益万元',
'补贴收入万元',
'其他业务利润万元')

#所有成本
chart_cost_items=('营业成本万元',
'利息支出万元',
'手续费及佣金支出万元',
'房地产销售成本万元',
'研发费用万元',
'退保金万元',
'赔付支出净额万元',
'提取保险合同准备金净额万元',
'保单红利支出万元',
'分保费用万元',
'其他业务成本万元',
'营业税金及附加万元',
'销售费用万元',
'管理费用万元',
'财务费用万元',
'资产减值损失万元')

#利润
chart_profit_items=('营业利润万元',
'营业外收入万元',
'营业外支出万元',
'非流动资产处置损失万元',
'利润总额万元',
'所得税费用万元',
'未确认投资损失万元',
'净利润万元',
'归属于母公司所有者的净利润万元')


input_dir = "/stockdata/163/"
output_dir = "/stockdata/163bigtable/"
save_dir = "/stockdata/163save/"
wholemarket_dir = "/stockdata/"
db_file = '/stockdata/stock.db'


input_cwbbzy_template1 = input_dir + "cwbbzy_%s.csv"
input_lrb_template1 = input_dir + "lrb_%s.csv"
input_zcfzb_template1 = input_dir + "zcfzb_%s.csv"
input_xjllb_template1 = input_dir + "xjllb_%s.csv"

save_lrb_template1 = save_dir + "%s%s利润表.csv"
save_zcfzb_template1 = save_dir + "%s%s资产负债表.csv"
save_xjllb_template1 = save_dir + "%s%s现金流量表.csv"
save_cwbbzy_template1 = save_dir + "%s%s简表.csv"

output_bigdata_template = output_dir + "%s%s.csv"
output_pic1_template = output_dir + "%s%s汇总图.jpg"
zcfzb_pic_template = output_dir + "%s%s资产负债图.jpg"
lrb_pic_template = output_dir + "%s%s利润图.jpg"
xjllb_pic_template = output_dir + "%s%s现金流量图.jpg"

wholemarket_filename = wholemarket_dir + "wholemarket.csv"


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

'''还是从新浪获取分红数据比较合理    
def cal_XR_factor(stock_data,latest_season, oldest_season):
    #通过净资产变化计算除权因子
    print("caculating XR_factor ", new_column, old_column)
    
    stock_data.loc[latest_season, '每股净资产下季'] = 0
    stock_data.loc[latest_season, '单季除权因子'] =  1
    stock_data.loc[latest_season, '累积除权因子'] =  1
    for s in range(latest_season -1, oldest_season , -1):
        try:
            stock_data.loc[s, '每股净资产下季'] = stock_data.loc[s+1, '每股净资产'] - stock_data.loc[s+1, '基本每股收益']
            stock_data.loc[s, '单季除权因子'] =  stock_data.loc[s, '每股净资产上季'] / stock_data.loc[s-1, '每股净资产'] 
        except:
            print(stock_data.loc[s, old_column] , stock_data.loc[s -4 , old_column], "divided by 0 or not number")
            stock_data.loc[s, new_column] = 0
    print(stock_data['除权因子'])         
    print(stock_data['每股净资产上季'])         
    return stock_data'''

    
def stock_process(stock_string,stock_name):

    #处理从163下载的原始文件，计算三个表的补充指标
    #cwbbzy_filename = input_cwbbzy_template1 % (stock_string)
    lrb_filename = input_lrb_template1 % (stock_string)
    zcfzb_filename = input_zcfzb_template1 % (stock_string)
    xjllb_filename = input_xjllb_template1 % (stock_string)

    lrb_savefile = save_lrb_template1 % (stock_string,stock_name)
    zcfzb_savefile = save_zcfzb_template1 % (stock_string,stock_name)
    xjllb_savefile = save_xjllb_template1 % (stock_string,stock_name)
    #cwbbzy_savefile = save_cwbbzy_template1 % (stock_string,stock_name)
    
    zcfzb_pic_filename = zcfzb_pic_template % (stock_string,stock_name)
    lrb_pic_filename = lrb_pic_template % (stock_string,stock_name)
    xjllb_pic_filename = xjllb_pic_template % (stock_string,stock_name)
    


    
    lrb = read_df(lrb_filename)
    lrb = lrb.replace("\s*--\s*", 0, regex=True)
    #relpace character --
    lrb = lrb.astype('float')
    #lrb['毛利率'] = lrb['营业利润万元'] / lrb['营业总收入万元']
    lrb['code'] = stock_string
    lrb['name'] = stock_name
    lrb.to_csv(lrb_savefile)
    lrb = lrb.sort_index(ascending=True)
    lrb.to_sql('lrb', con=conn, if_exists='append')


    
    xjllb = read_df(xjllb_filename)
    xjllb = xjllb.replace("\s*--\s*", 0, regex=True)
    #relpace character --
    xjllb = xjllb.astype('float')
    xjllb['code'] = stock_string
    xjllb['name'] = stock_name
    xjllb.to_csv(xjllb_savefile)
    xjllb = xjllb.sort_index(ascending=True)
    xjllb.to_sql('xjllb', con=conn, if_exists='append')
        
    zcfzb = read_df(zcfzb_filename)
    zcfzb = zcfzb.replace("\s*--\s*", 0, regex=True)
    zcfzb = zcfzb.astype('float')
    #zcfzb['资产负债率'] = zcfzb['负债合计万元'] / zcfzb['资产总计万元']
    #zcfzb['产权比率'] = zcfzb['负债合计万元'] / zcfzb['所有者权益或股东权益合计万元']
    #zcfzb['有形净值债务率'] = zcfzb['负债合计万元'] / (zcfzb['所有者权益或股东权益合计万元'] - zcfzb['无形资产万元'])
    #zcfzb['流动比率'] = zcfzb['流动资产合计万元'] / zcfzb['流动负债合计万元'] 
    #zcfzb['速动比率'] = (zcfzb['流动资产合计万元'] - zcfzb['存货万元'] ) / zcfzb['流动负债合计万元'] 
    #zcfzb['保守速动比率'] = (zcfzb['货币资金万元'] + zcfzb['交易性金融资产万元'] + zcfzb['应收票据万元'] + zcfzb['应收账款万元']) / zcfzb['流动负债合计万元'] 
    #zcfzb['本公司账户类现金与总资产比值'] = (zcfzb['货币资金万元'] + zcfzb['交易性金融资产万元'] + zcfzb['衍生金融资产万元'] + zcfzb['其他流动资产万元']) / zcfzb['资产总计万元'] 
    #zcfzb['非本公司账户类现金与总资产比值'] = (zcfzb['应收票据万元'] + zcfzb['应收账款万元'] + zcfzb['预付款项万元'] + zcfzb['其他应收款万元']) / zcfzb['资产总计万元'] 
    zcfzb['code'] = stock_string
    zcfzb['name'] = stock_name
    zcfzb.to_csv(zcfzb_savefile)
    zcfzb = zcfzb.sort_index(ascending=True) 
    zcfzb.to_sql('zcfzb', con=conn, if_exists='append')    

    cur.close()
    conn.close()

   

    
def formula_cal(stock_string,stock_name):
    #提取三个表的重要数据，合成综合表
    empty = pd.DataFrame()
    

    query = "SELECT * from lrb where code ='" + stock_string + "';"
    lrb = pd.read_sql_query(query,conn)
    lrb = lrb.set_index(lrb['index'])
    del lrb['level_0']
    del lrb['code']
    del lrb['name']
    del lrb['index']
    print(lrb.count())
    
    query = "SELECT * from xjllb where code ='" + stock_string + "';"
    xjllb = pd.read_sql_query(query,conn)
    xjllb = xjllb.set_index(xjllb['index'])
    del xjllb['level_0']
    del xjllb['code']
    del xjllb['name']
    del xjllb['index']
    del xjllb['财务费用万元']
    del xjllb['净利润万元']
    del xjllb['少数股东损益万元']
    print(xjllb.count())
 
    query = "SELECT * from zcfzb where code ='" + stock_string + "';"
    zcfzb = pd.read_sql_query(query,conn)
    zcfzb = zcfzb.set_index(zcfzb['index'])
    del zcfzb['level_0']
    print(zcfzb.count())

    stock_data = zcfzb.copy()
    stock_data = stock_data.join(lrb)
    stock_data = stock_data.join(xjllb)
    #以上代码用于三表合一
    
    
    stock_data['毛利率'] = stock_data['营业利润万元'] / stock_data['营业总收入万元']
    stock_data['资产负债率'] = stock_data['负债合计万元'] / stock_data['资产总计万元']
    stock_data['产权比率'] = stock_data['负债合计万元'] / stock_data['所有者权益或股东权益合计万元']
    stock_data['有形净值债务率'] = stock_data['负债合计万元'] / (stock_data['所有者权益或股东权益合计万元'] - stock_data['无形资产万元'])
    stock_data['流动比率'] = stock_data['流动资产合计万元'] / stock_data['流动负债合计万元'] 
    stock_data['速动比率'] = (stock_data['流动资产合计万元'] - stock_data['存货万元'] ) / stock_data['流动负债合计万元'] 
    stock_data['保守速动比率'] = (stock_data['货币资金万元'] + stock_data['交易性金融资产万元'] + stock_data['应收票据万元'] + stock_data['应收账款万元']) / stock_data['流动负债合计万元'] 
    stock_data['本公司账户类现金与总资产比值'] = (stock_data['货币资金万元'] + stock_data['交易性金融资产万元'] + stock_data['衍生金融资产万元'] + stock_data['其他流动资产万元']) / stock_data['资产总计万元'] 
    stock_data['非本公司账户类现金与总资产比值'] = (stock_data['应收票据万元'] + stock_data['应收账款万元'] + stock_data['预付款项万元'] + stock_data['其他应收款万元']) / stock_data['资产总计万元'] 

    
   

    print(stock_data['营业收入万元'])
    
    # derive year and month, write index
    stock_data = stock_data[stock_data['营业收入万元'].notnull() ]
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
    print("save to db!")
 

'''这部分是第一次入库用
conn=lite.connect(db_file)
cur = conn.cursor()

try:
    cur.execute('drop table lrb;')
except:
    print('dropping fail. no such table :lrb')

try:
    cur.execute('drop table xjllb;')
except:
    print('dropping fail. no such table :xjllb')

try:
    cur.execute('drop table zcfzb;')
except:
    print('dropping fail. no such table :zcfzb')
    
cur.close()
conn.close()''' 

#action = 'process data'
action = 'formula_cal'
#开关变变量，觉得处理文件，还是计算指标


db_file = '/code/stock/stock.db'

conn=lite.connect(db_file)
conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
#allow chinese character in database
cur = conn.cursor()

sql = "select * from stocklist"
stock_list = pd.read_sql_query(sql, conn)

    
for i in stock_list.index:
    stock, stock_name = stock_list.ix[i, 'code'], stock_list.ix[i, 'name']
    #已修正 stock_name = stock_name.replace('*','')
    #已修正 stock_string = str(stock).zfill(6)
    print(action, stock_string, stock_name)

    #读取并处理163原始文件
    if action == 'process data':
        stock_process(stock_string,stock_name)

    #合并计算指标
    elif action == 'formula_cal':
        formula_cal(stock_string,stock_name)


cur.close()
conn.close()