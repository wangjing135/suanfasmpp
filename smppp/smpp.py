import time
import pandas as pd
import json
import time
import requests
import pymysql.cursors
import pymysql
import csv
from smppp.mysql import selectmysql
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
print('11111')
class suanfaxiapu:
    def __init__(self):
        self.dao = selectmysql()
        print('22')

    #@classmethod
    def cal_all(self,datas):
        col_pnl = 2  # 净值所在的列数
        col_time = 0  # 时间所在列数

        def cal_sharp(self,datas, if_sdd=True):  # data为筛选出来的数据，需要是dataframe形式
            def cal_time(start_time, end_time):  # 计算数据起止日期时间长度
                start_time = [int(item) for item in start_time.split('-')]  # 把'yyyy-m-d’数据转化成['yyyy','m','d']形式的数组
                end_time = [int(item) for item in end_time.split('-')]
                ret = (start_time[0] - end_time[0]) + (start_time[1] - end_time[1]) * 30 / 365.0 + (
                                                                                                       start_time[2] -
                                                                                                       end_time[
                                                                                                           2]) / 365.0  # 计算时间间隔，单位为年
                return ret

                self, datas.iloc[:, col_pnl] = self,datas.iloc[:, col_pnl].apply(lambda x: float(x))
            time_delta = cal_time(self,datas.iloc[-1, col_time], self,datas.iloc[0, col_time])  # 计算数据起止日期时间长度
            returns = (self,datas.iloc[-1, col_pnl] -self,datas.iloc[0, col_pnl]) / (1 + self,datas.iloc[0, col_pnl])  # 计算区间段整体收益
            df = self,datas.iloc[:, col_pnl] + 1  # 净值序列
            chg = df.diff() / df  # 基金涨跌幅度
            variance = (chg ** 2).sum()  # 计算区间段方差
            sharp = returns / (variance * time_delta) ** 0.5  # sharp比率计算
            if if_sdd:
                return [(1 + returns) ** (1 / time_delta) - 1, sharp]
            else:
                return [returns, sharp]

        result = pd.DataFrame(index=['all', '2015', '2016', '2017'], columns=['pnl', 'sharp'])
        for year in result.index[1:]:
            data_year = self,datas[self,datas.iloc[:, col_time].apply(lambda x: x[:4]) == year]
            if year != '2017':
                result.loc[year, :] = cal_sharp(data_year)
            else:
                result.loc[year, :] = cal_sharp(data_year, False)
        result.loc['all', :] = cal_sharp(self,datas[self,datas.iloc[:, col_time] > '2014-12-31'])
        return result
    def xiapu(self):
        namelist=[]
        with open("smpp8.csv", "w+", newline="\n", encoding="utf-8") as datacsv:
            csvwriter = csv.writer(datacsv, dialect=("excel"))
            fp = open(r"C:\Users\wangjing\Desktop\names.txt")
            alldata = fp.read().split("\n")
            for i in range(len(alldata)):
                data = alldata[i].split("\t")
                namelist.append(data[0])
                for name in namelist:
                    print(name)
                    frames = self.dao.select(name)
                    print(frames)
                    a = self.dao.cal_all(frames)
                    #a1 = a(frames)
                    #a2 = a1(start_time, end_time)
                    print(a)

                    #return name
                    logger.info('Finish updating records')
if __name__ == '__main__':
    a_ = suanfaxiapu()
    b_ = a_.xiapu()

    #d_= a_.