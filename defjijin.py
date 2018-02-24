# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:21:34 2017

@author: wangjing
"""

import time
import pandas as pd
import json
import time
import requests
import pymysql.cursors
import pymysql
import csv


def cal_all(datas):
    col_pnl = 2  # 净值所在的列数
    col_time = 0  # 时间所在列数

    def cal_sharp(data, if_sdd=True):  # data为筛选出来的数据，需要是dataframe形式
        def cal_time(start_time, end_time):  # 计算数据起止日期时间长度
            start_time = [int(item) for item in start_time.split('-')]  # 把'yyyy-m-d’数据转化成['yyyy','m','d']形式的数组
            end_time = [int(item) for item in end_time.split('-')]
            ret = (start_time[0] - end_time[0]) + (start_time[1] - end_time[1]) * 30 / 365.0 + (
                                                                                                   start_time[2] -
                                                                                                   end_time[
                                                                                                       2]) / 365.0  # 计算时间间隔，单位为年
            return ret

        data.iloc[:, col_pnl] = data.iloc[:, col_pnl].apply(lambda x: float(x))
        time_delta = cal_time(data.iloc[-1, col_time], data.iloc[0, col_time])  # 计算数据起止日期时间长度
        returns = (data.iloc[-1, col_pnl] - data.iloc[0, col_pnl]) / (1 + data.iloc[0, col_pnl])  # 计算区间段整体收益
        df = data.iloc[:, col_pnl] + 1  # 净值序列
        chg = df.diff() / df  # 基金涨跌幅度
        variance = (chg ** 2).sum()  # 计算区间段方差
        sharp = returns / (variance * time_delta) ** 0.5  # sharp比率计算
        if if_sdd:
            return [returns / time_delta, sharp]
        else:
            return [returns, sharp]

    result = pd.DataFrame(index=['all', '2015', '2016', '2017'], columns=['pnl', 'sharp'])
    for year in result.index[1:]:
        data_year = datas[datas.iloc[:, col_time].apply(lambda x: x[:4]) == year]
        if year != '2017':
            result.loc[year, :] = cal_sharp(data_year)
        else:
            result.loc[year, :] = cal_sharp(data_year, False)
    result.loc['all', :] = cal_sharp(datas[datas.iloc[:, col_time] > '2014-12-31'])
    return result


def mysql():
    connection = pymysql.connect(host='58edd9c77adb6.bj.cdb.myqcloud.com',
                                 port=5432,
                                 user='root',
                                 password='1160329981wang',
                                 db='hotshuju',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    return (connection, cur)


def mysqlselect(name):
    connection, cur = mysql()
    # cur.execute("sELECT * FROM allrate1 WHERE title  LIKE '%%%s%%'" %(name))
    # cur.execute("sELECT * FROM smpp_ratess2 WHERE title  LIKE '中国龙稳健' AND categories >='2014-12-31'")
    cur.execute("sELECT * FROM smpp_ratess2 WHERE title  LIKE '%%%s%%' AND categories >='2014-12-31'" % (name))
    datae = cur.fetchall()
    frame = pd.DataFrame(list(datae))
    a = cal_all(frame)
    print(frame)
    print(a)
    return a


def pandas(a):
    try:
        # a.std()
        # stdpnl= a.std()['pnl']
        # stdsharp= a.std()['sharp']
        times = list(a.index)
        pnls = list(a['pnl'])
        print(pnls)
        sharps = list(a['sharp'])
    except:
        pass
    # for time_, pnl_, sharp_ in zip(times, pnls, sharps):
    #     time1 = str(time_)
    #     pnl = str(pnl_)
    #     sharp = str(sharp_)
    #     print(time1, pnl, sharp)
    return (times, pnls, sharps)
    # def zips(stdpnl,stdsharp,times,pnls,sharps):
    # stdpnl_ = stdpnl
    # stdsharp_=stdsharp
    # for time,pnl,sharp in zip(times,pnls,sharps):
    # print(time,pnl,sharp)
    # print(stdpnl_,stdsharp_,time,pnl,sharp)
    # yield (stdpnl_,stdsharp_,time,pnl,sharp)


def tocsv(name, time1, pnl, sharp):
    with open("smpp1.csv", "w+", newline="\n", encoding="gbk") as datacsv:
        csvwriter = csv.writer(datacsv, dialect=("excel"))
        csvwriter.writerow([name, time1, pnl, sharp])
        return (name, time1, pnl, sharp)


if __name__ == '__main__':
    import logging
    import logging.config

    logging.basicConfig(filename='example.log', filemode="w", level=logging.DEBUG)
    # logger = logging.getLogger('wwwww')
    # listall = ['聚发(11)混沌1号','双赢5期(民森)']
    # file_object = open('23.txt', 'w')
    with open("smpps11.csv", "w", newline='', encoding="gbk") as datacsv:
        csvwriter = csv.writer(datacsv, dialect="excel")
        fp = open(r"C:\Users\wangjing\Desktop\names.txt")
        alldata = fp.read().split("\n")
        for i in range(len(alldata)):
            data = alldata[i].split("\t")
            name = data[0]
            name1 = str(name)
            # for name in listall:
            name_ = mysqlselect(name)
            b_ = name_
            d_ = pandas(b_)
            times, pnls, sharps = d_
            # file_object = open('22.txt', 'w')
            for time1_, pnl_, sharp_ in zip(times, pnls, sharps):
                time1 = str(time1_)
                pnl = str(pnl_)
                sharp = str(sharp_)
                # print([(name1), (time1), (pnl), (sharp)])
                logging.info([(name1), (time1), (pnl), (sharp)])

                # file_object.writelines ([(name), (time1), (pnl), (sharp)])
                csvwriter.writerow([(name1), (time1), (pnl), (sharp)])
                # times= list(b_.index)
                # pnls = list(b_['pnl'])
                # print(pnls)
                # sharps= list(b_['sharp'])
                # for time,pnl,sharp in zip(times,pnls,sharps):
                # print(time,pnl,sharp)
                # d_ = pandas(b)
                # a,b,c = d_
                # print(a,b,c)
                # zips(a,b,c,d,e)
                # z_=zips(a,b,c,d,e)
                # a1,b1,c1,d1,e1= z_
                # print(a1,b1,c1,d1,e1)