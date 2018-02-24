import time
import pandas as pd
import json
import time
import requests
import pymysql.cursors
import pymysql
import csv
import sys
#sys.path.append('C:\Users\wangjing\PycharmProjects\suanfasmpp\smppp\mysql.py')
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
    def xiapu(self):
        # with open("smpp8.csv", "w+", newline="\n", encoding="utf-8") as datacsv:
        #     csvwriter = csv.writer(datacsv, dialect=("excel"))
        fp = open(r"C:\Users\wangjing\Desktop\100.txt", encoding='UTF-8')
        alldata = fp.read().split("\n")
        #listall=[]
        for i in range(len(alldata)):
            data = alldata[i].split("\t")
            listall=[]
            fund_id = data[0]
            listall.append(fund_id)
            print(fund_id)
            frames = self.dao.select_id_2017('fund_id')
            print(frames)
            #print(frames)
            a = self.dao.cal_all(frames)
            times = list(a.index)
            pnls = list(a['pnl'])
            print(a)
            sharps = list(a['sharp'])
            for time1_, pnl_, sharp_ in zip(times, pnls, sharps):
                time1 = str(time1_)
                listall.append(time1)
                pnl = str(pnl_)
                listall.append(pnl)
                sharp = str(sharp_)
                listall.append(sharp)
                #listall.append(name)
                #print(name,time1,pnl,sharp)
            print(listall)
            fund_id = listall[0]
            allrate = float(listall[2])
            #print(allrate)
            allxiapu = float(listall[3])
            time1 = float(listall[5])
            time2 = float(listall[6])
            time4 = float(listall[8])
            time5 = float(listall[9])
            time6 =float(listall[11])
            time7 = float(listall[12])
            print(fund_id,allrate, allxiapu, time1, time2, time4, time5, time6, time7)
            #self.dao.insert_base(fund_id, allrate, allxiapu, time1, time2, time4, time5, time6, time7)
            return a
            logger.info('Finish updating records')
if __name__ == '__main__':
    a_ = suanfaxiapu()
    b_ = a_.xiapu()