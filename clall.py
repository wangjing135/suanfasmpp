# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 16:31:09 2017

@author: wangjing
"""

import csv

with open("zhongzheng3.csv", "w", newline='', encoding="gbk") as datacsv:
    csvwriter = csv.writer(datacsv, dialect="excel")
    listall = []
    fp = open(r"C:\Users\wangjing\Desktop\22.txt")
    alldata = fp.read().split("\n")
    # for i in range(len(alldata)):
    for i in range(599):
        try:
            data1 = alldata[4 * i + 0].split("\t")
            data2 = alldata[4 * i + 1].split("\t")
            data3 = alldata[4 * i + 2].split("\t")
            data4 = alldata[4 * i + 3].split("\t")
        except:
            data1 = ''
            data2 = ''
            data3 = ''
            data4 = ''
        if data1 == '' or data2 == '' or data3 == '' or data4 == '':
            continue
        listall = data1 + data2 + data3 + data4

        name = listall[0]
        allrate = listall[2]
        allxiapu = listall[3]
        time1 = listall[6]
        time2 = listall[7]
        time4 = listall[10]
        time5 = listall[11]
        time6 = listall[14]
        time7 = listall[15]
        # print(name,allrate,allxiapu,time1,time2,time4,time5,time6,time7)
        csvwriter.writerow([name, allrate, allxiapu, time1, time2, time4, time5, time6, time7])
        print(listall)
