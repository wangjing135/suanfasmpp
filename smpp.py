import time
import pandas as pd
import json
import time
import requests
import pymysql.cursors
import pymysql
import defs
import csv
# listall =

connection = pymysql.connect(host='58edd9c77adb6.bj.cdb.myqcloud.com',
                             port=5432,
                             user='root',
                             password='1160329981wang',
                             db='hotshuju',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cur = connection.cursor()

fp = open(r"C:\Users\wangjing\Desktop\name.txt")
alldata = fp.read().split("\n")
for i in range(len(alldata)):
    data = alldata[i].split("\t")
    name = data[0]
    print(name)


    cur.execute("sELECT * FROM allrate1 WHERE title  LIKE '%s'" % (name))
    datae = cur.fetchall()
    frame = pd.DataFrame(list(datae))


    try:
        a = defs.cal_all(frame)
    except:
        pass
    #print(frame)
    print(a)

    a.std()
    stdpnl = str(float(a.std()['pnl']))
    stdsharp = str(float(a.std()['sharp']))
    times = list(a.index)
    pnls = list(a['pnl'])
    sharps = list(a['sharp'])
    for time_, pnl_, sharp_ in zip(times, pnls, sharps):
        time = str(time_)
        pnl= str(pnl_)
        sharp = str(sharp_)
        print(time, pnl, sharp)
        try:
            SQL = """insert into sharpsuanfa1(title,time, pnl, sharp,stdpnl,stdsharp)
                                values
                                (%s,%s,%s,%s,%s,%s)"""
            cur.execute(SQL, (name,time, pnl, sharp,stdpnl,stdsharp))
            print(name,time, pnl, sharp,stdpnl,stdsharp)
        except Exception as e:
            print('***** Logging failed with this error:', str(e))




