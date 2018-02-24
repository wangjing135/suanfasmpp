import time
import pandas as pd
import json
import time
import requests
import pymysql.cursors
import pymysql
import csv
from mysql import selectmysql
print('11111')
class suanfaxiapu():
    def __init__(self):
        self.dao = selectmysql()
    def xiapu(self):
        with open("smpp8.csv", "w+", newline="\n", encoding="utf-8") as datacsv:
            csvwriter = csv.writer(datacsv, dialect=("excel"))
            fp = open(r"C:\Users\wangjing\Desktop\namess.txt")
            alldata = fp.read().split("\n")
            for i in range(len(alldata)):
                data = alldata[i].split("\t")
                name = data[0]
                print(name)
                frames = self.dao.select(name)
                a = self.cal_all(frames)
                print(a)
                return a



