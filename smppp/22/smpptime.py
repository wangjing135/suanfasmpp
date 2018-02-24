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
class suanfaxiapu:
    def __init__(self):
        self.dao = selectmysql()

    def frames_time(self):
        #pzframe= self.dao.select_frame()
        frames_time = self.dao.select_time()
        #print(frames_time)
        meanrate = frames_time['allrate'].mean()
        print(meanrate)
        stdrate = frames_time['allrate'].std()
        meanxiapu = frames_time['allxiapu'].mean()
        stdxiapu = frames_time['allxiapu'].std()
        meanrate_2015= frames_time['2015rate'].mean()
        stdrate_2015=frames_time['2015rate'].std()
        meanxiapu_2015=frames_time['2015xiapu'].mean()
        stdxiapu_2015=frames_time['2015xiapu'].std()
        meanrate_2016 = frames_time['2016rate'].mean()
        stdrate_2016 = frames_time['2016rate'].std()
        meanxiapu_2016 = frames_time['2016xiapu'].mean()
        stdxiapu_2016 = frames_time['2016xiapu'].std()
        meanrate_2017 = frames_time['2017rate'].mean()
        stdrate_2017 = frames_time['2017rate'].std()
        meanxiapu_2017 = frames_time['2017xiapu'].mean()
        stdxiapu_2017 = frames_time['2017xiapu'].std()
        frames_time['pzrate_all'] = (frames_time['allrate'] - meanrate) / stdrate
        print(frames_time['pzrate_all'])
        frames_time['pzxiapu_all'] = (frames_time['allxiapu'] - meanxiapu) / stdxiapu
        frames_time['pzrate_2015'] = (frames_time['2015rate'] - meanrate_2015) / stdrate_2015
        frames_time['pzxiapu_2015'] = (frames_time['2015xiapu'] -meanxiapu_2015) / meanxiapu_2015
        frames_time['pzrate_2016'] = (frames_time['2016rate'] - meanrate_2016) / stdrate_2016
        frames_time['pzxiapu_2016'] = (frames_time['2016xiapu'] - meanxiapu_2016) / meanxiapu_2016
        frames_time['pzrate_2017'] = (frames_time['2017rate'] - meanrate_2017) / stdrate_2017
        frames_time['pzxiapu_2017'] = (frames_time['2017xiapu'] - meanxiapu_2017) / meanxiapu_2017
        frames_time['jieguo']=0.25*(frames_time['pzrate_2017']+frames_time['pzxiapu_2017'])+0.15*(frames_time['pzrate_2016']+frames_time['pzxiapu_2016'])+0.1*(frames_time['pzrate_2015']+frames_time['pzxiapu_2015'])
        #frames_time['jieguo'] = 0.4*(frames_time['pzrate']) +0.6* (frames_time['pzxiapu'])
        #print(frames_time)
        #print(frames_time['pzrate_2015'],frames_time['pzxiapu_2015'],frames_time['pzrate_2016'],frames_time['pzxiapu_2016'],frames_time['pzrate_2017'],frames_time['pzxiapu_2017'])
        frame1 = frames_time.sort_values(by='jieguo', ascending=False)
        lens = len(frame1.index)
        #print(lens)
        listlen = []
        for i in range(lens):
            listlen.append(i)
        frame1['fenshu'] = listlen
        frame1['paiming'] = (lens - frame1['fenshu']) / lens * 100
        fund_ids= frame1['fund_id']
        rates_2015 = frame1['2015rate']
        xiapus_2015=frame1['2015xiapu']
        rates_2016 = frame1['2016rate']
        xiapus_2016=frame1['2016xiapu']
        rates_2017= frame1['2017rate']
        xiapus_2017=frame1['2017xiapu']
        allrates= frame1['allrate']
        allxiapus= frame1['allxiapu']
        pzrates = frame1['pzrate_all']
        pzxiapus= frame1['pzxiapu_all']
        jieguos= frame1['jieguo']
        paimings = frame1['paiming']
        for fund_id,rate_2015_,xiapu_2015_,rate_2016_,xiapu_2016_,rate_2017_,xiapu_2017_,allrate_,allxiapu_,pzrate_,pzxiapu_,jieguo_,paiming_ in zip(fund_ids,rates_2015,xiapus_2015,rates_2016,xiapus_2016,rates_2017,xiapus_2017,allrates,allxiapus,pzrates,pzxiapus,jieguos,paimings):

            rate_2015= float(rate_2015_)
            xiapu_2015 = float(xiapu_2015_)
            rate_2016=float(rate_2016_)
            xiapu_2016= float(xiapu_2016_)
            rate_2017=float(rate_2017_)
            xiapu_2017= float(xiapu_2017_)
            allrate= float(allrate_)
            allxiapu= float(allxiapu_)
            pzrate= float(pzrate_)
            pzxiapu= float(pzxiapu_)
            jieguo= float(jieguo_)
            paiming = float(paiming_)
            #print(fund_id,rate_2015_,xiapu_2015,rate_2016_,xiapu_2016,rate_2017_,xiapu_2017,allrate_,allxiapu_,pzrate_,pzxiapu_,jieguo_,paiming_)
            #self.dao.insert_paiming(fund_id,rate_2015,rate_2016,rate_2017,allrate,allxiapu,pzrate,pzxiapu,jieguo,paiming)
            #self.dao.insert_paiming_xinsuanfa(fund_id,rate_2015,xiapu_2015,rate_2016,xiapu_2016,rate_2017,xiapu_2017,allrate,allxiapu,pzrate,pzxiapu,jieguo,paiming)
if __name__ == '__main__':
    a_ = suanfaxiapu()
    b_ = a_.frames_time()