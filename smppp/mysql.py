import pymysql.cursors
import pymysql
import pandas as pd
class selectmysql():
    def __init__(self):
        self.connection = pymysql.connect(host='58edd9c77adb6.bj.cdb.myqcloud.com',
                                     port=5432,
                                     user='root',
                                     password='1160329981wang',
                                     db='hotshuju',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        self.cur =self. connection.cursor()

    def select_id(self):
        try:
            cur = self.connection.cursor()
            #cur.execute("SELECT smppxiapus2.fund_id,smppxiapus2.fund_short_name FROM smppxiapus2 WHERE smppxiapus2.inception_date <='2014-12-31' ")
            cur.execute("SELECT DISTINCT fund_id FROM smpp_ratess1_1 WHERE categories <='2014-12-31' ")
            #datas = cur.fetchall()
            list_1 = cur.fetchall()
            for names in list_1:
                fund_id = names['fund_id']

            # for i in range(len(datas)):
            #     data = datas[i]
            #     fund_id = data['fund_id']
                print(fund_id)
                #listid.append(fund_id)
                #name = data['fund_short_name']

                return fund_id
        except Exception as e:
            print('***** Logging failed with this error:', str(e))
        return None
    def get_insert_sql(self):
        try:
            cur = self.connection.cursor()
            cur.execute("sELECT DISTINCT title FROM allrate1 ")
            list_ = cur.fetchall()
            for names in list_:
                title = names['title']

                return title
        except Exception as e:
            print('***** Logging failed with this error:', str(e))
        return None
    def select_id(self,fund_id):
        cur = self.connection.cursor()
        cur.execute("sELECT * FROM smpp_ratess1 WHERE categories >='2014-12-31' AND fund_id  ='%s'" %(fund_id))
        datae = cur.fetchall()
        frame = pd.DataFrame(list(datae))
        return frame
    def select_id_2017(self,fund_id):
        cur = self.connection.cursor()
        cur.execute("sELECT * FROM smpp_ratess1 WHERE fund_id = %s",fund_id)
        datae = cur.fetchall()
        frame = pd.DataFrame(list(datae))
        return frame

    def cal_all(self,datas):
        name_pnl = 'rate'  # 净值所在列名
        name_time = 'categories'  # 时间所在列名

        def cal_sharp(data, if_sdd=True):  # data为筛选出来的数据，需要是dataframe形式
            def cal_time(start_time, end_time):  # 计算数据起止日期时间长度
                start_time = [int(item) for item in start_time.split('-')]  # 把'yyyy-m-d’数据转化成['yyyy','m','d']形式的数组
                end_time = [int(item) for item in end_time.split('-')]
                ret = (start_time[0] - end_time[0]) + (start_time[1] - end_time[1]) * 30 / 365.0 + (
                                                                                                       start_time[2] -
                                                                                                       end_time[
                                                                                                           2]) / 365.0  # 计算时间间隔，单位为年
                return ret

            data.loc[:, name_pnl] = data.loc[:, name_pnl].apply(lambda x: float(x))
            time_delta = cal_time(data[name_time].iloc[-1], data[name_time].iloc[0])  # 计算数据起止日期时间长度
            returns = (data[name_pnl].iloc[-1] - data[name_pnl].iloc[0]) / (1 + data[name_pnl].iloc[0])  # 计算区间段整体收益
            df = data.loc[:, name_pnl] + 1  # 净值序列
            chg = df.diff() / df  # 基金涨跌幅度
            variance = (chg ** 2).sum()  # 计算区间段方差
            sharp = returns / (variance * time_delta) ** 0.5  # sharp比率计算
            if if_sdd:
                returns = (1 + returns) ** (1 / time_delta) - 1
                sharp = returns / (variance / time_delta) ** 0.5
            else:
                sharp = returns / (variance * time_delta) ** 0.5  # sharp比率计算
            return [returns, sharp]


        result = pd.DataFrame(index=['all', '2015', '2016', '2017'], columns=['pnl', 'sharp'])
        for year in result.index[1:]:
            data_year = datas[datas.loc[:, name_time] >= str(int(year) - 1) + '-12-01']
            data_year = data_year[data_year.loc[:, name_time] <= year + '-12-31']
            data_year = data_year.sort_values(by=name_time)
            if year != '2017':
                result.loc[year, :] = cal_sharp(data_year)
            else:
                result.loc[year, :] = cal_sharp(data_year, False)
        result.loc['all', :] = cal_sharp(datas[datas.loc[:, name_time] >= '2014-12-01'])
        return result
    def cal_all_2017(self,datas):
        col_pnl = 2  # 净值所在的列数
        col_time = 0  # 时间所在列数
        data = datas[datas.iloc[:, col_time].apply(lambda x: x[:4]) == '2017']
        data.iloc[:, col_pnl] = data.iloc[:, col_pnl].apply(lambda x: float(x))
        returns = (data.iloc[-1, col_pnl] - data.iloc[0, col_pnl]) / (1 + data.iloc[0, col_pnl])  # 计算区间段整体收益
        return returns
    def insert_base(self, fund_id, allrate, allxiapu, time1, time2, time4, time5, time6, time7):

        try:
            with  self.connection.cursor() as cursor:

                # 执行sql语句，插入记录
                SQL = "INSERT INTO smpptime_1ss(fund_id,allrate,allxiapu,2015rate,2015xiapu,2016rate,2016xiapu,2017rate,2017xiapu) VALUES ( %s, %s,%s,%s, %s,%s, %s,%s,%s)"
                cursor.execute(SQL, (
                    fund_id, allrate, allxiapu, time1, time2, time4, time5, time6, time7))
                # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
                self.connection.commit()
        except Exception as e:
            print('***** Logging failed with this error:', str(e))
    def select_time(self):
        cur = self.connection.cursor()
        #cur.execute("SELECT smpptime_1.title,smpptime_1.allrate,smpptime_1.allxiapu FROM smpptime_1" )
        #cur.execute("SELECT fund_id,allrate,allxiapu,2015rate,2016rate,2017rate FROM smpptime_1ss")
        cur.execute("SELECT * FROM smpptime_1ss")
        #cur.execute("SELECT * FROM smpptime_1ss WHERE fund_id = 'HF000002OO'")
        datae = cur.fetchall()
        frame = pd.DataFrame(list(datae))
        return frame
    def insert_paiming(self,fund_id,rate_2015,rate_2016,rate_2017,allrate, allxiapu,pzrate, pzxiapu,jieguo,paiming):

        try:
            with  self.connection.cursor() as cursor:

                # 执行sql语句，插入记录
                SQL = "INSERT INTO paiming_s_s(fund_id,rate_2015,rate_2016,rate_2017,allrate,allxiapu,pzrate,pzxiapu,jieguo,paiming) VALUES ( %s, %s,%s,%s, %s,%s, %s,%s,%s,%s)"
                cursor.execute(SQL, (
                    fund_id,rate_2015,rate_2016,rate_2017,allrate, allxiapu,pzrate, pzxiapu,jieguo,paiming))
                # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
                self.connection.commit()
        except Exception as e:
            print('***** Logging failed with this error:', str(e))
    def insert_paiming_xinsuanfa(self,fund_id,rate_2015,xiapu_2015,rate_2016,xiapu_2016,rate_2017,xiapu_2017,allrate, allxiapu,pzrate, pzxiapu,jieguo,paiming):

        try:
            with  self.connection.cursor() as cursor:

                # 执行sql语句，插入记录
                SQL = "INSERT INTO paiming_xinsuanfa(fund_id,rate_2015,xiapu_2015,rate_2016,xiapu_2016,rate_2017,xiapu_2017,allrate, allxiapu,pzrate, pzxiapu,jieguo,paiming) VALUES ( %s, %s,%s,%s, %s,%s, %s,%s,%s,%s,%s,%s, %s)"
                cursor.execute(SQL, (
                    fund_id, rate_2015, xiapu_2015, rate_2016, xiapu_2016, rate_2017, xiapu_2017, allrate, allxiapu,
                    pzrate, pzxiapu, jieguo, paiming))
                # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
                self.connection.commit()
        except Exception as e:
            print('***** Logging failed with this error:', str(e))