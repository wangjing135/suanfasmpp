import pandas as pd
def cal_all(data):
    col_pnl = 2  # 净值所在的列数
    col_time = 0  # 时间所在列数

    def cal_sharp(data):  # data为筛选出来的数据，需要是dataframe形式
        def cal_time(start_time, end_time):  # 计算数据起止日期时间长度
            start_time = [int(item) for item in start_time.split('/')]  # 把'yyyy-m-d’数据转化成['yyyy','m','d']形式的数组
            end_time = [int(item) for item in end_time.split('/')]
            ret = (start_time[0] - end_time[0]) + (start_time[1] - end_time[1]) * 30 / 365.0 + (
                                                                                               start_time[2] - end_time[
                                                                                                   2]) / 365.0  # 计算时间间隔，单位为年
            return ret

        data.iloc[:, col_pnl] = data.iloc[:, col_pnl].apply(lambda x: float(x))
        time_delta = cal_time(data.iloc[-1, col_time], data.iloc[0, col_time])  # 计算数据起止日期时间长度
        returns = (data.iloc[-1, col_pnl] - data.iloc[0, col_pnl]) / (1 + data.iloc[0, col_pnl])  # 计算区间段整体收益
        df = data.iloc[:, col_pnl] + 1  # 净值序列
        chg = df.diff() / df  # 基金涨跌幅度
        variance = (chg ** 2).sum()  # 计算区间段方差
        sharp = returns / (variance * time_delta) ** 0.5  # sharp比率计算
        return [returns / time_delta, sharp]

    result = pd.DataFrame(index=['all', '2015', '2016', '2017'], columns=['pnl', 'sharp'])
    for year in result.index[1:]:
        data_year = data[data.iloc[:, col_time].apply(lambda x: x[:4]) == year]
        result.loc[year, :] = cal_sharp(data_year)
    result.loc['all', :] = cal_sharp(data)
    return result