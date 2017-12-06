# -*-coding:utf8-*-
import os
import sys
import pandas as pd
import math
from multiprocessing import Pool, Process


#专门针对Apriori矩阵转换
def aprioriDataTrans(data):
    print(u'\n转换原始数据至0-1矩阵。。。')
    ct = lambda x: pd.Series(1, index=x[pd.notnull(x)])
    b = map(ct, data.as_matrix())
    data = pd.DataFrame(list(b)).fillna(0)
    del b
    return data


# 自定义连接函数，用于实现L_{k-1}到C_k的连接
def connect_string(x, ms):
    x = list(map(lambda i: sorted(i.split(ms)), x))
    try:
        l = len(x[0])
    except:
        raise IndexError("参数support数值太小，应该增大support数值！")
    r = []
    for i in range(len(x)):
        for j in range(i, len(x)):
            if x[i][:l - 1] == x[j][:l - 1] and x[i][l - 1] != x[j][l - 1]:
                r.append(x[i][:l - 1] + sorted([x[j][l - 1], x[i][l - 1]]))
    #print('the list of r is:' + '\n' + '{}'.format(r) + '\n')
    return r


# 寻找关联规则的函数
def find_rule(d, support, confidence, ms=u'--'):
    result = pd.DataFrame(index=['support', 'confidence', 'weights'], dtype=str)  # 定义输出结果

    support_series = 1.0 * d.sum() / len(d)  # 支持度序列
    #print('the support_series is {}'.format(support_series) + '\n')
    column = list(support_series[support_series > support].index)  # 初步根据支持度筛选
    #print('the list of column is {}'.format(column))
    k = 0

    #while len(column) > 1 :
    while k < 3 and len(column) > 1:
        k = k + 1
        print(u'\n正在进行第%s次搜索...' % k)
        column = connect_string(column, ms)
        #print(u'数目：%s...' % len(column))
        sf = lambda i: d[i].prod(axis=1, numeric_only=True)  # 新一批支持度的计算函数
        # pool = Pool(processes=4)
        # 创建连接数据，这一步耗时、耗内存最严重。当数据集较大时，可以考虑并行运算优化。
        d_2 = pd.DataFrame(list(map(sf, column)), index=[ms.join(i) for i in column]).T

        # print('the new data of d_2 is {}'.format(d_2))
        support_series_2 = 1.0 * d_2[[ms.join(i) for i in column]].sum() / len(d)  # 计算连接后的支持度
        column = list(support_series_2[support_series_2 > support].index)  # 新一轮支持度筛选
        support_series = support_series.append(support_series_2)
        #print('the append list of support_series is :' + '\n' '{}'.format(support_series))
        column2 = []

        for i in column:  # 遍历可能的推理，如{A,B,C}究竟是A+B-->C还是B+C-->A还是C+A-->B？
            i = i.split(ms)
            for j in range(len(i)):
                column2.append(i[:j] + i[j + 1:] + i[j:j + 1])
        #print('the new list of column2 is :' + '\n' '{}'.format(column2))
        cofidence_series = pd.Series(index=[ms.join(i) for i in column2])  # 定义置信度序列
        # print('the new data of cofidence_series is {}'.format(cofidence_series))
        for i in column2:  # 计算置信度序列
            cofidence_series[ms.join(i)] = support_series[ms.join(sorted(i))] / support_series[ms.join(i[:len(i) - 1])]

        for i in cofidence_series[cofidence_series > confidence].index:  # 置信度筛选
            result[i] = 0.0
            result[i]['confidence'] = cofidence_series[i]
            result[i]['support'] = support_series[ms.join(sorted(i.split(ms)))]
            result[i]['weights'] = cofidence_series[i] * 0.8 + support_series[ms.join(sorted(i.split(ms)))] * 0.2
            #print(result[i]['weights'])

    result = result.T.sort_values(['weights', 'confidence', 'support'], ascending=False)  # 结果整理，输出

    return result