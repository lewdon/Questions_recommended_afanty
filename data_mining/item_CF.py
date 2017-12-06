# -*-coding:utf8-*-
import os
import sys
import pandas as pd
import numpy as np
import random
from operator import itemgetter
from texttable import Texttable
import math
from collections import Counter
from lib.kmodes_init import set_nclusters
from machine_learning.kmodes import KModes

FILE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
DATABASE_PATH = FILE_PATH + '/database/'
KMODES_FILENAME = 'kmodes_cao.csv'
APRIORI_OUTPUTFILE = 'demo1_apriori_outputfile_{}.csv'


def userItemTestTrain(ceshi_data, dividdataM, testdatanum, seed):
    ''' 将数据分为训练集和测试集
            @param data              储存训练和测试数据的List
            @param dividdataM        将数据分为M份
            @param testdatanum       选取第key份数据做为测试数据
            @param seed              随机种子
            @return train            训练数据集Dict
            @return test             测试数据集Dict
        '''
    test_user_item = dict()
    train_user_item = dict()
    random.seed(seed)
    for i in range(len(ceshi_data)):
        if random.randint(0, dividdataM) == testdatanum:
            if ceshi_data.iloc[i][0] in test_user_item:
                for j in range(len(ceshi_data.iloc[i, 1:])):
                    if ceshi_data.iloc[i][j+1] is not np.nan:
                        test_user_item[ceshi_data.iloc[i][0]].append(ceshi_data.iloc[i][j+1])
            else:
                test_user_item[ceshi_data.iloc[i][0]] = []
                for j in range(len(ceshi_data.iloc[i,1:])):
                    if ceshi_data.iloc[i][j + 1] is not np.nan:
                        test_user_item[ceshi_data.iloc[i][0]].append(ceshi_data.iloc[i][j+1])

        else:
            if ceshi_data.iloc[i][0] in train_user_item:
                for k in range(len(ceshi_data.iloc[i, 1:])):
                    if ceshi_data.iloc[i][k + 1] is not np.nan:
                        train_user_item[ceshi_data.iloc[i][0]].append(ceshi_data.iloc[i][k+1])
            else:
                train_user_item[ceshi_data.iloc[i][0]] = []

                for k in range(len(ceshi_data.iloc[i, 1:])):
                    if ceshi_data.iloc[i][k + 1] is not np.nan:
                        train_user_item[ceshi_data.iloc[i][0]].append(ceshi_data.iloc[i][k+1])

    return test_user_item, train_user_item


#对item中每个元素进行统计频率，并返回dict
def itemWeight(user_item):
    user_item_weight_dict ={}

    for keys,values in user_item.items():
        values_counts = Counter(values)
        values_len = len(values)
        values_list = []
        for k,v in values_counts.items():
            values_list.append([k, v / values_len])
        user_item_weight_dict[keys] = values_list
        del values_list

    return user_item_weight_dict


def itemSimilarity(train_dict):
    ''' 计算物品相似度
        @param train_dict            训练数据集Dict
        @return UserSimilar2array    记录用户相似度的二维矩阵
    '''
    item_item_count = dict()
    item_count = dict()

    # 计算每两个item共有的user数目
    for user, items_weight in train_dict.items():
        for id1,count1 in items_weight:
            if id1 not in item_count:
                item_count[id1] = count1
            item_count[id1] += count1
            for id2,count2 in items_weight:
                if id1 == id2:
                    continue
                if id1 not in item_item_count:
                    item_item_count[id1] = dict()
                if id2 not in item_item_count[id1]:
                    item_item_count[id1][id2] = count1 + count2
                item_item_count[id1][id2] += count1 +count2

    UserSimi2arr = dict()
    for i, related_items in item_item_count.items():
        for j, cij in related_items.items():
            if i not in UserSimi2arr:
                UserSimi2arr[i] = dict()
            UserSimi2arr[i][j] = cij / math.sqrt(item_count[i] * item_count[j])

    return UserSimi2arr


def GetRecommendation(user, train_user_item_weight ,UserSimi2arr, SelNeighNum):
    ''' 获取推荐结果
            @param user                   输入的用户
            @param train_user_item_weight 训练数据集Dict
            @param UserSimi2arr           记录用户相似度的二维矩阵
            @param SelNeighNum            选取近邻的数目
        '''
    rank = dict()
    item_dict = {}
    for item_weight in train_user_item_weight[user]:
        item_dict[item_weight[0]] = item_weight[1]

    for i in item_dict.keys():
        # print(sorted(UserSimi2arr[i].items(), key=itemgetter(1),reverse = True)[0:SelNeighNum])
        for j,wj in sorted(UserSimi2arr[i].items(), key=itemgetter(1),\
            reverse = True)[0:SelNeighNum]:
            if j in item_dict.keys():
                continue
            if j in rank:
                rank[j] += wj
            else:
                rank[j] = wj

    # rank = sorted(rank.items(), key=itemgetter(1), reverse = True)[0:RecomResNum]
    # print(rank)
    return rank


def Recall(train_user_item_weight, test_user_item_weight, UserSimi2arr, RecomResNum, SelNeighNum):
    ''' 计算推荐结果的召回率
        @param train_user_item_weight 训练数据集Dict
        @param test_user_item_weight  测试数据集Dict
        @param UserSimi2arr           记录用户相似度的二维矩阵
        @param RecomResNum            推荐结果的数目
        @param SelNeighNum            选取近邻的数目
    '''
    hit = 0
    all = 0
    for user in train_user_item_weight.keys():
        if user in test_user_item_weight.keys():
            item_list = []
            # print(test_user_item_weight[user])
            for item_weight in test_user_item_weight[user]:
                item_list.append(item_weight[0])

            rank = GetRecommendation(
                user=user,
                train_user_item_weight=train_user_item_weight,
                UserSimi2arr=UserSimi2arr,
                SelNeighNum=SelNeighNum
            )
            rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:RecomResNum]


            for item, pui in rank:
                if item in item_list:
                    hit += 1
            all += len(item_list)
            del item_list

    return hit / ((all + 1) * 1.0)


def Precision(train_user_item_weight, test_user_item_weight, UserSimi2arr, RecomResNum, SelNeighNum):
    ''' 计算推荐结果的准确率
        @param train_user_item_weight 训练数据集Dict
        @param test_user_item_weight  测试数据集Dict
        @param UserSimi2arr           记录用户相似度的二维矩阵
        @param RecomResNum            推荐结果的数目
        @param SelNeighNum            选取近邻的数目
    '''
    hit = 0
    all = 0
    for user in train_user_item_weight.keys():
        if user in test_user_item_weight:
            item_list = []
            for item_weight in test_user_item_weight[user]:
                item_list.append(item_weight[0])

            rank = GetRecommendation(
                user=user,
                train_user_item_weight=train_user_item_weight,
                UserSimi2arr=UserSimi2arr,
                SelNeighNum=SelNeighNum
            )
            rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:RecomResNum]

            for item, pui in rank:
                if item in item_list:
                    hit += 1
            all += RecomResNum

    return hit / (all * 1.0)


def Popularity(train_user_item_weight, test_user_item_weight, UserSimi2arr, RecomResNum, SelNeighNum):
    ''' 计算推荐结果的流行度
        @param train_user_item_weight 训练数据集Dict
        @param test_user_item_weight  测试数据集Dict
        @param UserSimi2arr           记录用户相似度的二维矩阵
        @param RecomResNum            推荐结果的数目
        @param SelNeighNum            选取近邻的数目
    '''
    item_popularity = dict()
    for user, item_weight in train_user_item_weight.items():
        item_list = []
        for t in range(len(item_weight)):
            item_list.append(item_weight[t][0])
        for item in item_list:
            if item not in item_popularity:
                item_popularity[item] = 0
            item_popularity[item] += 1

    ret = 0
    n = 0

    for user in train_user_item_weight.keys():
        rank = GetRecommendation(
            user=user,
            train_user_item_weight=train_user_item_weight,
            UserSimi2arr=UserSimi2arr,
            SelNeighNum=SelNeighNum
        )
        rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:RecomResNum]

        for item, pui in rank:
            ret += math.log(1+ item_popularity[item])
            n += 1

    ret /= n * 1.0
    return ret


def Coverage(train_user_item_weight, test_user_item_weight, UserSimi2arr, RecomResNum, SelNeighNum):
    ''' 获取推荐结果
        @param train_user_item_weight 训练数据集Dict
        @param test_user_item_weight  测试数据集Dict
        @param UserSimi2arr           记录用户相似度的二维矩阵
        @param RecomResNum            推荐结果的数目
        @param SelNeighNum            选取近邻的数目
    '''
    recommned_items = set()
    all_items = set()

    for user in train_user_item_weight.keys():
        for item_weight in train_user_item_weight[user]:
            all_items.add(item_weight[0])

        rank = GetRecommendation(
            user=user,
            train_user_item_weight=train_user_item_weight,
            UserSimi2arr=UserSimi2arr,
            SelNeighNum=SelNeighNum
        )
        rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:RecomResNum]

        for item, pui in rank:
            recommned_items.add(item)

    print('this is from Coverage ! \nThe length of recommend_item : ',len(recommned_items),'\n')
    return len(recommned_items) / (len(all_items) * 1.0)




