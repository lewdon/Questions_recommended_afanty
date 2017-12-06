# -*-coding:utf8-*-
import os
import sys
import csv
import json
import logging
import pandas as pd
import numpy as np
from multiprocessing import Pool
from lib.util import getProvinceSet,mkdir
from simhash import Simhash
from collections import Counter
import math
from operator import itemgetter

datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/anoah_Analy_itemCF_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
ANALY = _DIR + '/new_database/Analy/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
ITEMCF_PATH = ANALY + 'ItemCF_output/'
datetime = '09-11'
prov_subj_file = 'prov_subject_{}_{}_{}.txt'
itemcf_output_file = 'item_colf_{}_{}_{}.txt'


#对item中每个元素进行统计频率，并返回dict
def itemWeight(user_item):
    user_item_weight_dict ={}

    for keys,values in user_item.items():
        values_counts = Counter(values)
        values_len = len(values)
        user_item_weight_dict[keys] = [[k, v/values_len] for k,v in values_counts.items()]

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

            UserSimi2arr[i][j] = 1000 * cij / (
                math.sqrt(item_count[i] * item_count[j]) * (Simhash(i).distance(Simhash(j)) ** 2) )

    return UserSimi2arr


def packageItemCFRun(prov, subj, datetime, ICF_PATH):
    ''' 普通过程的Apriori打包程序
                                    @param prov         省份
                                    @param subj         年级学科
                                    @param ICF_PATH      ItemCF(基于项目的协同过滤)算法输出路径
                                    @param datetime     日期区间
                                '''
    exist_file = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, subj, datetime)
    if os.path.exists(exist_file):
        logging.info(u"正在读取{0}省份下{1}学科的Prov_Sub_input文件的数据！".format(prov, subj))
        user_item_dic = {}
        with open(exist_file, 'r') as ps_file:
            while True:
                data_json = ps_file.readline()
                if data_json:
                    if eval(data_json.replace('\n', ''))[0] not in user_item_dic.keys():
                        user_item_dic[eval(data_json.replace('\n', ''))[0]] = eval(
                            data_json.replace('\n', ''))[1:]

                    else:
                        user_item_dic[eval(data_json.replace('\n', ''))[0]].extend(
                            eval(data_json.replace('\n', ''))[1:])
                        user_item_dic[eval(data_json.replace('\n', ''))[0]] = user_item_dic[
                            eval(data_json.replace('\n', ''))[0]]
                    del data_json

                else:
                    break

        user_item_weight_dict = itemWeight(user_item_dic)
        del user_item_dic
        UserSimi2arr = itemSimilarity(user_item_weight_dict)
        del user_item_weight_dict

        output_file = ICF_PATH + '/' + itemcf_output_file.format(prov, subj, datetime)
        with open(output_file, 'wt') as new_file:
            for k,v in UserSimi2arr.items():
                new_file.writelines(json.dumps({k:v}))
                new_file.write('\n')

            new_file.close()

        logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件里！".format(prov, subj))


if __name__ == '__main__':
    # prov_set = getProvinceSet()
    #'甘肃','宁夏','四川','全国','重庆','陕西','吉林','北京','海南','江苏','山东',
    prov_set = {'湖北', '辽宁'}
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    # subj_set = {'7'}

    pool = Pool(3)
    for prov in prov_set:
        ICF_PATH = ITEMCF_PATH + datetime + '/' + prov
        mkdir(ICF_PATH)

        for subj in subj_set:
            pool.apply_async(packageItemCFRun,kwds={
                'prov':prov,
                'subj':subj,
                'datetime':datetime,
                'ICF_PATH':ICF_PATH
            })
    pool.close()
    pool.join()

    # for prov in prov_set:
    #     ICF_PATH = ITEMCF_PATH + datetime + '/' + prov
    #     mkdir(ICF_PATH)
    #
    #     for subj in subj_set:
    #                 packageItemCFRun(
    #                     prov=prov,
    #                     subj=subj,
    #                     datetime=datetime,
    #                     ICF_PATH=ICF_PATH
    #                 )
