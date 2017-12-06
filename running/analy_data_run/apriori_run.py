# -*-coding:utf8-*-
import os
import sys
import csv
import json
import pandas as pd
import numpy as np
import logging
from multiprocessing import Pool
from data_mining.Apriori import find_rule,aprioriDataTrans
from lib.util import getProvinceSet,mkdir
from data_mining.Kmodes_Simhash import K_Simhash

datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/Analy_apriori_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
ANALY = _DIR + '/new_database/Analy/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
APRIORI_PATH = ANALY + 'Apriori_output/'

prov_subj_file = 'prov_subject_{}_{}_{}.txt'
apriori_output_file = 'apri_supconfweig_{}_{}_{}.csv'

#修饰数据
def dealJsonData(data_json):
    for i in range(len(data_json)):
        data_json[i] = list(set(eval(data_json[i].replace('\n',''))[1:]))

    return data_json


#获取数组最大长度
def maxListLen(data_list):
    count = 0

    for i in data_list:
        if len(i) > count:
            count = len(i)

    return count


#list转换成DataFrame格式
def listToDataFrame(data_list, max_len):
    for i in range(len(data_list)):
        i_len = len(data_list[i])
        if i_len < max_len:
            data_list[i].extend([np.nan for _ in range(i_len,max_len)])

    return pd.DataFrame(data_list)


def packageAprioriRun(prov, subj, support, confidence, datetime, AO_PATH):
    ''' 普通过程的Apriori打包程序
                                @param prov         省份
                                @param subj         年级学科
                                @param support      Apriori算法参数，支持度
                                @param confidence   Apriori算法参数，置信度
                                @param AO_PATH      Apriori算法输出路径
                                @param datetime     日期区间
                            '''

    output_file = AO_PATH + '/' + apriori_output_file.format(prov, subj, datetime)
    if os.path.exists(output_file):
        os.remove(output_file)

    prosub_file = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, subj, datetime)
    if os.path.exists(prosub_file):
        logging.info(u"正在读取{0}省份下{1}学科的Prov_Sub_input文件的数据！".format(prov, subj))

        with open(prosub_file, 'r') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)

            max_len = maxListLen(data_json)
            data_frame = listToDataFrame(data_json, max_len)
            del data_json
            data_frame = aprioriDataTrans(data_frame)

            find_rule(data_frame, support, confidence, ms='--').to_csv(output_file)
            del data_frame
            ps_file.close()
        logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件里！".format(prov, subj))


def packageWhileAprioriRun(prov, subj, support, confidence, datetime, AO_PATH):
    ''' 有while判断过程的Apriori打包程序
                            @param prov         省份
                            @param subj         年级学科
                            @param support      Apriori算法参数，支持度
                            @param confidence   Apriori算法参数，置信度
                            @param AO_PATH      Apriori算法输出路径
                            @param datetime     日期区间
                        '''
    filename = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, subj, datetime)
    if os.path.exists(filename):
        with open(filename, 'r') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)

            max_len = maxListLen(data_json)
            data_frame = listToDataFrame(data_json, max_len)
            del data_json
            data_frame = aprioriDataTrans(data_frame)

            flag = 0
            while True:

                print(flag)

                apriori_all = find_rule(data_frame, support, confidence, ms='--')
                if len(apriori_all) > 300:
                    output_file = AO_PATH + '/' + apriori_output_file.format(prov, subj, datetime)
                    if os.path.exists(output_file):
                        os.remove(output_file)

                    apriori_all.to_csv(output_file,mode='wt')
                    del apriori_all
                    break

                del apriori_all
                support = support * 0.9
                confidence = confidence * 0.9
                flag += 1


if __name__ == '__main__':
    #prov_set = getProvinceSet()
    # , '甘肃', '宁夏', '四川', '全国', '重庆', '陕西', '吉林', '北京', '海南'
    prov_set = {'江苏'}
    support = 0.03
    confidence = 0.10
    # subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    subj_set = {'5'}
    for prov in prov_set:
        AO_PATH = APRIORI_PATH + datetime + '/' + prov
        mkdir(AO_PATH)

        for subj in subj_set:
            packageWhileAprioriRun(
                prov=prov,
                subj=subj,
                support=support,
                confidence=confidence,
                datetime=datetime,
                AO_PATH=AO_PATH
            )

    # pool = Pool()
    # for prov in prov_set:
    #     AO_PATH = APRIORI_PATH + datetime + '/' + prov
    #     mkdir(AO_PATH)
    #     for subj in subj_set:
    #         pool.apply_async(packageAprioriRun, kwds={
    #             'prov': prov,
    #             'subj': subj,
    #             'support': support,
    #             'confidence': confidence,
    #             'datetime': datetime,
    #             'AO_PATH': AO_PATH
    #         })
    #
    # pool.close()
    # pool.join()
