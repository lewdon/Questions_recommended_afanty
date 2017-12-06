# -*-coding:utf8-*-
import os
import sys
import csv
import logging
import pandas as pd
import numpy as np
import json
from multiprocessing import Pool
from data_mining.FPGrowth import fpGrowth
from lib.util import getProvinceSet,mkdir

datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/anoah_Analy_fpgrowth_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
ANALY = _DIR + '/new_database/Analy/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
FPGROWTH_PATH = ANALY + 'FPGrowth_output/'

prov_subj_file = 'prov_subject_{}_{}_{}.txt'
fpgth_output_file = 'fp_growth_{}_{}_{}.txt'


#修饰数据
def dealJsonData(data_json):
    for i in range(len(data_json)):
        data_json[i] = list(set(eval(data_json[i].replace('\n',''))[1:]))

    return data_json


def getFreqItems(dataSet, k):
    door_len = 0
    #设置minSup，当大于10000时，赋值为90,；否则为（len()*0.007）
    if len(str(len(dataSet))) > 4:
        minSup = 100

        while door_len < 2500 and minSup > 4:
            freqItems = []
            freqlist = fpGrowth(dataSet, minSup=minSup)

            for i in freqlist:
                # 当组合元素大于等于2个时，当内存够大时，可以提高此值
                if len(i) >= k:
                    freqItems.append(i)
            del freqlist
            door_len = len(freqItems)

            minSup = int(minSup * 0.95)
    else:
        minSup = max(int(len(dataSet) * 0.007), 12)

        while door_len < 800 and minSup > 2:
            freqItems = []
            freqlist = fpGrowth(dataSet, minSup=minSup)

            for i in freqlist:
                # 当组合元素大于等于k个时，当内存够大时，可以提高此值
                if len(i) >= k:
                    freqItems.append(i)
            del freqlist
            door_len = len(freqItems)

            minSup = int(minSup * 0.95)

    if len(freqItems) < 50 and k > 2:
        getFreqItems(dataSet=dataSet, k= k-1)
    else:
        return freqItems



def packageFPGrowthRun(prov, subj, datetime, FO_PATH):
    ''' 普通过程的Apriori打包程序
                                @param prov         省份
                                @param subj         年级学科
                                @param FO_PATH      FPGrowth算法输出路径
                                @param datetime     日期区间
                            '''

    output_file = FO_PATH + '/' + fpgth_output_file.format(prov, subj, datetime)

    exist_file = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, subj, datetime)
    if os.path.exists(exist_file):
        logging.info(u"正在读取{0}省份下{1}学科的Prov_Sub_input文件的数据！".format(prov, subj))

        with open(exist_file,'r') as ps_file:
            data_json = ps_file.readlines()
            data_json = dealJsonData(data_json)

            freqItems = getFreqItems(data_json, 3)
            # print(freqItems)
            with open(output_file, 'wt') as csv_file:
                for fi in freqItems:
                    csv_file.writelines('--'.join(i for i in fi))
                    csv_file.write('\n')
                csv_file.close()

            ps_file.close()
        logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到文件！".format(prov, subj))


if __name__ == '__main__':
    prov_set = getProvinceSet()
    subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    # prov_set = {'全国'}
    # subj_set = {'3'}

    pool = Pool(2)
    for prov in prov_set:
        FO_PATH = FPGROWTH_PATH + datetime + '/' + prov
        mkdir(FO_PATH)

        for subj in subj_set:
            pool.apply_async(packageFPGrowthRun, kwds={
                "prov":prov,
                "subj":subj,
                "datetime":datetime,
                "FO_PATH":FO_PATH
            })

    pool.close()
    pool.join()
