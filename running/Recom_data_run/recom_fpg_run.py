# -*-coding:utf8-*-
import os
import sys
import numpy as np
import time
import logging
from lib.table_data import tableToJson, getQuestionId
from lib.util import getProvinceSet,mkdir
import pandas as pd
import csv
import json

datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/Recom_fpg_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
RECOMMEND = _DIR + '/new_database/Recommend/'
ANALY = _DIR + '/new_database/Analy/'
FPGTH_PATH = ANALY + 'FPGrowth_output/'
USER_PATH = RECOMMEND + 'user/'
RECOM_FPGTH_PATH = RECOMMEND + 'FPGrowth/'

fpgth_output_file = 'fp_growth_{}_{}_{}.txt'


#获取推荐question_id
def getRecomFPGth(fpg_list, subkp_set):
    subkp_str = ''
    flag = 0
    for fpg in fpg_list:
        if fpg in subkp_set:
            flag += 1
            subkp_str += fpg
        else:
            flag += 0

    if flag == 1:
        return subkp_str


def packageRcomFPGth(requir_user_file, output_file, diff):
    ''' 对推荐试题结果程序进行封装打包
                                @param requir_user_file     原始数据文件(user_id, province， question)
                                @param diff                 难度系数，int
                                @param output_file          输出文件名
                            '''
    prov_set = getProvinceSet()
    diff = diff or 1

    filename = RECOM_FPGTH_PATH + output_file.format(datetime, time.strftime("%Y%m%d"))
    if os.path.exists(filename):
        os.remove(filename)

    with open(USER_PATH + requir_user_file, 'r') as user_file:
        readers = csv.DictReader(user_file)
        for reader in readers:
            prov = reader['province'][:2]
            if prov not in prov_set:
                prov = '全国'

            user_id = reader['user_id']
            question_id = eval(reader['question'])

            question_list = tableToJson(
                table='question_simhash_20171111',
                question_id=question_id
            )

            if len(question_list) > 0:
                subj_dic = {}
                recom_set = set([])

                for sub_kpoint in question_list:
                    if str(sub_kpoint[1]) not in subj_dic.keys():
                        subj_dic[str(sub_kpoint[1])] = [sub_kpoint[0]]
                    else:
                        subj_dic[str(sub_kpoint[1])].append(sub_kpoint[0])

                for ks, vs in subj_dic.items():
                    fpg_file = FPGTH_PATH + datetime + '/' + prov + '/' + fpgth_output_file.format(prov, ks,
                                                                                                        datetime)
                    if os.path.exists(fpg_file):
                        logging.info(u"正在读取{0}省份下{1}学科的Analy下的FPGrowth数据！".format(prov, ks))
                        with open(fpg_file, 'r') as recom_file:
                            while True:
                                result = recom_file.readline()
                                if result:
                                    recom_set.add(getRecomFPGth(result.replace('\n','').split('--'), set(vs)))

                                else:
                                    break

                        recom_list = getQuestionId(
                            table='question_simhash_20171111',
                            question_id=question_id,
                            recom_set=recom_set,
                            diff=diff
                        )

                        with open(filename, 'a') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow([user_id, ks, recom_list])

                        logging.info(u"已经解析完用户{}！".format(user_id))

                    else:
                        print(u"没有查询到生成相关的FPGrowth输出表！")

            else:
                print(u"用户{}传入的question_id在表**question_simhash_20171111**里查询不到！".format(user_id))

        user_file.close()


if __name__ == '__main__':
    # prov_set = getProvinceSet()
    # subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)}
    # prov_set = {'福建'}
    # subj_set = {'2'}
    requir_user_file = 'requir_user.csv'
    output_file = 'fp_output_{}_{}.csv'

    packageRcomFPGth(
        requir_user_file=requir_user_file,
        output_file=output_file,
        diff=1
    )

