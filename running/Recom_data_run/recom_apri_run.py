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
                    filename='working/Recom_apri_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
RECOMMEND = _DIR + '/new_database/Recommend/'
ANALY = _DIR + '/new_database/Analy/'
APRIORI_PATH = ANALY + 'Apriori_output/'
USER_PATH = RECOMMEND + 'user/'
RECOM_APRI_PATH = RECOMMEND + 'Apriori/'

apriori_output_file = 'apri_supconfweig_{}_{}_{}.csv'

#获取推荐question_id
def getRecomApri(apri_list, subkp_set):
    for A_B in apri_list:
        if A_B[-1] not in subkp_set:
            flag = 0
            for a in A_B[:-1]:
                if a in subkp_set:
                    flag += 0
                else:
                    flag = 1

            if flag != 0:
                return A_B[-1]


def packageRcomApri(requir_user_file, diff, output_file):
    ''' 对推荐试题结果程序进行封装打包
                            @param requir_user_file     原始数据文件(user_id, province， question)
                            @param diff                 难度系数，int
                            @param output_file          输出文件名
                        '''
    prov_set = getProvinceSet()
    diff = diff or 1
    # print(os.path.getsize())
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
                    apri_file = APRIORI_PATH + datetime + '/' + prov + '/' + apriori_output_file.format(prov, ks,
                                                                                                        datetime)
                    if os.path.exists(apri_file):
                        logging.info(u"正在读取{0}省份下{1}学科的Analy下的Apriori数据！".format(prov, ks))

                        with open(apri_file, 'r') as recom_file:
                            results = csv.DictReader(recom_file)
                            for result in results:
                                recom_set.add(getRecomApri(result[''].split('--'), set(vs)))
                            recom_file.close()
                    else:
                        print(u"没有查询到生成相关的Apriori输出表！")

                del subj_dic
                recom_list = getQuestionId(
                    table='question_simhash_20171111',
                    question_id=question_id,
                    recom_set=recom_set,
                    diff=diff
                )

                with open(RECOM_APRI_PATH + output_file.format(datetime, time.strftime("%Y%m%d")), 'a') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([user_id, recom_list])

                logging.info(u"已经解析完{0}省份下{1}学科的数据，并存入到Prov_Sub_input文件！".format(prov))

            else:
                print(u"用户{}传入的question_id在表**question_simhash_20171111**里查询不到！".format(user_id))

        user_file.close()



if __name__ == '__main__':
    requir_user_file = 'requir_user.csv'
    output_file = 'apri_output_{}_{}.csv'

    packageRcomApri(
        requir_user_file=requir_user_file,
        output_file=output_file,
        diff=1
    )