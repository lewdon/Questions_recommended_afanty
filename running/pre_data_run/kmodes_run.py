# -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import json
import logging
from lib.util import getProvinceSet,mkdir
import csv
from lib.table_data import tableToJson
from data_mining.Kmodes_Simhash import K_Simhash

datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/Input_kmodes_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
RECOMMEND = _DIR + '/new_database/Recommend/'
PROV_SUB_PATH = DATABASE + 'Prov_Sub_input/'
USER_PATH = RECOMMEND + 'user/'
K_SIMH_PATH = DATABASE + 'K_simhash_input/'

prov_subj_file = 'prov_subject_{}_{}_{}.txt'


def packageKSimhash(requir_user_file, thsold):
    ''' 对prov_sub数据通过user作为质点，进行降维
                                    @param requir_user_file     原始数据文件(user_id, province， question)
                                    @param thsold               阈值，如果小于此数值即为有用数值，int
                                '''
    prov_set = getProvinceSet()
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
                    ps_file = PROV_SUB_PATH + datetime + '/' + prov + '/' + prov_subj_file.format(prov, ks,
                                                                                                        datetime)

                    if os.path.exists(ps_file):
                        PATH = K_SIMH_PATH + datetime + '/' + prov
                        mkdir(PATH)

                        if os.path.exists(PATH + '/' + output_file.format(prov, ks, datetime)):
                            os.remove(PATH + '/' + output_file.format(prov, ks, datetime))
                        k_simh_list = []
                        with open(ps_file, 'r') as ksimhash_file:
                            while True:
                                ksimhash = ksimhash_file.readline()
                                if ksimhash:
                                    sim_dis = K_Simhash(ksimhash=eval(ksimhash), vs=vs)

                                    if sim_dis < thsold:
                                        with open(PATH + '/' + output_file.format(prov, ks, datetime),
                                                  'a') as txt_file:
                                            txt_file.writelines(json.dumps(eval(ksimhash)))
                                            txt_file.write('\n')

                                else:
                                    break
                            ksimhash_file.close()


                    else:
                        print(u"没有在Prov_Sub_input中查询到生成的（省份-学科）文件！")


if __name__ == '__main__':
    # prov_set = getProvinceSet()
    #subj_set = {str(j) for j in range(1, 11)} | {str(j) for j in range(21, 31)} | {str(j) for j in range(41, 51)} |{'52', '62', '72'}
    # subj_set = {'2', '5'}
    # prov_set = {'福建'}
    requir_user_file = 'requir_user.csv'
    output_file = 'KSimhash_{}_{}_{}.txt'


    packageKSimhash(requir_user_file=requir_user_file,thsold=30)

