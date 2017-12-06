# -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import json
import csv
from multiprocessing import Pool
from lib.util import mkdir,getProvinceSet
from lib.table_data import tableToJson
import logging


datetime = '09-11'
LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/LYF_find_subkp_{}.log'.format(datetime), filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
SUB_KPOINT_PATH = DATABASE + 'Sub_kpoint_input/'

pre_province_file = 'user_json_{}_{}.txt'
skp_file =  'question_sub_kpoint_{}_{}.txt'


def packFind_subkp(prov, PATH):
    if os.path.exists(PATH + skp_file.format(prov, datetime)):
        os.remove(PATH + skp_file.format(prov, datetime))

    with open(PRE_DATA_PATH + datetime + '/' + pre_province_file.format(prov, datetime), 'r',
              encoding='utf-8') as pre_file:
        while True:
            line = pre_file.readline()
            if line:
                user_qid_dic = eval(line)

                question_list = tableToJson(
                    table='question_simhash_20171111',
                    question_id=list(user_qid_dic.values())[0]
                )

                if len(question_list) > 0:
                    userid = list(user_qid_dic.keys())
                    user_sub = userid.extend(question_list)

                    with open(PATH + skp_file.format(prov, datetime), 'a') as prov_sub_file:
                        prov_sub_file.writelines(json.dumps(userid) + '\n')

            else:
                break

        pre_file.close()
        prov_sub_file.close()
        logging.info("the prov : {} has been finished !".format(prov))


if __name__ == '__main__':
    prov_set = getProvinceSet()
    # prov_set = {'全国'}
    pool = Pool(3)
    mkdir(SUB_KPOINT_PATH + datetime)
    PATH = SUB_KPOINT_PATH + datetime + '/'
    for prov in prov_set:
        logging.info("running the prov is: {}".format(prov))

        pool.apply_async(packFind_subkp,kwds={
            "prov":prov,
            "PATH":PATH
        })

    pool.close()
    pool.join()

