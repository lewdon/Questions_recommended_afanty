# -*-coding:utf8-*-
import os
import sys
import csv
import time
import json
import pandas as pd
from lib.util import getProvinceSet,mkdir


_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
RAW_PATH = DATABASE + 'Raw_input/'
PRE_DATA_PATH = DATABASE + 'Pre_data_input/'
init_file = 'user_if_{}.csv'
pre_province_file = 'user_json_{}_{}.csv'
pre_province_txt = 'user_json_{}_{}.txt'
datetime = '09-11'

if __name__ == '__main__':
    province_set = {'nan', '0', '1', '2', '3', 'Rize', 'Juzny', 'hobbit', '全国', '台湾', 'NULL'}
    mkdir(PRE_DATA_PATH + datetime)
    PATH = PRE_DATA_PATH + datetime + '/'
    with open(RAW_PATH + init_file.format(datetime), 'r', encoding='utf-8') as raw_file:
        # data = csv.reader(f)
        # for d in data:
        #     print(d)
        readers = csv.DictReader(raw_file)
        for reader in readers:
            prov = reader['province']
            if prov in province_set:
                prov = '全国'
            elif prov in {'闽'}:
                prov = '福建'
            else:
                prov = prov[:2]

            time_question = []
            try:
                if isinstance(reader['question'], str):
                    if len(eval(reader['question'])) > 0:
                        time_question = [i[1] for i in eval(reader['question'])]
            except Exception as e:
                print(e)
            finally:
                rank = {reader['user_id']: time_question}
                with open(PATH + pre_province_txt.format(prov, datetime), 'a', encoding='utf-8') as prov_file:
                    prov_file.writelines(json.dumps(rank) + '\n')

        raw_file.close()






