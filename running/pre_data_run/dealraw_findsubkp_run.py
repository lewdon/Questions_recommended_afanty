# -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import json
import csv
from lib.util import mkdir
from lib.table_data import tableToJson

_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).replace('\\','/')
DATABASE = _DIR + '/new_database/'
RAW_PATH = DATABASE + 'Raw_input/'
SUB_KPOINT_PATH = DATABASE + 'Sub_kpoint_input/'
datetime = '09-11'
init_file = 'user_if_{}.csv'
skp_file =  'question_sub_kpoint_{}_{}.txt'

if __name__ == '__main__':
    province_set = {'nan', '0', '1', '2', '3', 'Rize', 'Juzny', 'hobbit', '全国', '台湾', 'NULL'}
    mkdir(SUB_KPOINT_PATH + datetime)
    PATH = SUB_KPOINT_PATH + datetime + '/'
    with open(RAW_PATH + init_file.format(datetime), 'r', encoding='utf-8') as raw_file:
        readers = csv.DictReader(raw_file)
        for reader in readers:
            prov = reader['province']
            if prov in province_set:
                prov = '全国'
            else:
                prov = prov[:2]

            question_id = []
            try:
                if isinstance(reader['question'], str):
                    if len(eval(reader['question'])) > 0:
                        question_id = [i[1] for i in eval(reader['question'])]
            except Exception as e:
                print(e)
            finally:
                question_list = tableToJson(
                    table='question_simhash_20171111',
                    question_id=question_id
                    )

                if len(question_list) > 0:
                    with open(PATH + skp_file.format(prov,datetime),'a',encoding='utf-8') as prov_file:
                        prov_file.writelines(json.dumps({reader['user_id']: question_list}) + '\n')

        raw_file.close()
        prov_file.close()
