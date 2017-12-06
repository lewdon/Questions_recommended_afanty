# -*-coding:utf8-*-
import os
import sys
import pandas as pd
import numpy as np
import re
import hashlib
import json


_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
DATABASE = _DIR + '/new_database/Input/'
RAW_PATH = DATABASE + 'Raw_input/'


def getProvinceSet():
    with open(RAW_PATH + 'prov.txt', 'r', encoding='utf-8') as f:
        prov = f.readlines()
        prov_set = eval(prov[0])
        f.close()
    return prov_set


#获取value最大长度
def getMaxLength(data_dic):
    max_len = 0
    for v in data_dic.values():
        v_len = len(v)
        if v_len > max_len:
            max_len = v_len

    return max_len


#检验，并生成文件路径
def mkdir(path):
    isExitPath = os.path.exists(path)
    if not isExitPath:
        os.makedirs(path)