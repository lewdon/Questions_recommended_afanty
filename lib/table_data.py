# -*-coding:utf8-*-
import os
import sys
import pymysql
import pymysql.cursors
from multiprocessing import Pool, Process
import re
import time
import pandas as pd
import numpy as np
import datetime
import logging
import json
from lib.util import getProvinceSet,mkdir
import requests
import asyncio
import random
import math

# LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
# logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
#                     filename='working/questionid_sub_01.log', filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
CONFIG_FILE = os.path.join(_DIR, 'config')
DATABASE = _DIR + '/new_database/'
# SUB_KPOINT_PATH = DATABASE + '/Sub_kpoint_input/'
datetime = '09-11'

async def qidSubject(cur, table, question_id):
    sql = "select sub_kpoint,subject from {0} where question_id = '{1}' ".format(table, question_id)
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        return [question_id,data[0]['subject']]


async def getKpoint(cur, table, question_id):
    sql = "select sub_kpoint,subject from {0} where question_id = '{1}' ".format(table, question_id)
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        return (data[0]['sub_kpoint'],data[0]['subject'])


async def main_tasks(tasks):
    return await asyncio.gather(*tasks)


def tableToJson(table, question_id):
    ''' 通过question_id获取字段sub_kpoint_diff
                            @param table            table表
                            @param question_id      包含question_id的list
                        '''
    config = json.load(open(CONFIG_FILE))
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()
    sub_kpoint_dict = {}
    question_list = []
    tasks = []
    for i in range(len(question_id)):
        tasks.append(asyncio.ensure_future(getKpoint(cur, table, question_id[i])))

    loop = asyncio.get_event_loop()
    try:
        results = loop.run_until_complete(main_tasks(tasks))
        for result in results:
            if result is not None:
                question_list.append(result)

    except KeyboardInterrupt as e:
        print(asyncio.Task.all_tasks())
        for task in asyncio.Task.all_tasks():
            print(task.cancel())
        loop.stop()
        loop.run_forever()

    return question_list


def getQidSubj(table, question_id):
    ''' 通过question_id获取字段sub_kpoint_diff
                            @param table            table表
                            @param question_id      包含question_id的list
                        '''
    config = json.load(open(CONFIG_FILE))
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()
    sub_kpoint_dict = {}
    question_list = []
    tasks = []
    for i in range(len(question_id)):
        tasks.append(asyncio.ensure_future(qidSubject(cur, table, question_id[i])))

    loop = asyncio.get_event_loop()
    try:
        results = loop.run_until_complete(main_tasks(tasks))
        for result in results:
            if result is not None:
                question_list.append(result)

    except KeyboardInterrupt as e:
        print(asyncio.Task.all_tasks())
        for task in asyncio.Task.all_tasks():
            print(task.cancel())
        loop.stop()
        loop.run_forever()

    return question_list


def getQuestionId(table, recom_set, diff, question_id):
    ''' 通过sub_kpoint_diff获取字段question_id
                                @param table        table表
                                @param recom_set    推荐结果的sub_kpoint的set
                                @param question_id  包含question_id的list
                                @param diff         选择难度，int
                            '''
    config = json.load(open(CONFIG_FILE))
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()
    recom_list = []

    diff = diff or 1
    difficulty = diff
    for n,recom in enumerate(recom_set):
        if recom is not None:
            flag = 2
            while flag > 0:
                subj_kp_diff = recom + str(difficulty)
                sql = "select question_id from {0} where sub_kpoint_diff = '{1}' ".format(table, subj_kp_diff)
                cur.execute(sql)
                data = cur.fetchall()

                if len(data) != 0:
                    random.seed(math.log(int(recom), 100) * (difficulty+1) * (n+1) * (flag + 1))
                    if data[random.randint(0,len(data)-1)]['question_id'] not in set(question_id):
                        recom_list.append(data[random.randint(0,len(data)-1)]['question_id'])
                        flag = -10

                else:
                    if flag != diff and flag != difficulty:
                        difficulty = flag
                    else:
                        difficulty = flag - 1
                flag -= 1

    return recom_list