# -*-coding:utf8-*-
import os
import sys
import pymysql
import pymysql.cursors
import re
import time
import datetime
import logging
import json
import jieba.posseg
from simhash import Simhash

LOGGING_FORMAT = '%(asctime)-15s:%(levelname)s: %(message)s'
# logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
#                     filename='working/new_150_260.log', filemode='a')
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO,
                    filename='working/new_92_150.log', filemode='a')

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(_DIR, 'config')
FLAG_SET = {'uj', 'c', 'x'}

def convertDiff(difficulty):
    difficult = int(difficulty / 40)
    diff = str(difficult)
    return diff


def dealStringSimHash(sub_kpoint_string):
    sub_kpoint = []
    sub_kpoint_string = sub_kpoint_string.replace('.', '').replace('；', '')
    words = jieba.posseg.cut(sub_kpoint_string)
    for word, flag in words:
        if flag not in FLAG_SET:
            sub_kpoint.append(word)

    return Simhash(sub_kpoint).value


def Data_to_MySQL(datas):
    #采用同步的机制写入mysql
    config = json.load(open(CONFIG_FILE))
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    record_dict = {
        'on_off':datas['on_off'],
        'exam_city': datas['exam_city'],
        'answer_all_html': datas["answer_all_html"],
        'knowledge_point_exam_num': datas['knowledge_point_exam_num'],
        'subject': datas['subject'],
        'exam_year': datas['exam_year'],
        'zhuanti': datas['zhuanti'],
        'question_type': datas['question_type'],
        'spider_source':datas['spider_source'],
        'question_degree':datas['question_degree'],
        'question_html': datas["question_html"],
        'create_time':datas['create_time'],
        'error_rate':datas['error_rate'],
        'sub_kpoint':datas['sub_kpoint'],
        'question_id':datas['question_id'],
        'jieda':datas['jieda'],
        'update_time':datas['update_time'],
        'fenxi':datas['fenxi'],
        'spider_url': datas['spider_url'],
        'question_quality': datas['question_quality'],
        'knowledge_point': datas['knowledge_point'],
        'dianping': datas['dianping'],
        'option_html': datas['option_html'],
        'zujuan':datas['zujuan'],
        'sub_kpoint_diff':datas['sub_kpoint_diff']
    }

    cols, values = zip(*record_dict.items())


    insert_sql = 'insert ignore into {table} ({cols}) values ({values})'.format(
        table='question_simhash_20171111',
        cols=', '.join(['`%s`' % col for col in cols]),
        values=', '.join(['%s' for col in cols])
    )

    try:
        cursor.execute(insert_sql, values)
    except Exception as e:
        print(e)
    conn.commit()
    del datas

def tableToJson(table):
    config = json.load(open(CONFIG_FILE))
    first_id = 38495122
    # first_id = 112558395
    conn = pymysql.connect(host=config['host'], user=config['user'], passwd=config['password'], db='tiku_cloud',
                           port=3306, charset= "utf8", use_unicode=True, cursorclass = pymysql.cursors.DictCursor)
    cur = conn.cursor()

    while True:
        sql = 'select * from {0} WHERE question_id > {1} limit 20000'.format(table, first_id)
        # sql = 'select * from {0} limit {1},{2}'.format(table, first_id, end_id)
        cur.execute(sql)
        data = cur.fetchall()

        if int(data[0]['question_id']) > 112558395:
            break

        try:
            Parser(data)
        except Exception as e:
            print(e)

        logging.info('read the data from question_id = {}'.format(first_id))

        first_id = int(data[-1]['question_id'])
        del data

def Parser(data):
    tasks = []
    for row in data:
        result = parse_detail(row)
        Data_to_MySQL(result)


def parse_detail(row):
    k_point = row['knowledge_point']
    difficulty = 100 - int(row['question_degree'])
    diff = convertDiff(difficulty)
    subject = row['subject']

    sub_kpoint_str = str(subject) + k_point
    sub_kpoint = dealStringSimHash(sub_kpoint_str)
    row['sub_kpoint'] = str(sub_kpoint)

    row['sub_kpoint_diff'] = str(sub_kpoint) + diff

    return row


if __name__ == '__main__':
    jsonData = tableToJson('question_20171111_old')


