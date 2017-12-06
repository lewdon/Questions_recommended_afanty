# -*-coding:utf8-*-
import os
import sys
from collections import defaultdict
import pandas as pd
import numpy as np
import time
from sklearn.utils.validation import check_array
from simhash import Simhash
import multiprocessing
import time
from simhash import Simhash


def median(lst):
    if not lst:
        return
    lst = sorted(lst)
    if len(lst)%2 == 1:
        return lst[len(lst) // 2]
    else:
        return  (lst[len(lst) // 2 - 1] + lst[len(lst) // 2]) / 2.0


def K_Simhash(ksimhash, vs):
    sim_list = []

    for ks in ksimhash[1:]:
        sim_list.append(min([Simhash(v).distance(Simhash(ks)) for v in vs]))

    return median(sim_list)
