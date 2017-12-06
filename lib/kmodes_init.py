# -*-coding:utf8-*-
import os
import sys
import numpy as np
from simhash import Simhash


#=获得dict.keys()中最大的。
def get_max_value_key(dic):
    v = np.array(list(dic.values()))
    k = np.array(list(dic.keys()))

    maxima = np.where(v == np.max(v))[0]
    if len(maxima) == 1:
        return k[maxima[0]]
    else:
        # 当出现多个相同最大值时，选择argmin即位置最小的值。
        return k[maxima[np.argmin(k[maxima])]]


#将X中数值进行set()，并添加到dict(enc_map)z中
def encode_features(X, enc_map=None):
    if np.issubdtype(X.dtype, np.integer):
        enc_map = [{val: val for val in np.unique(col)} for col in X.T]
        return X, enc_map

    if enc_map is None:
        fit = True
        enc_map = []
    else:
        fit = False

    Xenc = np.zeros(X.shape).astype('int')

    for ii in range(X.shape[1]):
        if fit:
            col_enc = {val: jj for jj, val in enumerate(np.unique(X[:, ii]))
                       if not (isinstance(val, float) and np.isnan(val))}
            enc_map.append(col_enc)

        #给np.NaNs等未知数据赋值为-1.
        Xenc[:, ii] = np.array([enc_map[ii].get(x, -1) for x in X[:, ii]])

    return Xenc, enc_map


#通过list(mapping)将编码过的centroids解码回原始数据labels
def decode_centroids(encoded, mapping):
    decoded = []
    for ii in range(encoded.shape[1]):
        # 通过转换成mapping映射来decode转码
        inv_mapping = {v: k for k, v in mapping[ii].items()}
        decoded.append(np.vectorize(inv_mapping.__getitem__)(encoded[:, ii]))
    return np.atleast_2d(np.array(decoded)).T


#获取numpy数组中的唯一行。
def get_unique_rows(a):
    return np.vstack({tuple(row) for row in a})


#设置n_clusters数值
def set_nclusters(number, k):
    if isinstance(number, int):
        n_clusters = int(number / k)
        shifenwei = int(number % k)
        if shifenwei >= 6:
            n_clusters += 1
        if n_clusters <= 0:
            n_clusters = 1
        return n_clusters


#kmodes算法下字符串距离
def matching_dissim(a, b):
    return np.sum(a != b, axis=1)

#Kmodes算法下（a-b）*2计算字符串距离
def euclidean_dissim(a, b):
    """Euclidean distance dissimilarity function"""
    if np.isnan(a).any() or np.isnan(b).any():
        raise ValueError("Missing values detected in numerical columns.")
    return np.sum((a - b) ** 2, axis=1)


def simhash_dissim(a,b):
    return Simhash(a).distance(Simhash(b))
