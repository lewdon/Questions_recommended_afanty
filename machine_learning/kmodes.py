# -*-coding:utf8-*-
import os
import sys
from collections import defaultdict
import pandas as pd
import numpy as np
import time
from scipy import sparse
from sklearn.base import BaseEstimator, ClusterMixin
from sklearn.utils.validation import check_array
from lib.kmodes_init import (
    get_max_value_key,
    encode_features,
    get_unique_rows,
    decode_centroids,
    matching_dissim,
    euclidean_dissim,
    set_nclusters
)

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\','/')
DATABASE = _DIR + '/database/'
KMODES_PATH = DATABASE + 'Kmods/'
SUB_FILE = DATABASE + 'sub_kpoint_diff/'


def init_huang(X, n_clusters, dissim):
    ''' huang算法情况下，寻找质点centroids
                            @param X            DataFrame格式数据
                            @param n_clusters   clusters分区值
                            @param dissim       字符串距离计算逻辑
                            @param centroids    选取的样本数组
                        '''
    nattrs = X.shape[1]
    centroids = np.empty((n_clusters, nattrs), dtype='object')
    # 统计属性的频率
    for iattr in range(nattrs):
        freq = defaultdict(int)
        for curattr in X[:, iattr]:
            freq[curattr] += 1

        choices = [chc for chc, wght in freq.items() for _ in range(wght)]

        choices = sorted(choices)
        centroids[:, iattr] = np.random.choice(choices, n_clusters)

    for ik in range(n_clusters):
        ndx = np.argsort(dissim(X, centroids[ik]))

        while np.all(X[ndx[0]] == centroids, axis=1).any():
            ndx = np.delete(ndx, 0)
        centroids[ik] = X[ndx[0]]

    return centroids


def init_cao(X, n_clusters, dissim):
    ''' cao算法情况下，寻找质点centroids
                                @param X            DataFrame格式数据
                                @param n_clusters   clusters分区值
                                @param dissim       字符串距离计算逻辑
                                @param centroids    选取的样本数组
                            '''
    npoints, nattrs = X.shape
    centroids = np.empty((n_clusters, nattrs), dtype='object')
    # 方法基于确定点的密度。
    dens = np.zeros(npoints)
    for iattr in range(nattrs):
        freq = defaultdict(int)
        for val in X[:, iattr]:
            freq[val] += 1
        for ipoint in range(npoints):
            dens[ipoint] += freq[X[ipoint, iattr]] / float(nattrs)
    dens /= npoints

    # 根据距离和密度选择初始质心。
    centroids[0] = X[np.argmax(dens)]
    if n_clusters > 1:
        # 对于其余的质心，选择最大dens * dissim到最低dens * dissim（已分配）的质心。
        for ik in range(1, n_clusters):
            dd = np.empty((ik, npoints))
            for ikk in range(ik):
                dd[ikk] = dissim(X, centroids[ikk]) * dens
            centroids[ik] = X[np.argmax(np.min(dd, axis=0))]

    return centroids


def move_point_cat(point, ipoint, to_clust, from_clust, cl_attr_freq,
                   membship, centroids):
    '''在cluster之间移动点，分类属性。
                        @param point，ipoint         memship切片区间数值
                        @param to_clust，from_clust  memship切片区间数值
                        @param centroids             选取的样本数组
                        @param cl_attr_freq          clusters属性频率
                        @param membship              clusters中成员属性关系
                    '''

    membship[to_clust, ipoint] = 1
    membship[from_clust, ipoint] = 0
    # 更新集群中属性的频率。
    for iattr, curattr in enumerate(point):
        to_attr_counts = cl_attr_freq[to_clust][iattr]
        from_attr_counts = cl_attr_freq[from_clust][iattr]

        to_attr_counts[curattr] += 1

        current_attribute_value_freq = to_attr_counts[curattr]
        current_centroid_value = centroids[to_clust][iattr]
        current_centroid_freq = to_attr_counts[current_centroid_value]
        if current_centroid_freq < current_attribute_value_freq:
            centroids[to_clust][iattr] = curattr

        from_attr_counts[curattr] -= 1

        old_centroid_value = centroids[from_clust][iattr]
        if old_centroid_value == curattr:
            centroids[from_clust][iattr] = get_max_value_key(from_attr_counts)

    return cl_attr_freq, membship, centroids


def _labels_cost(X, centroids, dissim):
    ''' 给定一个矩阵的点和k-modes算法的质心列表
                        @param X            DataFrame格式数据
                        @param centroids    选取的样本数组
                        @param dissim       字符串距离计算逻辑
                        @param labels       生成标签数组
                        @param cost         字符串距离大小
                    '''
    X = check_array(X)

    npoints = X.shape[0]
    cost = 0.
    labels = np.empty(npoints, dtype=np.uint8)
    for ipoint, curpoint in enumerate(X):
        diss = dissim(centroids, curpoint)
        clust = np.argmin(diss)
        labels[ipoint] = clust
        cost += diss[clust]

    return labels, cost


def _k_modes_iter(X, centroids, cl_attr_freq, membship, dissim):
    ''' kmodes聚类算法的单次迭代,通过move_point_cat函数找到更换质点centroids
                    @param X                DataFrame格式数据
                    @param centroids        选取的样本数组
                    @param cl_attr_freq     clusters属性频率
                    @param membship         clusters中成员属性关系
                    @param dissim           最大迭代数值，默认为200
                '''
    moves = 0
    for ipoint, curpoint in enumerate(X):
        clust = np.argmin(dissim(centroids, curpoint))
        if membship[clust, ipoint]:
            continue

        #移动质点，并更新群集频率和质心。
        moves += 1
        old_clust = np.argwhere(membship[:, ipoint])[0][0]

        cl_attr_freq, membship, centroids = move_point_cat(
            curpoint, ipoint, clust, old_clust, cl_attr_freq, membship, centroids
        )

        # 如果是空集群，则使用最大集群中的一个随机点重新初始化。
        if np.sum(membship[old_clust, :]) == 0:
            from_clust = membship.sum(axis=1).argmax()
            choices = [ii for ii, ch in enumerate(membship[from_clust, :]) if ch]
            rindx = np.random.choice(choices)

            cl_attr_freq, membship, centroids = move_point_cat(
                X[rindx], rindx, old_clust, from_clust, cl_attr_freq, membship, centroids
            )

    return centroids, moves


def k_modes(X, n_clusters, max_iter, dissim, init, n_init, verbose):
    ''' k-modes算法
                @param X            DataFrame格式数据
                @param n_clusters   clusters数值，对cluster进行分区
                @param max_iter     最大迭代数值，默认为200
                @param dissim       字符串距离算法选择
                @param init         最大迭代数值，默认为200
                @param n_init       记录迭代次数标签
                @param verbose      相当于flag作用
            '''
    if sparse.issparse(X):
        raise TypeError("k-modes does not support sparse data.")

    # 将pandas对象转换为numpy数组。
    if 'pandas' in str(X.__class__):
        X = X.values

    X = check_array(X, dtype=None)

    # 将X中的分类值转换为整数以获得速度。
    # 基于X中的唯一值，通过一个映射来实现。
    X, enc_map = encode_features(X)

    npoints, nattrs = X.shape
    assert n_clusters <= npoints, "Cannot have more clusters ({}) " \
                                  "than data points ({}).".format(n_clusters, npoints)

    unique = get_unique_rows(X)
    n_unique = unique.shape[0]
    if n_unique <= n_clusters:
        max_iter = 0
        n_init = 1
        n_clusters = n_unique
        init = unique

    all_centroids = []
    all_labels = []
    all_costs = []
    all_n_iters = []
    for init_no in range(n_init):

        # _____ INIT _____
        if verbose:
            print("Init: initializing centroids")
        if isinstance(init, str) and init.lower() == 'huang':
            centroids = init_huang(X, n_clusters, dissim)
        elif isinstance(init, str) and init.lower() == 'cao':
            centroids = init_cao(X, n_clusters, dissim)
        elif isinstance(init, str) and init.lower() == 'random':
            seeds = np.random.choice(range(npoints), n_clusters)
            centroids = X[seeds]
        elif hasattr(init, '__array__'):
            if len(init.shape) == 1:
                init = np.atleast_2d(init).T
            assert init.shape[0] == n_clusters, \
                "Wrong number of initial centroids in init ({}, should be {})."\
                .format(init.shape[0], n_clusters)
            assert init.shape[1] == nattrs, \
                "Wrong number of attributes in init ({}, should be {})."\
                .format(init.shape[1], nattrs)
            centroids = np.asarray(init, dtype=np.uint8)
        else:
            raise NotImplementedError

        if verbose:
            print("Init: initializing clusters")
        membship = np.zeros((n_clusters, npoints), dtype=np.uint8)

        cl_attr_freq = [[defaultdict(int) for _ in range(nattrs)]
                        for _ in range(n_clusters)]
        for ipoint, curpoint in enumerate(X):
            clust = np.argmin(dissim(centroids, curpoint))
            membship[clust, ipoint] = 1
            for iattr, curattr in enumerate(curpoint):
                cl_attr_freq[clust][iattr][curattr] += 1

        for ik in range(n_clusters):
            for iattr in range(nattrs):
                if sum(membship[ik]) == 0:
                    centroids[ik, iattr] = np.random.choice(X[:, iattr])
                else:
                    centroids[ik, iattr] = get_max_value_key(cl_attr_freq[ik][iattr])

        # _____ ITERATION _____
        if verbose:
            print("Starting iterations...")
        itr = 0
        converged = False
        cost = np.Inf
        while itr <= max_iter and not converged:
            itr += 1
            centroids, moves = _k_modes_iter(X, centroids, cl_attr_freq, membship, dissim)

            labels, ncost = _labels_cost(X, centroids, dissim)
            converged = (moves == 0) or (ncost >= cost)
            cost = ncost
            if verbose:
                print("Run {}, iteration: {}/{}, moves: {}, cost: {}"
                      .format(init_no + 1, itr, max_iter, moves, cost))


        all_centroids.append(centroids)
        all_labels.append(labels)
        all_costs.append(cost)
        all_n_iters.append(itr)

    best = np.argmin(all_costs)
    if n_init > 1 and verbose:
        print("Best run was number {}".format(best + 1))

    return all_centroids[best], enc_map, all_labels[best], \
        all_costs[best], all_n_iters[best]


class KModes(BaseEstimator, ClusterMixin):

    """
    Parameters
    -----------
    n_clusters : int, optional, default: 8
        簇的个数
    max_iter : int, default: 200
        最大迭代次数
    cat_dissim : func, default: matching_dissim
        计算字符串之间距离的逻辑算法
    init : {'Huang', 'Cao', 'random' or an ndarray}, default: 'Cao'
    n_init : int, default: 10
        kmodes算法将运行不同centroids seed的时间数。 最终的结果将是cost上n_init连续运行的最佳输出。
    verbose : int, optional
        相当于是flag功能，只有0和大于0的区别
    Attributes
    ----------
    cluster_centroids_ : array, [n_clusters, n_features]
        cluster下centroids类别
    labels_ :
        每个点的标签，即是距离哪个centroids最近
    cost_ : float
        cluster的cost，定义为所有点到它们各自的cluster下centroids的总距离。
    n_iter_ : int
        迭代次数.
    """

    def __init__(self, n_clusters=8, max_iter=200, cat_dissim=matching_dissim,
                 init='Cao', n_init=1, verbose=0):

        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.cat_dissim = cat_dissim
        self.init = init
        self.n_init = n_init
        self.verbose = verbose
        if ((isinstance(self.init, str) and self.init == 'Cao') or
                hasattr(self.init, '__array__')) and self.n_init > 1:
            if self.verbose:
                print("Initialization method and algorithm are deterministic. "
                      "Setting n_init to 1.")
            self.n_init = 1

    def fit(self, X, y=None, **kwargs):
        self._enc_cluster_centroids, self._enc_map, self.labels_,\
            self.cost_, self.n_iter_ = k_modes(X,
                                               self.n_clusters,
                                               self.max_iter,
                                               self.cat_dissim,
                                               self.init,
                                               self.n_init,
                                               self.verbose)
        return self


    def predict(self, X, **kwargs):
        assert hasattr(self, '_enc_cluster_centroids'), "Model not yet fitted."
        X = check_array(X, dtype=None)
        X, _ = encode_features(X, enc_map=self._enc_map)
        return _labels_cost(X, self._enc_cluster_centroids, self.cat_dissim)[0]

    @property
    def cluster_centroids_(self):
        if hasattr(self, '_enc_cluster_centroids'):
            return decode_centroids(self._enc_cluster_centroids, self._enc_map)
        else:
            raise AttributeError("'{}' object has no attribute 'cluster_centroids_' "
                                 "because the model is not yet fitted.")


def mergeLabelSaveCsv(X, labels, filename):
    ''' 通过numpy.savetxt保存原始数据与labels合并后的数组
                @param X            传入DataFrame格式数据
                @param labels       labels列表
                @param filenaem     输出数据到指定文件路径
            '''
    labels_col = labels.shape[0]
    X_col = X.shape[0]

    if labels_col == X_col:
        merge_labels = np.column_stack((labels, X))
        headers = ', '.join(['#' + str(k + 1) for k in range(merge_labels.shape[1])])
        np.savetxt(KMODES_PATH + filename,
                   merge_labels,
                   delimiter=',',
                   header=headers,
                   fmt='%s',
                   comments=' ')

    else:
        raise ValueError("all the input array dimensions "
                         "except for the concatenation axis must match exactly")


def packageKmodes(filename, clusters_k, outputfile):
    ''' kmodes封装
            @param filename       传入数据文件
            @param clusters_k     进行cluster分区
            @param outputfile     输出数据到指定文件路径
        '''
    Y = np.genfromtxt(SUB_FILE + filename, dtype=str, delimiter=',')[1:,:]
    X = Y[:,1:]

    clusters_k = clusters_k or 200
    n_clusters = set_nclusters(int(X.shape[0]), k=clusters_k)

    kmodes_cao = KModes(n_clusters=n_clusters,
                        init='cao', verbose=1)
    kmodes_cao.fit(X)

    # 示例[2 2 2 2 2 2 2 2 2 2 1 1 1 1 1 1 1 1 1 1 3 3 3 3 3 3 3 0 3 3 4 0 0 4 0 4 0 0 4 4 0 4 0 4 0 4 0]
    labels_cao = kmodes_cao.labels_
    # 储存中间数值
    mergeLabelSaveCsv(Y, labels_cao, outputfile)
