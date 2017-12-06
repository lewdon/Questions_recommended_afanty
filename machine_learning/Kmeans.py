 # -*-coding:utf8-*-
import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from lib.util import loadDataSet

def getKmeans(filename, k, iteration= 500):
    dataSet = loadDataSet(filename, '\t')
    dataMat = np.mat(dataSet[: ,1 :])
    kmeans = KMeans(init='k-means++',
                    n_clusters=k,
                    n_jobs= 4,
                    max_iter= iteration)
    kmeans.fit(dataMat)
    return dataMat, kmeans

def printResult(inputfile, kmeans, outputfile):
    data = pd.read_excel(inputfile, index_col= 'Id')
    r1 = pd.Series(kmeans.labels_).value_counts()   #统计各个类别数目
    r2 = pd.DataFrame(kmeans.cluster_centers_)      #找出聚类中心
    r = pd.concat([r2, r1],
                  axis=1)                 #横向连接，得到聚类中心对应的类别下的数目
    r.colums = list(data.colums) + [u'类别数目']
    print(r)
    r = pd.concat([data, pd.Series(kmeans.labels_, index= data.index)],
                  axis=1)
    r.colums = list(data.colums) + [u'聚类类别']
    r.to_excel(outputfile)

def showDraw(dataMat, kmeans):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.scatter(plt,
               dataMat,
               marker='^',
               size=20,
               color= 'b')
    ax.scatter(plt,
               kmeans.cluster_centers_,
               marker='o',
               size=60,
               color='red')
    plt.show()

