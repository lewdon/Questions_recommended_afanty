# -*-coding:utf8-*-
import os
import sys

class treeNode:
    def __init__(self, nameValue, numOccur, parentNode):
        self.name = nameValue       #节点元素名称
        self.count = numOccur       #出现次数
        self.nodeLink = None        #指向下一个相似节点的指针
        self.parent = parentNode    #指向父节点的指针，在构造时初始化为定值
        self.children = {}          #指向子节点的字典，以子节点的元素名称为键，指向子节点的指针为值

    #增加节点的出现次数值
    def inc(self, numOccur):
        self.count += numOccur

    #输出节点和子节点的FP树结构
    def disp(self, ind=1):
        print('  ' * ind , self.name , '  ', self.count)
        for child in self.children.values():
            child.disp(ind +1)

#输入：数据集、最小值尺度；输出：FP树、头指针表
def createTree(dataSet, minSup):
    #第一次遍历数据集，创建头指针表
    minSup = minSup or 10
    headerTable = {}

    #{'s': 3, 'o': 1, 'v': 1, 'r': 3, 'u': 1, 'e': 1, 'm': 1, 'x': 4, 'h': 1}
    for trans in dataSet:
        for item in trans:
            headerTable[item] = headerTable.get(item, 0) + dataSet[trans]

    #移除不满足最小支持度的元素项：{'z': 0.8333333333333334, 's': 0.5, 't': 0.5, 'r': 0.5, 'y': 0.5}
    # for k in headerTable.keys():
    #     if headerTable[k] < minSup:
    #         headerTable.pop(k)
    removeMinSupHeaderTable = {k:v for k,v in headerTable.items()
                               if v >= minSup}

    #空元素集，返回空
    freqItemSet = set(removeMinSupHeaderTable.keys())
    if len(freqItemSet) == 0:
        return None, None

    #增加一个数据项，用于存储指向相似元素项指针
    for k in removeMinSupHeaderTable:
        removeMinSupHeaderTable[k] = [removeMinSupHeaderTable[k], None]
    retTree = treeNode('Null Set', 1, None)

    #第二次遍历数据集，创建FP树
    for tranSet, count in dataSet.items():
        localD = {}  #记录每个元素项的全局频率，用于排序
        for item in tranSet:
            if item in freqItemSet:
                localD[item] = removeMinSupHeaderTable[item][0]
        if len(localD) > 0:
            orderedItems = [v[0] for v in sorted(localD.items(), key=lambda p: p[1], reverse=True)]
            updateTree(orderedItems, retTree, removeMinSupHeaderTable, count)

    return retTree, removeMinSupHeaderTable


def updateTree(items, inTree, headerTable, count):
    if items[0] in inTree.children:
        inTree.children[items[0]].inc(count)
    else:
        #若没有这个元素，创建一个新节点
        inTree.children[items[0]] = treeNode(items[0], count, inTree)
        if headerTable[items[0]][1] == None:
            headerTable[items[0]][1] = inTree.children[items[0]]
        else:
            updateHeader(headerTable[items[0]][1], inTree.children[items[0]])

    if len(items) > 1:
        updateTree(items[1::], inTree.children[items[0]], headerTable, count)


def updateHeader(nodeToTest, targetNode):
    while (nodeToTest.nodeLink != None):
        nodeToTest = nodeToTest.nodeLink
    nodeToTest.nodeLink = targetNode

def loadSimpDat():
    simpDat = [['r', 'z', 'h', 'j', 'p', 'h'],
               ['z', 'y', 'x', 'w', 'v', 'u', 't', 's'],
               ['z', 'h'],
               ['r', 'x', 'x', 'n', 'o', 's'],
               ['y', 'r', 'x', 'z', 'q', 't', 'p'],
               ['y', 'z', 'x', 'e', 'q', 's', 't', 'm']]
    return simpDat

#初始化dataSet
def createInitSet(dataSet):
    retDict = {}
    for trans in dataSet:
        retDict[frozenset(trans)] = 1
    return  retDict

#创建前缀路径
def findPrefixPath(basePat, treeNode):
    condPats = {}
    while treeNode != None:
        prefixPath = []
        ascendTree(treeNode, prefixPath)
        if len(prefixPath) > 1:
            condPats[frozenset(prefixPath[1:])] = treeNode.count
        treeNode = treeNode.nodeLink
    return condPats


def ascendTree(leafNode, prefixPath):
    if leafNode.parent != None:
        prefixPath.append(leafNode.name)
        ascendTree(leafNode.parent, prefixPath)

#递归查找频繁项集
# inTree和headerTable是由createTree()函数生成的数据集的FP树
# minSup表示最小支持度
# preFix请传入一个空集合(set([])),将在函数中用于保存当前前缀
# freqItemList请传入一个空列表（[]）,将用来存储删除的频繁项集
def mineTree(inTree, headerTable, minSup, preFix, freqItemList):
    bigL = [v[0] for v in sorted(headerTable.items(), key=lambda p : p[0])]
    for basePat in bigL:
        newFreqSet = preFix.copy()
        newFreqSet.add(basePat)
        freqItemList.append(newFreqSet)
        condPattBases = findPrefixPath(basePat, headerTable[basePat][1])
        myCondTree, myHead = createTree(condPattBases, minSup)

        if myHead != None:
            # print('conditional tree for :', newFreqSet)
            # myCondTree.disp()

            mineTree(myCondTree, myHead, minSup, newFreqSet, freqItemList)

#实现封装
def fpGrowth(dataSet, minSup):
    initSet = createInitSet(dataSet)
    del dataSet
    myFPtree, myHeaderTab = createTree(initSet, minSup)
    freqItems = []
    mineTree(myFPtree, myHeaderTab, minSup, set([]), freqItems)
    return freqItems