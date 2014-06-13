#!/usr/bin/python 
from pyspark import SparkContext
import sys
#sys.path.append("../")
import os
import logging
import logging.handlers
import resource
import time
import subprocess, threading
from collections import defaultdict
from os.path import realpath
import cProfile
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getLoad, getNeighborGradients

def generateBS2Data(line):
    
    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])

    global intervals
    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break
    assert(T!=0)

    return (curBS,{T: {imsi: [(startTime,procID)]}})

def filterData(line):
    
    fields = line.split(";")
    time = int(fields[FIELD.START_TIME-1])
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    return time < max(intervals) and len(curBS)>0

def reduceBS2IMSI2Data(x,y):

    #x,y are dictionaries of dictionaries; merge them
    res = x
    for k in y:
        if k in res:
            y2 = y[k]
            for c in y2:
                if c in res[k]:
                    #the '+' operator append the two lists; python construct 
                    res[k][c] = res[k][c] + y2[c]
                else:
                    res[k][c] = y2[c]
        else:
            res[k] = y[k]

    return res

def U(bs2sata,bs2time2activeUEs,intervals,bs2prevTimeStats):
    for x in bs2data:
        getLoad(x[0],x[1],bs2time2activeUEs,intervals,bs2prevTimeStats)
    
def reduceBS2Data(x,y):

    #x,y are dictionaries, each key pointing to a set
    res = x
    for k in y:
        if k in res:
            res[k] = res[k].union(y[k])
        else:
            res[k] = y[k]
    return res

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (sec)> <neighbor list file>"
        exit(-1)

    sys.stdout = open('p.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])
    neighbors_f = sys.argv[3]

    global bs2neighbors
    bs2neighbors = defaultdict(set)
    for line in open(neighbors_f, 'r'):
        line = line.rstrip().split()
        bs2neighbors[line[0]] = set(line[1:])
        
    global prevTime
    prevTime = 0

    inputFiles = []
    for fname in os.listdir('.'):
        if os.path.isfile(fname):
            if fname.find('MMEpcmd') >= 0:
                inputFiles.append(fname)

    inputFiles = sorted(inputFiles)
    startTime = (int(inputFiles[0].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 int(inputFiles[0].split(".")[1][3:])*60*1000  #start time past utc midnight in millisec
    endTime = (int(inputFiles[-1].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 (int(inputFiles[-1].split(".")[1][3:])+ 1)*60*1000  #end time past utc midnight in millisec

    global intervals
    intervals = []
    for t in range(startTime,endTime+1,loadInterval*1000):
        intervals.append(t)

    bs2time2activeUEs = defaultdict(lambda: defaultdict(set))

    bs2time2neighbor2flow = defaultdict(lambda: defaultdict(lambda: defaultdict()))

    bs2prevTimeStats = defaultdict()

    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])
    for i in range(len(inputFiles)):
        f = inputFiles[i]
        #if i==len(inputFiles)-1: #last file
        bs2data = sc.textFile(f).filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data).collect()

        cProfile.run('U(bs2data,bs2time2activeUEs,intervals,bs2prevTimeStats)')

    i = 0
    print len(bs2time2activeUEs)
    for bs in bs2time2activeUEs:
        cProfile.run('getNeighborGradients(bs,bs2neighbors[bs],bs2time2activeUEs,bs2time2neighbor2flow)')
        i += 1
        print i
        sys.stdout.flush()

    m = 0
    r = ['',0]
    for bs in bs2time2neighbor2flow:
        for t in sorted(bs2time2neighbor2flow[bs].keys()):
            inflow = 0
            for n in bs2time2neighbor2flow[bs][t]:
                v = len(bs2time2neighbor2flow[bs][t][n][0])-len(bs2time2neighbor2flow[bs][t][n][1])
                inflow += v
            if inflow >= m:
                m = inflow
                r = [bs,t]

    bs = r[0]
    for t in sorted(bs2time2neighbor2flow[bs].keys()):
        inflow = 0
        for n in bs2time2neighbor2flow[bs][t]:
            v = len(bs2time2neighbor2flow[bs][t][n][0])-len(bs2time2neighbor2flow[bs][t][n][1])
            inflow += v
        print str(t) + " " + str(inflow)   

    #for n in bs2time2neighbor2flow[r[0]][r[1]]:
    #    print n + " " + str(len(bs2time2neighbor2flow[r[0]][r[1]][n][0])) + " " + str(len(bs2time2neighbor2flow[r[0]][r[1]][n][1])) 
                



