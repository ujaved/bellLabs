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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, cellPrevTimeStats

def collectCellLoadInfo(x):
    cell = x[0]
    maxTime_idx = intervals.index(max(x[1].keys()))
    if cell in cell2prevTimeStats:
        prevTimeBoundary = cell2prevTimeStats[cell].prevTimeBoundary
    else:
        prevTimeBoundary = intervals[0]
    prevTimeBoundary_idx = intervals.index(prevTimeBoundary)

    if cell in cell2prevTimeStats:
        prevActiveUEs = cell2prevTimeStats[cell].prevActiveUEs 
    else:
        prevActiveUEs = set()
    for i in range(prevTimeBoundary_idx+1,maxTime_idx+1): #this is our time range
        t = intervals[i]
        if t not in x[1]:
            #no change in load
            activeUEs = prevActiveUEs
            cell2time2Load[cell][t] = len(prevActiveUEs)
        else:
            terminatedUEs = set()
            newUEs = set()
            for imsi in x[1][t]:
                recs = sorted(x[1][t][imsi])
                if recs[-1][1]==PROC_ID.CTX_RELEASE_REQ or recs[-1][1]==PROC_ID.DEL_BEARER_REQ:
                    terminatedUEs.add(imsi)
                if (imsi not in prevActiveUEs) and (imsi not in terminatedUEs):
                    newUEs.add(imsi)
            activeUEs = (prevActiveUEs-terminatedUEs).union(newUEs)
            cell2time2Load[cell][t] = len(activeUEs) + len(terminatedUEs)
        prevActiveUEs = activeUEs
    if cell not in cell2prevTimeStats:
        cell2prevTimeStats[cell] = cellPrevTimeStats()
    cell2prevTimeStats[cell].prevActiveUEs = prevActiveUEs
    cell2prevTimeStats[cell].prevTimeBoundary = t
        
def generateCell2Data(line):
    
    fields = line.split(";")
    curCell = fields[FIELD.CUR_CELL-1]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])

    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break

    if T==0:
        print line

    return (curCell,{T: {imsi: [(startTime,procID)]}})

def filterData(line):
    
    time = int(line.split(";")[FIELD.START_TIME-1])
    return time < max(intervals)
    
def ReduceCell2IMSI2Data(x,y):

    #x,y are dictionaries of dictionaries; merge them
    res = x
    for k in y:
        if k in res:
            y2 = y[k]
            for c in y2:
                if c in res[k]:
                    res[k][c] = res[k][c] + y2[c]
                else:
                    res[k][c] = y2[c]
        else:
            res[k] = y[k]

    return res


def printTuple(x):
    print x[0] + " " + str(len(x[1]))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (sec)>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])

    inputFiles = []
    for fname in os.listdir('.'):
        if os.path.isfile(fname):
            if fname.find('MMEpcmd') >= 0:
                inputFiles.append(fname)

    inputFiles = sorted(inputFiles)
    startTime = (int(inputFiles[0].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 int(inputFiles[0].split(".")[1][2:])*60*1000  #start time past utc midnight in millisec
    endTime = (int(inputFiles[-1].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 (int(inputFiles[-1].split(".")[1][2:])+ 1)*60*1000  #end time past utc midnight in millisec

    global intervals
    intervals = []
    for t in range(startTime,endTime+1,loadInterval*1000):
        intervals.append(t)

    global cell2time2Load
    cell2time2Load = defaultdict(lambda: defaultdict())

    global cell2prevTimeStats
    cell2prevTimeStats = defaultdict()

    prevRDD = None
    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])
    for i in range(len(inputFiles)):
        f = inputFiles[i]
        if i==len(inputFiles)-1: #last file
            cell2data = sc.textFile(f).filter(filterData).map(generateCell2Data).reduceByKey(ReduceCell2IMSI2Data)
        else:
            cell2data = sc.textFile(f).map(generateCell2Data).reduceByKey(ReduceCell2IMSI2Data)
        if prevRDD is None:
            prevRDD = cell2data
        else:
            #merge prevRDD and cell2data
        #cell2data.foreach(collectCellLoadInfo)
           
        g = cell2data.first()
        print g[0]
        for t in intervals:
            if t not in g[1]:
                continue
            print "time: " + str(t)
            print "--------------"
            for imsi in g[1][t]:
                print imsi
                for p in g[1][t][imsi]:
                    print p
        
        
        '''
        for k in g[1]:
            print "imsi: " + str(k)
            print "--------------"
            for p in g[1][k]:
                print parseRecord(p)
        '''

    #print len(cell2time2Load)
    







