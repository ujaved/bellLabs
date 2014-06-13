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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, mobilityFilter, GLOBAL_VARS, getBS2BS, reduceBS2BS

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
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: getLoad <numCores>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]

    inputFiles = []
    for fname in os.listdir('.'):
        if os.path.isfile(fname):
            if fname.find('MMEpcmd') >= 0:
                inputFiles.append(fname)

    inputFiles = sorted(inputFiles)

    GLOBAL_VARS.WANT_INTRABS_HO = False
    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])
    BS2neigbors2count = defaultdict(lambda: defaultdict())
    for i in range(len(inputFiles)):
        f = inputFiles[i]
        BS2neighbors = sc.textFile(f).filter(mobilityFilter).map(getBS2BS).reduceByKey(reduceBS2BS).collect()
        for b in BS2neighbors:
            bs = b[0]
            nei_dict = b[1]
            if bs in BS2neigbors2count:
                for n in nei_dict:
                    if n in BS2neigbors2count[bs]:
                        BS2neigbors2count[bs][n] += nei_dict[n]
                    else:
                        BS2neigbors2count[bs][n] = nei_dict[n]
            else:
                BS2neigbors2count[bs] = nei_dict

    
    for b in BS2neigbors2count:
        output_str = b + " "
        for n in BS2neigbors2count[b]:
            output_str += (n + " ")
        print output_str







