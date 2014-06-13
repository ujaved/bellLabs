#!/usr/bin/python 
from pyspark import SparkContext
import sys
import os
import logging
import logging.handlers
import resource
import time
import subprocess, threading
from collections import defaultdict
from os.path import realpath
import cProfile
import math
import shutil
from pyspark.accumulators import AccumulatorParam
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getGMTTime, resetDirectories, VectorAccumulatorParamVector, \
    getSD, getDistFromGPS

def getBSStats(bs,imsi2time2recs):
    
    global intervals
    global intervalBoundary
    global bs2time2load
    global bs2time2imsis
    global eNodeBs

    bs_dec = int(bs.split(":")[-1],16)

    for imsi in imsi2time2recs:
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2recs[imsi]:
                  bs2time2imsis[bs][t].add(imsi)
                  if bs_dec not in eNodeBs:
                      continue
                  recs = sorted(imsi2time2recs[imsi][t])
                  numCtxReleases = 0
                  for r in recs:
                      if r[1]==PROC_ID.CTX_RELEASE_REQ or r[1]==PROC_ID.DEL_BEARER_REQ:
                          numCtxReleases += 1

                  if t in bs2time2load[bs]:
                      bs2time2load[bs][t] += (numCtxReleases+1)
                  else:
                      bs2time2load[bs][t] = (numCtxReleases+1)
                  
            
def generateBS2Data(line):

    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])
    errorCode = int(fields[FIELD.PROC_ERROR_CODE-1])
    secErrorCode = fields[FIELD.PROC_SEC_ERROR_CODE-1]

    global intervals
    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break
    assert(T!=0)

    return (curBS,{imsi: {T: [(startTime,procID,errorCode,secErrorCode)]}})

def filterData(line):
    
    global intervals
    global relevantBS
    
    fields = line.split(";")
    time = int(fields[FIELD.START_TIME-1])
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    if len(curBS)==0 or time >= max(intervals):
         return False
    return (curBS in relevantBS)

def reduceBS2IMSI2Data(x,y):

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
    

if __name__ == "__main__":
    if len(sys.argv) < 11:
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (min)> <file directory> <eNodeB group file> <neighbor file> <startTime (hh:mm)> <endTime (hh:mm)>  <file partition size> <startPosTime (hh:mm)> <endPosTime (hh:mm)>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])
    input_dir = sys.argv[3]
    eNodeBGroup_f = sys.argv[4]
    neighbors_f = sys.argv[5]
    startTime_input =  sys.argv[6] #local nyc time, format: hh:mm
    endTime_input =  sys.argv[7] #local nyc time
    filePartitionSize = int(sys.argv[8])
    startPosTime_input = sys.argv[9]
    endPosTime_input = sys.argv[10]
    if filePartitionSize%loadInterval != 0:
         print >> sys.stderr, "file partition size has to be divisible by the load interval. Exiting"
         exit(-1)

    startTime = (int(startTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(startTime_input.split(":")[1])*60*1000
    endTime = (int(endTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(endTime_input.split(":")[1])*60*1000

    startPosTime = (int(startPosTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(startPosTime_input.split(":")[1])*60*1000
    endPosTime = (int(endPosTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(endPosTime_input.split(":")[1])*60*1000

    inputFiles = []
    for fname in os.listdir(input_dir):
        if fname.find('MMEpcmd') >= 0:
             t = getGMTTime(fname)
             if t >= startTime and t < endTime:
                  inputFiles.append(fname)
    inputFiles = sorted(inputFiles)

    global intervals
    intervals = []
    for t in range(startTime,endTime+1,loadInterval*60*1000):
        intervals.append(t)
    intervals = sorted(intervals)

    eNodeBs = dict()
    for line in open(eNodeBGroup_f,'r'):
         bs = int(line.rstrip())
         eNodeBs[bs] = True

    relevantBS = set()

    bs2neighbors = defaultdict(set)
    for line in open(neighbors_f,'r'):
        line = line.rstrip().split()
        bs = line[0]
        bs_dec = int(bs.split(":")[-1],16)
        if bs_dec not in eNodeBs:
            continue
        relevantBS.add(bs)
        for n in line[1:]:
            bs2neighbors[bs].add(n)
            relevantBS.add(n)

    numPartitions = math.ceil(len(inputFiles)/float(filePartitionSize))
    dirTimeBoundaries = []
    step = int(math.ceil((len(intervals)-1)/numPartitions))
    for i in range(step,len(intervals),step):
         dirTimeBoundaries.append(intervals[i])

    subDirs = []
    subDirNum = 0
    subFileCount = 0
    subDir = str(subDirNum) + "/"
    os.makedirs(input_dir + subDir)
    for i in range(len(inputFiles)):
         f = inputFiles[i]
         if subFileCount==filePartitionSize:
              subDirs.append(subDir)
              subDirNum += 1
              subFileCount = 0
              subDir = str(subDirNum) + "/"
              os.makedirs(input_dir + subDir)
         shutil.move(input_dir + f,input_dir + subDir)
         subFileCount += 1
    if subFileCount==filePartitionSize:
         subDirs.append(subDir)

    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])

    bs2time2load = defaultdict(lambda: defaultdict())
    bs2time2imsis = defaultdict(lambda: defaultdict(set))

    prev_idx = 0
    for i in range(len(subDirs)):
         d = subDirs[i]
         end_idx = intervals.index(dirTimeBoundaries[i])
         intervalBoundary = (prev_idx+1,end_idx) #both indexes are included
         prev_idx = end_idx

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data).collect()
         for b in bs2data:
             getBSStats(b[0],b[1])

    resetDirectories(subDirs,input_dir)

    time2bs2stats = defaultdict(lambda: defaultdict(list))
    PAST_SAMPLES = [3,5,10]
    NUM_FIELDS = 2

    cumBSLoad = [0.0]*len(intervals)
    for i in range(len(intervals[1:])):
        t = intervals[i+1]
        for bs in bs2time2load:
            if t in bs2time2load[bs]:
                cumBSLoad[i+1] += bs2time2load[bs][t]
    for i in range(len(cumBSLoad)):
        cumBSLoad[i] /= loadInterval
        t = intervals[i]
        time2bs2stats[t]['all'].append(cumBSLoad[i])

    cumBSFlow = [0.0]*len(intervals)
    for bs in bs2time2load:
        prev_t = intervals[0]
        for t in intervals[1:]:
            idx = intervals.index(t)
            if t in bs2time2load[bs]:
                load_rate = float(bs2time2load[bs][t])/loadInterval
            else:
                load_rate = 0.0
            time2bs2stats[t][bs].append(load_rate)
            inFlow = 0
            outFlow = 0
            cumInFlow = 0
            cumOutFlow = 0
            for i in bs2time2imsis[bs][t]:
                for n in bs2neighbors[bs]:
                    if i in bs2time2imsis[n][prev_t]:
                        inFlow += 1
                        n_dec = int(n.split(":")[-1],16)
                        if n_dec not in eNodeBs:
                            cumInFlow += 1

            for i in bs2time2imsis[bs][prev_t]:
                for n in bs2neighbors[bs]:
                    if i in bs2time2imsis[n][t]:
                        outFlow += 1
                        n_dec = int(n.split(":")[-1],16)
                        if n_dec not in eNodeBs:
                            cumOutFlow += 1
                        break
            inFlow = float(inFlow)/loadInterval
            outFlow = float(outFlow)/loadInterval
            time2bs2stats[t][bs].append(inFlow-outFlow)

            cumBSFlow[idx] += (cumInFlow-cumOutFlow)

            prev_t = t

    for i in range(len(cumBSFlow)):
        cumBSFlow[i] /= loadInterval
        t = intervals[i]
        time2bs2stats[t]['all'].append(cumBSFlow[i])

    for i in range(len(intervals)):
        t = intervals[i]
        for n in PAST_SAMPLES:
            if n>i:
                break
            for bs in time2bs2stats[t]:
                vec = []
                for j in range(NUM_FIELDS):
                    samples = [time2bs2stats[t][bs][j]]
                    for q in range(1,n):
                        past_time = intervals[i-1]
                        samples.append(time2bs2stats[past_time][bs][j])
                    avg = sum(samples)/len(samples)
                    vec.append(avg)
                for v in vec:
                    time2bs2stats[t][bs].append(v)

    bs2stats = dict()
    for bs in time2bs2stats[t]:
        bs2stats[bs] = [0.0]*len(time2bs2stats[t][bs])

    for t in sorted(time2bs2stats):
        if t < startPosTime or t > endPosTime:
            continue
        for bs in time2bs2stats[t]:
            for i in range(len(time2bs2stats[t][bs])):
                v = time2bs2stats[t][bs][i]
                bs2stats[bs][i] += (v/len(time2bs2stats))
                
    for bs in bs2stats:
        print bs + " " + str(bs2stats[bs])



