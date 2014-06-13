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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getGMTTime, resetDirectories, VectorAccumulatorParamTriple, \
    getSD, getDistFromGPS, isFailureRecord, getRollingAve

def getBearerLoad(x):
    global intervals 
    global eNodeBs
    global eNodeBLoadVec

    bs = x[0]
    bs_dec = int(bs.split(":")[-1],16)
    idx = eNodeBs[bs_dec][0]
    imsi2time2recs = x[1] 

    v = [(0,0,0)]*len(intervals)
    for imsi in imsi2time2recs:
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2recs[imsi]:
                   recs = sorted(imsi2time2recs[imsi][t])
                   numCtxReleases = 0
                   numRecords = 0
                   numFailures = 0
                   for r in recs:
                       if isFailureRecord(r[1]):
                           numRecords += 1
                           if r[2]>1:
                               numFailures += 1
                       if r[1]==PROC_ID.CTX_RELEASE_REQ or r[1]==PROC_ID.DEL_BEARER_REQ:
                           numCtxReleases += 1
                       
                   v[i] = (v[i][0]+numCtxReleases+1,v[i][1]+numRecords,v[i][2]+numFailures)
    eNodeBLoadVec[idx] += v
            
def generateBS2Data(line):
    
    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])
    cfc = int(fields[FIELD.PROC_CFC-1])

    global intervals
    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break
    assert(T!=0)

    return (curBS,{imsi: {T: [(startTime,procID,cfc)]}})


def filterData(line):
    
    global eNodeBs 
    global intervals
    
    fields = line.split(";")
    time = int(fields[FIELD.START_TIME-1])
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    if len(curBS)==0 or time >= max(intervals):
         return False
    curBS_dec = int(curBS.split(":")[-1],16)
    return (curBS_dec in eNodeBs) 

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
    if len(sys.argv) < 8:
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (min)>  <file directory> <gps coordinate file> <startTime (hh:mm)> <endTime (hh:mm)> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])
    input_dir = sys.argv[3]
    coordinateFile = sys.argv[4]  #in csv format
    startTime_input =  sys.argv[5] #local nyc time, format: hh:mm
    endTime_input =  sys.argv[6] #local nyc time
    filePartitionSize = int(sys.argv[7])
    if filePartitionSize%loadInterval != 0:
         print >> sys.stderr, "file partition size has to be divisible by the load interval. Exiting"
         exit(-1)

    startTime = (int(startTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(startTime_input.split(":")[1])*60*1000
    endTime = (int(endTime_input.split(":")[0])+4)*60*60*1000 + \
                 int(endTime_input.split(":")[1])*60*1000

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

    origin = (40.756,-73.846)
    #read in the coordinate file
    eNodeBs = dict()
    coord_f = open(coordinateFile,'r')
    isHeader = True
    idx = 0
    for line in coord_f:
         if (isHeader):
              isHeader = False
              continue
         bs = int(line.split(",")[3])
         lat = float(line.split(",")[1])
         lon = float(line.split(",")[2])
         dist = getDistFromGPS(lat,lon,origin[0],origin[1])
         desc = line.rstrip().split(",")[4]
         eNodeBs[bs] = (idx,desc,dist)
         idx += 1

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
    eNodeBLoadVec = []
    for bs in eNodeBs:
         v = sc.accumulator([(0,0,0)]*len(intervals), VectorAccumulatorParamTriple())
         eNodeBLoadVec.append(v)

    prev_idx = 0
    for i in range(len(subDirs)):
         d = subDirs[i]
         end_idx = intervals.index(dirTimeBoundaries[i])
         intervalBoundary = (prev_idx+1,end_idx) #both indexes are included
         prev_idx = end_idx

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data)
         bs2data.foreach(getBearerLoad)
     
    resetDirectories(subDirs,input_dir) 

    header = "time "
    V = []
    for bs in eNodeBs:
         idx = eNodeBs[bs][0]
         desc = eNodeBs[bs][1]
         dist = "%.2f" % eNodeBs[bs][2]
         v = eNodeBLoadVec[idx]
         header += (str(i) + " " + desc + "(" + str(dist) + ") ")
         #header += (str(i) + " " + dist + " ")
         V.append(v.value)

    print header
    rolling = defaultdict(list)
    for i in range(1,len(intervals)):
        t = (intervals[i]-intervals[0])/(1000*60)
        d = str(t) + " "
        j = 0
        for v in V:
            bearerLoad = v[i][0]
            if v[i][1]>0:
                failureRate = float(v[i][2])/v[i][1]
            else:
                failureRate = 0.0
            rolling[j].append(failureRate)
            f3 = getRollingAve(rolling[j],3)
            f5 = getRollingAve(rolling[j],5)
            #failureRate = "%.4f" % failureRate
            #d += (str(bearerLoad) + " " + str(failureRate) + " ")
            d += (str(f3) + " " + str(f5) + " ")
            j += 1
        print d
