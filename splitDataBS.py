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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getGMTTime, resetDirectories, VectorAccumulatorParamPair, \
    getSD, getDistFromGPS

def getBearerLoad(x):
    global intervals 
    global eNodeBs
    global eNodeBLoadVec

    bs = x[0]
    bs_dec = int(bs.split(":")[-1],16)
    idx = eNodeBs[bs_dec][0]
    imsi2time2recs = x[1] 

    v = [(0,0)]*len(intervals)
    for imsi in imsi2time2recs:
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2recs[imsi]:
                   #v[i] += 1 #at least one bearer
                   v[i] = (v[i][0]+1,v[i][1]+1)
                   recs = sorted(imsi2time2recs[imsi][t])
                   numCtxReleases = 0
                   for r in recs:
                        if r[1]==PROC_ID.CTX_RELEASE_REQ or r[1]==PROC_ID.DEL_BEARER_REQ:
                             numCtxReleases += 1
                   #v[i] += numCtxReleases
                   v[i] = (v[i][0]+numCtxReleases,v[i][1])
    eNodeBLoadVec[idx] += v
            
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
    if len(sys.argv) < 5:
        print >> sys.stderr, "Usage: getLoad <numCores> <file directory> <file partition size> <bs id file>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    input_dir = sys.argv[2]
    filePartitionSize = int(sys.argv[3])

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

    origin = (40.829,-73.926)
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
         v = sc.accumulator([(0,0)]*len(intervals), VectorAccumulatorParamPair())
         eNodeBLoadVec.append(v)

    prev_idx = 0
    for i in range(len(subDirs)):
         d = subDirs[i]
         end_idx = intervals.index(dirTimeBoundaries[i])
         intervalBoundary = (prev_idx+1,end_idx) #both indexes are included
         prev_idx = end_idx

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data)
         bs2data.foreach(getBearerLoad)
         '''
         a = bs2data.first()
         for imsi in a[1]:
              print "imsi: " + imsi
              print "--------------"
              for t in sorted(a[1][imsi]):
                   print t
                   print "********************"
                   for r in sorted(a[1][imsi][t]):
                        print r
         '''
     
    resetDirectories(subDirs,input_dir) 

    stddev = [(0.0,'')]*len(eNodeBLoadVec)
    for bs in eNodeBs:
         idx = eNodeBs[bs][0]
         desc = eNodeBs[bs][1]
         dist = eNodeBs[bs][2]
         sd = getSD(eNodeBLoadVec[idx].value[1:])
         stddev[idx] = (sd,desc,idx,dist)

    header = "time "
    V = []
    i = 0
    for b in sorted(stddev,reverse=True):
         if i==15:
             break
         v = eNodeBLoadVec[b[2]]
         desc = b[1]
         dist = b[3]
         header += (str(i) + " " + desc + "(" + str(dist) + ") ")
         V.append(v.value)
         i += 1

    print header
    for i in range(1,len(intervals)):
        t = (intervals[i]-intervals[0])/(1000*60)
        d = str(t) + " " 
        for v in V:
            bearerLoad = v[i][0]
            UELoad = v[i][1]
            d += (str(bearerLoad) + " " + str(UELoad) + " ")
        print d



