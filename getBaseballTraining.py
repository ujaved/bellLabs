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
    global bs2time2stats
    global bs2time2imsis
    
    for imsi in imsi2time2recs:
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2recs[imsi]:
                  bs2time2imsis[bs][t].add(imsi)
                  recs = sorted(imsi2time2recs[imsi][t])
                  numCtxReleases = 0
                  inFlow = 0
                  outFlow = 0
                  for r in recs:
                      if r[1]==PROC_ID.CTX_RELEASE_REQ or r[1]==PROC_ID.DEL_BEARER_REQ:
                          numCtxReleases += 1
                      if len(r[4])>0 and len(r[5])>0:
                          sourceBS = r[4]
                          targetBS = r[5]
                          if sourceBS==bs:
                              outFlow += 1
                          if targetBS==bs:
                              inFlow += 1

                  #update bs2time2Stats
                  if len(bs2time2stats[bs][t])==0:
                      #stats for the imsi
                      bs2time2stats[bs][t] = [numCtxReleases+1,inFlow,outFlow]
                  else:
                      bs2time2stats[bs][t][0] += (numCtxReleases+1)
                      bs2time2stats[bs][t][1] += inFlow
                      bs2time2stats[bs][t][2] += outFlow
            
def generateBS2Data(line):

    global eNodeBs

    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    sourceBS = fields[FIELD.SOURCE_CELL-1][0:13]
    targetBS = fields[FIELD.TARGET_CELL-1][0:13]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])
    errorCode = int(fields[FIELD.PROC_ERROR_CODE-1])
    secErrorCode = fields[FIELD.PROC_SEC_ERROR_CODE-1]
    sourceBS = fields[FIELD.SOURCE_CELL-1][0:13]
    targetBS = fields[FIELD.TARGET_CELL-1][0:13]

    global intervals
    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break
    assert(T!=0)

    ret = []

    if len(sourceBS)>0 and len(targetBS)>0:
        #this is a handoff record; at least one is in eNodeBs
        sourceBS_dec = int(sourceBS.split(":")[-1],16)
        targetBS_dec = int(targetBS.split(":")[-1],16)

        if sourceBS_dec in eNodeBs:
            ret.append((sourceBS,{imsi: {T: [(startTime,procID,errorCode,secErrorCode,sourceBS,targetBS)]}}))
        if targetBS_dec in eNodeBs:
            ret.append((targetBS,{imsi: {T: [(startTime,procID,errorCode,secErrorCode,sourceBS,targetBS)]}}))

    else:
        ret.append((curBS,{imsi: {T: [(startTime,procID,errorCode,secErrorCode,sourceBS,targetBS)]}}))

    return ret

def filterData(line):
    
    global eNodeBs 
    global intervals
    
    fields = line.split(";")
    time = int(fields[FIELD.START_TIME-1])
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    sourceBS = fields[FIELD.SOURCE_CELL-1][0:13]
    targetBS = fields[FIELD.TARGET_CELL-1][0:13]

    if time >= max(intervals):
        return False

    if len(sourceBS)>0 and len(targetBS)>0:
        #at least one of these has to be in eNodeB
        sourceBS_dec = int(sourceBS.split(":")[-1],16)
        targetBS_dec = int(targetBS.split(":")[-1],16)
        
        if (sourceBS_dec not in eNodeBs) and (targetBS_dec not in eNodeBs):
            return False
        if sourceBS_dec==targetBS_dec:
            return False
    else:
        if len(curBS)==0:
            return False
        curBS_dec = int(curBS.split(":")[-1],16)
        if curBS_dec not in eNodeBs:
            return False

    return True

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
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (min)> <file directory> <eNodeB group file> <startTime (hh:mm)> <endTime (hh:mm)> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])
    input_dir = sys.argv[3]
    eNodeBGroup_f = sys.argv[4]
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

    eNodeBs = dict()
    idx = 0
    for line in open(eNodeBGroup_f,'r'):
         bs = int(line.rstrip())
         eNodeBs[bs] = idx
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

    bs2time2stats = defaultdict(lambda: defaultdict(list))
    bs2time2imsis = defaultdict(lambda: defaultdict(set))

    prev_idx = 0
    for i in range(len(subDirs)):
         d = subDirs[i]
         end_idx = intervals.index(dirTimeBoundaries[i])
         intervalBoundary = (prev_idx+1,end_idx) #both indexes are included
         prev_idx = end_idx

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).flatMap(generateBS2Data).reduceByKey(reduceBS2IMSI2Data).collect()
         for b in bs2data:
             getBSStats(b[0],b[1])
             
         '''
         a = bs2data.first()
         print a[0]
         print int(a[0].split(":")[-1],16)
         for imsi in a[1]:
              print "imsi: " + imsi
              print "--------------"
              for t in sorted(a[1][imsi]):
                   print t
                   print "********************"
                   for r in sorted(a[1][imsi][t]):
                        print r
         '''

    for b in bs2time2stats:
        print b
        print int(b.split(":")[-1],16)
        print "----------"
        prev_t = 0
        for t in sorted(bs2time2stats[b]):
            if prev_t==0:
                a = 0
            else:
                a = len(bs2time2imsis[b][t]-bs2time2imsis[b][prev_t])
            print str(t) + " " + str(bs2time2stats[b][t]) + " " + str(a)
            prev_t = t
        

    resetDirectories(subDirs,input_dir)
    sys.exit()

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
         if i==30:
             break
         v = eNodeBLoadVec[b[2]]
         desc = b[1]
         dist = "%.2f" % b[3]
         #header += (str(i) + " " + desc + "(" + str(dist) + ") ")
         header += (str(i) + " " + dist + " ")
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



