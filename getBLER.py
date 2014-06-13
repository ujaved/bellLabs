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
import shutil
import math
from pyspark.accumulators import AccumulatorParam
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getLoad, getNeighborGradients

class VectorAccumulatorParam(AccumulatorParam):
     def zero(self, value):
         return [0.0] * len(value)
     def addInPlace(self, val1, val2):
         for i in xrange(len(val1)):
              val1[i] += val2[i]
         return val1

def resetDirectories(subDirs,input_dir):
     for d in subDirs:
          for fname in os.listdir(input_dir+d):
              shutil.move(input_dir + d + fname, input_dir + fname)
          os.rmdir(input_dir + d)

def getAccumAvBLER(x):
    global timeAvBLER
    global intervals #sorted list
    global intervalBoundary #this is a pair specifying the start and end time indices of the interval

    bs = x[0]
    imsi2time2BLER = x[1]

    
    v = [0.0]*len(intervals)
    w = [0]*len(intervals)
    r = [0.0]*len(intervals)
    for imsi in imsi2time2BLER:
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2BLER[imsi]:
                   avBLER = float(imsi2time2BLER[imsi][t][0])/(100*imsi2time2BLER[imsi][t][1])
                   v[i] += avBLER
                   w[i] += 1
    for i in range(len(v)):
         if w[i]>0:
              r[i] = v[i]/w[i]

    timeAvBLER += r
            
def generateBS2Data(line):
    
    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    imsi = fields[FIELD.IMSI-1]
    startTime = int(fields[FIELD.START_TIME-1])
    procID = int(fields[FIELD.PROC_ID-1])

    numSamples = 0
    sum_BLER = 0
    if len(fields[FIELD.MAX_INIT_UL_BLER-1])>0:
         sum_BLER += int(fields[FIELD.MAX_INIT_UL_BLER-1])
         numSamples += 1
    if len(fields[FIELD.MAX_RES_UL_BLER-1])>0:
         sum_BLER += int(fields[FIELD.MAX_RES_UL_BLER-1])
         numSamples += 1
    
    #maxInitDLBLER = fields[FIELD.MAX_INIT_DL_BLER-1]
    #maxResDLBLER = fields[FIELD.MAX_RES_DL_BLER-1]

    global intervals
    T = 0
    for i in range(1,len(intervals)):
        t = intervals[i]
        if startTime <= t:
            T = t
            break
    assert(T!=0)

    #return (curBS,{imsi: {T: [(startTime,procID,maxInitULBLER,maxResULBLER,maxInitDLBLER,maxResDLBLER)]}})
    return (curBS,{imsi: {T: (sum_BLER,numSamples)}})


def filterData(line):
    
    fields = line.split(";")
    time = int(fields[FIELD.START_TIME-1])
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    maxInitULBLER = fields[FIELD.MAX_INIT_UL_BLER-1]
    maxResULBLER = fields[FIELD.MAX_RES_UL_BLER-1]
    return time < max(intervals) and len(curBS)>0 and \
        (len(maxInitULBLER)>0 or len(maxResULBLER)>0)  

def reduceBS2IMSI2Data(x,y):

     res = x
     for k in y:
          if k in res:
               y2 = y[k]
               for c in y2:
                    if c in res[k]:
                         res[k][c] = (res[k][c][0] + y2[c][0],res[k][c][1] + y2[c][1])
                    else:
                         res[k][c] = y2[c]
          else:
               res[k] = y[k]
     return res
    

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: getLoad <numCores> <interval (min)> <file directory> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    loadInterval = int(sys.argv[2])
    input_dir = sys.argv[3]
    filePartitionSize = int(sys.argv[4])
    if filePartitionSize%loadInterval != 0:
         print >> sys.stderr, "file partition size has to be divisible by the load interval. Exiting"
         exit(-1)

    inputFiles = []
    for fname in os.listdir(input_dir):
        if fname.find('MMEpcmd') >= 0:
            inputFiles.append(fname)

    inputFiles = sorted(inputFiles)
    startTime = (int(inputFiles[0].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 int(inputFiles[0].split(".")[1][3:])*60*1000  #start time past utc midnight in millisec
    endTime = (int(inputFiles[-1].split(".")[1][0:2]) + 4)*60*60*1000 + \
                 (int(inputFiles[-1].split(".")[1][3:])+ 1)*60*1000  #end time past utc midnight in millisec

    global intervals
    intervals = []
    for t in range(startTime,endTime+1,loadInterval*60*1000):
        intervals.append(t)
    intervals = sorted(intervals)

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
    timeAvBLER = sc.accumulator([0]*len(intervals), VectorAccumulatorParam())

    prev_idx = 0
    numBS = 0
    for i in range(len(subDirs)):
         d = subDirs[i]
         end_idx = intervals.index(dirTimeBoundaries[i])
         intervalBoundary = (prev_idx+1,end_idx) #both indexes are included
         prev_idx = end_idx

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data)
         bs2data.foreach(getAccumAvBLER)
         if (bs2data.count() >= numBS):
              numBS = bs2data.count()

    mean = [float(x)/numBS for x in timeAvBLER.value]
    for i in range(1,len(mean)):
         print str(loadInterval*i) + " " + str(mean[i])

    resetDirectories(subDirs,input_dir)
                



