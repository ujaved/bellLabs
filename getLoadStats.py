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

def getAccumLoad(x):
    global timeLoads
    global intervals #sorted list

    bs = x[0]
    imsi2time2records = x[1]

    #times above correspond to the timeLoads array
    #the idea here is decide whether imsi should be added to the time intervals or not
    for imsi in imsi2time2records:
         v = [0]*len(intervals)
         if bs in bs2imsi2wasActivePrevTime and imsi in bs2imsi2wasActivePrevTime[bs]:
              wasActiveInPrevTime = bs2imsi2wasActivePrevTime[bs][imsi]
         else:
              wasActiveInPrevTime = False
         for i in range(intervalBoundary[0],intervalBoundary[1]+1):
              t = intervals[i]
              if t in imsi2time2records[imsi]:
                   v[i] = 1
                   #recs = sorted(imsi2time2records[imsi][t])
                   procID = imsi2time2records[imsi][t][1]
                   if procID==PROC_ID.CTX_RELEASE_REQ or procID==PROC_ID.DEL_BEARER_REQ:
                        wasActiveInPrevTime = False
                   else:
                        wasActiveInPrevTime = True
              else:
                   if (wasActiveInPrevTime):
                        v[i] = 1
         #v is formed; now add v to timeLoads
         timeLoads += v
         #time to write to a file
         #prevTimeFile.write(bs + " " + imsi + " " + str(wasActiveInPrevTime) + "\n")
         bs2imsi2wasActivePrevTime[bs][imsi] = wasActiveInPrevTime
            
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

    #return (curBS,{imsi: {T: [(startTime,procID)]}})
    return (curBS,{imsi: {T: (startTime,procID)}})


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
                    #res[k][c] = res[k][c] + y2[c]
                    #just need to keep the later time
                    if y2[c][0] >= res[k][c][0]:
                         res[k][c] = y2[c]
                     
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
    timeLoads = sc.accumulator([0]*len(intervals), VectorAccumulatorParam())

    bs2imsi2time2rec = defaultdict(lambda: defaultdict(lambda: defaultdict()))
    for i in range(len(subDirs)):
         d = subDirs[i]
         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2IMSI2Data).collect()
         for b in bs2data:
              bs = b[0]
              if bs in bs2imsi2time2rec:
                   for imsi in b[1]:
                        if imsi in bs2imsi2time2rec[bs]:
                             for t in b[1][imsi]:
                                  bs2imsi2time2rec[bs][imsi][t] = b[1][imsi][t]
                        else:
                             bs2imsi2time2rec[bs][imsi] = b[1][imsi]
              else:
                   bs2imsi2time2rec[bs] = b[1]

    for bs in bs2imsi2time2rec:
         print bs + " ------------------------- "
         for imsi in bs2imsi2time2rec[bs]:
              print imsi + " ****************** "
              for t in sorted(bs2imsi2time2rec[bs][imsi]):
                   print str(t) + " " + str(bs2imsi2time2rec[bs][imsi][t])

    resetDirectories(subDirs,input_dir)
    sys.exit()
    '''
    a = bs2data.first()
    for imsi in a[1]:
         print "imsi: " + imsi
         print "--------------"
         for t in sorted(a[1][imsi]):
              print str(t) + " " + str(a[1][imsi][t])
              #print "---------------"
              #for r in sorted(a[1][imsi][t]):
              #     print r
    sys.exit()
    '''    
    mean = [float(x)/numBS for x in timeLoads.value]
    print mean

    resetDirectories(subDirs,input_dir)
                



