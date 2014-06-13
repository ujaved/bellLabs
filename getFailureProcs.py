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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getGMTTime, resetDirectories
import operator

def mapProc(line):
    
    fields = line.split(";")
    proc = int(fields[FIELD.PROC_ID-1])

    return (proc,1)

def filterData(line):

    global eNodeBs
    fields = line.split(";")
    code = fields[FIELD.PROC_CFC-1]
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    if len(code)==0 or len(curBS)==0:
        return False
    code = int(code)
    curBS_dec = int(curBS.split(":")[-1],16)
    return (curBS_dec in eNodeBs)
    
if __name__ == "__main__":
    if len(sys.argv) < 6:
        print >> sys.stderr, "Usage: getLoad <numCores>  <file directory> <gps coordinate file> <startTime (hh:mm)> <endTime (hh:mm)> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    input_dir = sys.argv[2]
    coordinateFile = sys.argv[3]  #in csv format
    startTime_input =  sys.argv[4] #local nyc time, format: hh:mm
    endTime_input =  sys.argv[5] #local nyc time
    filePartitionSize = int(sys.argv[6])

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

    eNodeBs = dict()
    coord_f = open(coordinateFile,'r')
    isHeader = True
    for line in coord_f:
         if (isHeader):
              isHeader = False
              continue
         bs = int(line.split(",")[3])
         eNodeBs[bs] = True

    numPartitions = math.ceil(len(inputFiles)/float(filePartitionSize))

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

    proc2count = dict()
    for i in range(len(subDirs)):
         d = subDirs[i]
         proc2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).map(mapProc).countByKey()
         for p in proc2data:
             if p in proc2count:
                 proc2count[p] += proc2data[p]
             else:
                 proc2count[p] = proc2data[p]
     
    resetDirectories(subDirs,input_dir)
    for p in sorted(proc2count.iteritems(),key=operator.itemgetter(1),reverse=True):
        print str(p[0]) + " " + str(p[1])



