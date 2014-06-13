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

def printRecord(line):
    print parseRecord(line)

def filterData(line):

    fields = line.split(";")
    code = fields[FIELD.PROC_CFC-1]
    if len(code)==0:
        return False
    code = int(code)
    return code>1
    '''
    if line.find('[')>=0:
        return True
    return False
    '''
    
if __name__ == "__main__":
    if len(sys.argv) < 6:
        print >> sys.stderr, "Usage: getLoad <numCores>  <file directory> <startTime (hh:mm)> <endTime (hh:mm)> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    input_dir = sys.argv[2]
    startTime_input =  sys.argv[3] #local nyc time, format: hh:mm
    endTime_input =  sys.argv[4] #local nyc time
    filePartitionSize = int(sys.argv[5])

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

    for i in range(len(subDirs)):
         d = subDirs[i]

         bs2data = sc.textFile(input_dir + d + '*.gz').filter(filterData).collect()
         for b in bs2data:
             print parseRecord(b)
         #bs2data.foreach(printRecord)
     
    resetDirectories(subDirs,input_dir)



