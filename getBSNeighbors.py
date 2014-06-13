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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, getGMTTime, resetDirectories, mobilityFilter
            
def generateBS2Data(line):

    fields = line.split(";")
    sourceBS = fields[FIELD.SOURCE_CELL-1][0:13]
    targetBS = fields[FIELD.TARGET_CELL-1][0:13]

    return [(sourceBS,{targetBS: True}),(targetBS,{sourceBS: True})]

def reduceBS2BS(x,y):

    #x,y are dictionaries; merge them
    res = x
    for k in y:
        if k not in res:
            res[k] = True

    return res
    

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print >> sys.stderr, "Usage: getLoad <numCores> <file directory> <startTime (hh:mm)> <endTime (hh:mm)> <file partition size>"
        exit(-1)

    sys.stdout = open('neighbors.txt', 'w')
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

    bs2neighbors = defaultdict(set)

    for i in range(len(subDirs)):
         d = subDirs[i]
         bs2data = sc.textFile(input_dir + d + '*.gz').filter(mobilityFilter).flatMap(generateBS2Data).reduceByKey(reduceBS2BS).collect()
         for b in bs2data:
             bs = b[0]
             for n in b[1]:
                 bs2neighbors[bs].add(n)
             
         
    for b in bs2neighbors:
        output_str = b + " "
        for n in bs2neighbors[b]:
            output_str += (n + " ")
        print output_str

    resetDirectories(subDirs,input_dir)



