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
from operator import add
import shutil
from helper import parseRecord, getCDFList, FIELD, HO_TYPE, PROC_ID, STATUS, resetDirectories

def generateBS2Data(line):
    
    fields = line.split(";")
    bs = []
    if len(fields[FIELD.CUR_CELL-1])>0:
        curBS = int(fields[FIELD.CUR_CELL-1][0:13].split(":")[-1],16)
        bs.append((str(curBS),1))
    if len(fields[FIELD.PREV_CELL-1])>0:
        prevBS = int(fields[FIELD.PREV_CELL-1][0:13].split(":")[-1],16)
        bs.append((str(prevBS),1))
    if len(fields[FIELD.SOURCE_CELL-1])>0:
        sourceBS = int(fields[FIELD.SOURCE_CELL-1][0:13].split(":")[-1],16)
        bs.append((str(sourceBS),1))
    if len(fields[FIELD.TARGET_CELL-1])>0:
        targetBS = int(fields[FIELD.TARGET_CELL-1][0:13].split(":")[-1],16)
        bs.append((str(targetBS),1))
    
    return bs
    
    
def reduceBS2Data(x,y):
    return x

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: getLoad <numCores> <file directory> <file partition size>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    input_dir = sys.argv[2]
    filePartitionSize = int(sys.argv[3])

    inputFiles = []
    for fname in os.listdir(input_dir):
        if fname.find('MMEpcmd') >= 0:
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

    eNodeBs = set()

    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])
    for i in range(len(subDirs)):
        d = subDirs[i]
        bs2data = sc.textFile(input_dir + d + '*.gz').flatMap(generateBS2Data).reduceByKey(reduceBS2Data).collect()
        for b in bs2data:
            eNodeBs.add(b[0])
    resetDirectories(subDirs,input_dir)

    for b in eNodeBs:
        print b
    
                



