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
import shutil
import math
from pyspark.accumulators import AccumulatorParam
from helper import parseRecord, getCDFList, FIELD, resetDirectories
            
def generateBS2Data(line):
    
    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    SGW = fields[FIELD.SGW_ID-1]
    return (curBS,{SGW:True})


def filterData(line):

    fields = line.split(";")
    curBS = fields[FIELD.CUR_CELL-1][0:13]
    sgw = fields[FIELD.SGW_ID-1]
    return len(curBS)>0 and len(sgw)>0

def reduceBS2Data(x,y):
    for s in y:
        if s not in x:
            x[s] = True
    return x
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: getLoad <numCores> <file directory>"
        exit(-1)

    sys.stdout = open('o.txt', 'w')
    numCores = sys.argv[1]
    input_dir = sys.argv[2]
    
    sc = SparkContext("local[" + numCores + "]" , "job", pyFiles=[realpath('helper.py')])

    bs2sgw = dict()
    bs2data = sc.textFile(input_dir + '*.gz').filter(filterData).map(generateBS2Data).reduceByKey(reduceBS2Data).collect()
    for b in bs2data:
        if len(b[1])>0:
            print b
                



