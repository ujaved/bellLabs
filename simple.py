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
from helper import parseRecord, getCDFList, FIELD, HO_TYPE

def MobFilter(line):
    fields = line.split(";")
    if len(fields[FIELD.MOB_TRIGGER-1])==0:
        return False

    mobTrigger = int(fields[FIELD.MOB_TRIGGER-1])
    procID = int(fields[FIELD.PROC_ID-1])
    procErrorCode = int(fields[FIELD.PROC_ERROR_CODE-1])
    sourceCell = fields[FIELD.SOURCE_CELL-1]
    targetCell = fields[FIELD.TARGET_CELL-1]


    if mobTrigger==3 and procID==36 and procErrorCode!=100:
        return False #csfb handoffs that we don't want

    if len(sourceCell)>0 and len(targetCell)>0:
        sourceBS = sourceCell[0:13]
        targetBS = targetCell[0:13]
        if (sourceBS==targetBS) and (len(fields[FIELD.INTRABS_HO_STATUS-1])>0) and (int(fields[FIELD.INTRABS_HO_STATUS-1])!=0):
            return False
        if (sourceBS!=targetBS) and (len(fields[FIELD.INTERBS_X2_HO_STATUS-1])>0) and (int(fields[FIELD.INTERBS_X2_HO_STATUS-1])!=0):
            return False
        if (sourceBS!=targetBS) and (len(fields[FIELD.INTERBS_S1_HO_STATUS-1])>0) and (int(fields[FIELD.INTERBS_S1_HO_STATUS-1])!=0):
            return False
        
    return True
    
sys.stdout = open('o.txt', 'w')

inputFiles = []
for fname in os.listdir('.'):
    if os.path.isfile(fname):
        if fname.find('MMEpcmd') >= 0:
            inputFiles.append(fname)

inputFiles = sorted(inputFiles)

sc = SparkContext("local[16]", "job", pyFiles=[realpath('helper.py')])
RDDs = []
for f in inputFiles:
    #mobilityRecords = sc.textFile(f).filter(lambda x: x.split(";")[FIELD.MOB_TRIGGER-1] != "")
    mobilityRecords = sc.textFile(f).filter(MobFilter).cache()
    
