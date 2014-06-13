#!/usr/bin/python
import sqlite3
import sys
import os
import errno
import random
import gzip
import resource
import logging
import logging.handlers
import time
from optparse import OptionParser
import subprocess, threading
import re
import itertools
import copy
import bisect
import math
import shutil
from pyspark.accumulators import AccumulatorParam

class FIELD:
    CUR_CELL = 21
    PREV_CELL = 41
    IMSI = 18
    SERV_INIT_NUM = 10
    REC_SEQ_NUM = 9
    START_TIME = 8
    PROC_ID = 7
    PROC_DURATION = 11
    PROC_CFC = 12
    PROC_CFCQ = 13
    PROC_SCFCQ = 14
    SGW_ID = 38
    MAX_INIT_UL_BLER = 92
    MAX_RES_UL_BLER = 93
    MAX_INIT_DL_BLER = 143
    MAX_RES_DL_BLER = 144
    SOURCE_CELL = 131
    TARGET_CELL = 132
    MOB_TRIGGER = 137
    INTRABS_HO_START_TIME = 101
    INTRABS_HO_STATUS = 102
    INTERBS_X2_HO_START_TIME = 103
    INTERBS_X2_HO_STATUS = 104
    INTERBS_S1_HO_START_TIME = 105
    INTERBS_S1_HO_STATUS = 106
    ENODEB_STOPCOLLECTION_TIME = 87

class HO_TYPE:
    HO_INTRABS = 0
    HO_INTERBS_X2 = 1
    HO_INTERBS_S1 = 2
    HO_CSFB = 3

class PROC_ID:
    CTX_RELEASE_REQ = 11
    DEL_BEARER_REQ = 3
    HO_MSG = 45
    INIT_ATTACH = 1
    UE_INIT_SERVE_REQ = 16
    PAGING = 15

class STATUS:
    ACTIVE = 0
    IDLE = 1

class GLOBAL_VARS:
    WANT_INTRABS_HO = False

class cellPrevTimeStats(object):

    def __init__(self):
        self.prevTimeBoundary = 0
        self.prevActiveUEs = set()

def getRollingAve(V,windowLen):
    av = 0.0
    if len(V)>=windowLen:
        for i in range(1,windowLen+1):
            v = V[len(V)-i]
            av += v
        av /= windowLen
    return "%.4f" % av

def isFailureRecord(proc_id):
    if proc_id==PROC_ID.INIT_ATTACH or proc_id==PROC_ID.UE_INIT_SERVE_REQ \
            or proc_id==PROC_ID.PAGING:
        return True
    return False

def resetDirectories(subDirs,input_dir):
     for d in subDirs:
          for fname in os.listdir(input_dir+d):
              shutil.move(input_dir + d + fname, input_dir + fname)
          os.rmdir(input_dir + d)

def getSD(v):
    #return the standard deviation for the vector of doubles v;
    w = [0.0]*len(v)
    for i in range(len(w)):
        w[i] = v[i][0]
    mean = sum(w)/len(w)
    var = map(lambda x: (x - mean)**2, w)
    var = sum(var)/len(w)
    sd = math.sqrt(var)
    return sd

def getDistFromGPS(lat1,lon1,lat2,lon2):
    #return distance in km
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    km = 63.67 * c
    return km 

def getGMTTime(fname):
    #start time past utc midnight in millisec
    fields = fname.split(".")
    hours = (int(fields[1][0:2])+4)*60*60*1000
    mins = int(fields[1][3:])*60*1000

    return hours+mins

class VectorAccumulatorParamPair(AccumulatorParam):
     def zero(self, value):
         return [(0,0)] * len(value)
     def addInPlace(self, val1, val2):
         for i in xrange(len(val1)):
             val1[i] = (val1[i][0]+val2[i][0],val1[i][1]+val2[i][1])
         return val1

class VectorAccumulatorParamTriple(AccumulatorParam):
     def zero(self, value):
         return [(0,0,0)] * len(value)
     def addInPlace(self, val1, val2):
         for i in xrange(len(val1)):
             val1[i] = (val1[i][0]+val2[i][0],val1[i][1]+val2[i][1],val1[i][2]+val2[i][2])
         return val1

class VectorAccumulatorParamVector(AccumulatorParam):
     def zero(self, value):
         return [[0]*len(value[0])]* len(value)
     def addInPlace(self, val1, val2):
         for i in range(len(val1)):
             for j in range(len(val1[i])):
                 val1[i][j] = val1[i][j] + val2[i][j] 
         return val1

def getLoad(bs,time2UE2records,bs2time2activeUEs,intervals,bs2prevTimeStats):
    
    maxTime_idx = intervals.index(max(time2UE2records.keys())) #for a single RDD
    if bs in bs2prevTimeStats:
        prevTimeBoundary = bs2prevTimeStats[bs].prevTimeBoundary
        prevActiveUEs = bs2prevTimeStats[bs].prevActiveUEs
    else:
        prevTimeBoundary = intervals[0]
        prevActiveUEs = set()
    prevTimeBoundary_idx = intervals.index(prevTimeBoundary)

    #we are including prevTimeBoundary because of corner cases 
    for i in range(prevTimeBoundary_idx,maxTime_idx+1): #this is our time range
        t = intervals[i]
        if t not in time2UE2records:
            #no change in load
            activeUEs = prevActiveUEs
            bs2time2activeUEs[bs][t] = prevActiveUEs
        else:
            terminatedUEs = set()
            newUEs = set()
            for imsi in time2UE2records[t]:
                recs = sorted(time2UE2records[t][imsi])
                if recs[-1][1]==PROC_ID.CTX_RELEASE_REQ or recs[-1][1]==PROC_ID.DEL_BEARER_REQ:
                    terminatedUEs.add(imsi)
                if (imsi not in prevActiveUEs) and (imsi not in terminatedUEs):
                    newUEs.add(imsi)
            activeUEs = (prevActiveUEs-terminatedUEs).union(newUEs)
            bs2time2activeUEs[bs][t] = activeUEs.union(terminatedUEs)
        prevActiveUEs = activeUEs
        
    if bs not in bs2prevTimeStats:
        bs2prevTimeStats[bs] = cellPrevTimeStats()
    bs2prevTimeStats[bs].prevActiveUEs = prevActiveUEs
    bs2prevTimeStats[bs].prevTimeBoundary = t

def getNeighborGradients(bs,neighbors,bs2time2activeUEs,bs2time2neighbor2flow):

    times = sorted(bs2time2activeUEs[bs].keys())
    
    prevTime = times[0]
    for t in times[1:]:
        for n in neighbors:
            #n is an eNodeB
            #calculate how many UEs were connected to n at the previous timestep
            if n not in bs2time2activeUEs:
                continue
            newUEsFromNeighbor = bs2time2activeUEs[n][prevTime].intersection(bs2time2activeUEs[bs][t])
            oldUEsToNeighbor = bs2time2activeUEs[bs][prevTime].intersection(bs2time2activeUEs[n][t])
            bs2time2neighbor2flow[bs][t][n] = (newUEsFromNeighbor,oldUEsToNeighbor)
            '''
            if len(newUEsFromNeighbor)>0 or len(oldUEsToNeighbor)>0:
                print bs
                print n
                print t
                print bs2time2neighbor2flow[bs][t][n]
            '''
        prevTime = t
    

def parseRecord(line):
    fields = line.rstrip().split(";")
    s = ""
    for i in range(len(fields)):
        f = fields[i]
        s = s + str(i+1) + ": " + f + " "
    return s

def getCDFList(vector):
	#vector: an unsorted list of values
        #output: sorted list of (val, fraction) 
    v = sorted(vector)
    r = []
    cum = 0
    prev = v[0]
    for a in v:
        if a!=prev:
            r.append((prev,float(cum)/len(vector)))
        cum = cum + 1
        prev = a
    r.append((a,float(cum)/len(vector)))
    return r

def TEST():
    print GLOBAL_VARS.WANT_INTRABS_HO

def mobilityFilter(line):
    fields = line.split(";")
    sourceBS = fields[FIELD.SOURCE_CELL-1][0:13]
    targetBS = fields[FIELD.TARGET_CELL-1][0:13]

    if len(sourceBS)==0 or len(targetBS)==0 or sourceBS==targetBS:
        return False
        
    return True

def generateCell2Data(line):
    fields = line.split(";")
    curCell = fields[FIELD.CUR_CELL-1]
    imsi = fields[FIELD.IMSI-1]
    startTime = fields[FIELD.START_TIME-1]
    return (curCell,{imsi: [line]})

def getBS2BS(line):
    fields = line.split(";")
    sourceCell = fields[FIELD.SOURCE_CELL-1]
    targetCell = fields[FIELD.TARGET_CELL-1]
    sourceBS = sourceCell[0:13]
    targetBS = targetCell[0:13]
    #sourceBS and targetBS are different

    return (sourceBS,{targetBS: 1})

def reduceBS2BS(x,y):
    #x,y are dictionaries; merge them
    res = x
    for k in y:
        if k in res:
            res[k] += y[k]
        else:
            res[k] = 1
    return res

def ReduceCell2IMSI2Data(x,y):
    #x,y are dictionaries; merge them
    res = x
    for k in y:
        if k in res:
            res[k] = res[k] + y[k]
        else:
            res[k] = y[k]
    return res
    
