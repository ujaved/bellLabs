#!/usr/bin/python 
import sys
sys.path.append("../")
import os
import logging
import logging.handlers
import resource
import time
import argparse
import subprocess, threading
from collections import defaultdict

from helper import parseRecord, getCDFList

field_curCell = 21
field_prevCell = 41
field_IMSI = 18
field_serviceInitNum = 10
field_recordSeqNum = 9
field_startTime = 8
field_seqNum = 9
field_procID = 7
field_procDuration = 11
field_procErrorCode = 13
field_sourceCell = 131
field_targetCell = 132
field_mobilityTrigger = 137
field_intraBSHOStartTime = 101
field_intraBSHOStatus = 102
field_interBSX2HOStartTime = 103
field_interBSX2HOStatus = 104
field_interBSS1HOStartTime = 105
field_interBSS1HOStatus = 106
field_intraCellHOStartTime = 107
field_eNodeBStopCollectionTime = 87

HO_INTRABS = 0
HO_INTERBS_X2 = 1
HO_INTERBS_S1 = 2
HO_CSFB = 3

def main():

	inputFiles = []

	for fname in os.listdir('.'):
		if os.path.isfile(fname):
			if fname.find('MMEpcmd') >= 0:
				inputFiles.append(fname)

	inputFiles = sorted(inputFiles)

        imsi2records = defaultdict(list)
        handoffType2durations = defaultdict(list)
        numRecords_same = 0
        numRecords_diff = 0
        numRecords_Mob = 0
         
        numHandoffs = 0
        numFailed = 0
	numCSFB = 0
	numIntraCell = 0 
        
	for fname in inputFiles:
		print fname
		f = open(fname)
		for line in f:
			fields = line.rstrip().split(";")
			curCell = fields[field_curCell-1]
			prevCell = fields[field_prevCell-1]
                        imsi = fields[field_IMSI-1]
                        time = int(fields[field_startTime-1])
                        procID = int(fields[field_procID-1])
			procDuration = int(fields[field_procDuration-1])
			procErrorCode = int(fields[field_procErrorCode-1])
                        sourceCell = fields[field_sourceCell-1]
                        targetCell = fields[field_targetCell-1]

			'''
                        if len(fields[field_intraCellHOStartTime-1])>0:
				numIntraCell = numIntraCell + 1
				print parseRecord(line)
			'''
                        if len(fields[field_mobilityTrigger-1]) == 0:
                            continue
			mobilityTrigger = int(fields[field_mobilityTrigger-1])
			if mobilityTrigger==3 and procID==36: 
				if procErrorCode != 100:
					continue
				numCSFB = numCSFB + 1
				'''
				handoffStartTime = int(fields[field_startTime-1])
				handoffEndTime = int(fields[field_eNodeBStopCollectionTime-1])
				handoffDuration = handoffEndTime-handoffStartTime
				handoffType2durations[HO_CSFB].append(handoffDuration)
				'''
				handoffType2durations[HO_CSFB].append(procDuration)
				
			if len(sourceCell)>0 and len(targetCell)>0:
				#this does not include circuit switch fallback; CSFB does not have both fields populated
                        
                        #since we have a mobility trigger, source cell and target cell ought to be different
				sourceBS = sourceCell[0:13]
				targetBS = targetCell[0:13]
				if sourceBS==targetBS:
                            #this is an intraBS handoff
					if len(fields[field_intraBSHOStatus-1])>0:
						handoffStatus = int(fields[field_intraBSHOStatus-1])
						if handoffStatus!=0:
							continue
                            #successfull handoff
						handoffStartTime = int(fields[field_intraBSHOStartTime-1])
						handoffEndTime = int(fields[field_eNodeBStopCollectionTime-1])
						handoffDuration = handoffEndTime-handoffStartTime
						handoffType2durations[HO_INTRABS].append(handoffDuration)
				else:
					# this is an inter-BS handoff
					if len(fields[field_interBSX2HOStatus-1])>0:
						#this is an X2 handover
						handoffStatus = int(fields[field_interBSX2HOStatus-1])
						if handoffStatus!=0:
							continue
						handoffStartTime = int(fields[field_interBSX2HOStartTime-1])
						handoffEndTime = int(fields[field_eNodeBStopCollectionTime-1])
						handoffDuration = handoffEndTime-handoffStartTime
						handoffType2durations[HO_INTERBS_X2].append(handoffDuration)

					if len(fields[field_interBSS1HOStatus-1])>0:
						#this is an X2 handover
						handoffStatus = int(fields[field_interBSS1HOStatus-1])
						if handoffStatus!=0:
							numFailed = numFailed + 1 
							continue
						numHandoffs = numHandoffs + 1
						handoffStartTime = int(fields[field_interBSS1HOStartTime-1])
						handoffEndTime = int(fields[field_eNodeBStopCollectionTime-1])
						handoffDuration = handoffEndTime-handoffStartTime
						handoffType2durations[HO_INTERBS_S1].append(handoffDuration)
						
			r = (time,line)
                        imsi2records[imsi].append(r)

	'''
        print numHandoffs
        print len(handoffType2durations[HO_INTRABS])
        print numFailed
	'''
	
	cdf_s1 = getCDFList(handoffType2durations[HO_INTERBS_S1])
	cdf_x2 = getCDFList(handoffType2durations[HO_INTERBS_X2])
	cdf_intra = getCDFList(handoffType2durations[HO_INTRABS])
	cdf_csfb = getCDFList(handoffType2durations[HO_CSFB])

	'''
	A = [cdf_s1, cdf_x2, cdf_intra, cdf_csfb]
	idx = [0,0,0,0]
	isDone = [False,False,False,False]
	numDone = 0
	while (True):
		p = "" 
		for i in range(len(idx)):
			if (isDone[i]):
				p += " "
				continue
			j = idx[i]
			if len(A[i])==j:
				isDone[i] = True
				p += " "
				numDone += 1
				continue
			p += (str(A[i][j]) + " ")
			idx[i] += 1
		print p
		if numDone==len(isDone):
			break
	'''	
			
	print len(handoffType2durations[HO_INTRABS])
	print len(handoffType2durations[HO_INTERBS_X2])
	print len(handoffType2durations[HO_INTERBS_S1])
	print len(handoffType2durations[HO_CSFB])

	f_intra = open("intra.txt",'w')
	f_x2 = open("x2.txt",'w')
	f_s1 = open("s1.txt",'w')
	f_csfb = open("csfb.txt",'w')
	for a in cdf_csfb:
		f_csfb.write(str(a[0]) + " " + str(a[1]) + "\n")
	for a in cdf_intra:
		f_intra.write(str(a[0]) + " " + str(a[1]) + "\n")
	for a in cdf_x2:
		f_x2.write(str(a[0]) + " " + str(a[1]) + "\n")
	for a in cdf_s1:
		f_s1.write(str(a[0]) + " " + str(a[1]) + "\n")

        '''
        for imsi in imsi2records:
            for r in sorted(imsi2records[imsi]):
                print parseRecord(r[1])
            print "------------------------"
        '''

if __name__ == '__main__':
	sys.exit(main())

'''
                        if curCell != prevCell:
                            if (len(sourceCell)==0 and len(targetCell)>0) or (len(sourceCell)>0 and len(targetCell)==0):
                                print parseRecord(line)
                        continue
                        if curCell == prevCell:
                            if sourceCell != targetCell and len(sourceCell)>0 and len(targetCell)>0: 
                                print parseRecord(line)
                                #sys.exit()
                                numRecords_same = numRecords_same + 1
                        else:
                            numRecords_diff = numRecords_diff + 1
                        '''
    
