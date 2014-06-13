#!/usr/bin/python 
import sys
import os

def main():

	for fname in os.listdir('.'):
		if os.path.isfile(fname):
			if fname.find('MMEpcmd') >= 0:	
				col_idx = fname.index(".-") - 3
				a = list(fname)
				a[col_idx] = '-'
				newName = "".join(a)
				os.rename(fname,newName)

if __name__ == '__main__':
	sys.exit(main())
    
