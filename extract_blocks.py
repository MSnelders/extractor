#!/usr/bin/env python2

# This script should be run with Python 2, and will fail with Python 3.

import numpy as np
import sys
import os
import glob



if len(sys.argv) != 6:
        print "function call:"
        print "  [1] path to files"
        print "  [2] repeating part in file names"
        print "  [3] starting time in s"
        print "  [4] stopping time in s"
        print "  [5] path to extract the data to"
        sys.exit()

os.chdir(sys.argv[1])
nTotalBlocs = 0
nListBlocs = []
nListBlocsCumul = []
idx = 0

fname = sys.argv[2]

flist = sorted(glob.glob(sys.argv[2] + "*.raw"))        # lists all files corresponding to entries
nnumfiles = len(flist)                                  # number of files to process
print nnumfiles," files found in the dataset"           # print number of files

# There is no good reason on why the header should contain more than 300 lines
max_header_lines = 300

# set up standard values
direct_io_size = 512 # bytes
bytes_per_line = 80 # bytes
value_start_idx = 10


for file in flist:                              # .raw files starting with argv 2
        f = open(file,'rb')                     # open as [b]inary and to [r]ead
        f.seek(0,0)                             # 0=bof, 1=current, 2=eof
        
        nHeadLine = 0
        for i in range(max_header_lines):
            currline = f.read(bytes_per_line)           
            nHeadLine += 1 # increase nHeadLine counter
            
            # escape when end of header has been found
            if currline.startswith('END'):
                    break
                
            # the header end should be reached before max_header_lines
            if nHeadLine == max_header_lines - 1:
                    sys.exit("""End of header not found within the first 300 lines.
                             Are you sure you the files are correct and 
                             you are calling this function with Python 2?""")
            
            subline = currline[value_start_idx:] # remove keyword from string
            subline = subline.replace('"', '').replace("'", "").replace(' ', '') # clean string
            
            if ('BLOCSIZE' in subline):
                    nBlocsize = int(subline) # convert string to integer
                    
            if ('DIRECTIO' in subline):
                    directio = int(subline) # convert string to integer
                    
            if ('CHAN_BW ' in subline):
                    dchanbw = float(subline) # convert string to float
        
            # the header consists of N lines, each bytes_per_line long, and the last line should be "END" followed by 77 spaces.
            nHeaderSize = nHeadLine * bytes_per_line
            
            # if directio is enabled, padding the header is (likely) required to
            # make nHeaderSize % direct_io_size == 0
            if directio == 1:
                    nHeaderSize = direct_io_size * np.ceil( (nHeadLine * bytes_per_line) / direct_io_size )
          
            f.close()
            
            assert nHeaderSize == int(nHeaderSize)
            nHeaderSize = int(nHeaderSize)
            
            nBlocs = float(os.path.getsize(file)) / float(nHeaderSize + nBlocsize)
            assert nBlocs == int(nBlocs)
            nBlocs = int(nBlocs)
            
            nTotalBlocs = nTotalBlocs + nBlocs      # total number of blocks
            nListBlocs.append(nBlocs)               # lists number of blocks per file
            nListBlocsCumul.append(nTotalBlocs)     # lists number of cumulative blocks

# print "# of blocs = ",nTotalBlocs
# print "total duration = ",nTotalBlocs*nBlocsize/64./4./abs(dchanbw)/1e6," seconds"

if float(sys.argv[3]) < 0:      # verify starting time is >0
        print "starting time must be positive and < ",nTotalBlocs*nBlocsize/64./4./abs(dchanbw)/1e6," s"
        sys.exit()
        
# verify stopping time is > starting time and < total duration
if float(sys.argv[4]) <= float(sys.argv[3]) or float(sys.argv[4]) > (nTotalBlocs*nBlocsize/64./4./abs(dchanbw)/1e6):
        print "stopping time must be > ",sys.argv[3]," and < ",nTotalBlocs*nBlocsize/64./4./abs(dchanbw)/1e6," s"
        sys.exit()

idx = 0
while nListBlocsCumul[idx]*nBlocsize/64./4./abs(dchanbw)/1e6 <= float(sys.argv[3]):
        idx = idx + 1
        
StartFile = idx
if StartFile == 0:
        StartBlock = int(np.floor(float(sys.argv[3]) * nListBlocs[StartFile] / (nListBlocs[StartFile]*nBlocsize/64./4./abs(dchanbw)/1e6)))
else:
        timefile = float(sys.argv[3]) - nListBlocsCumul[idx-1]*nBlocsize/64./4./abs(dchanbw)/1e6
        StartBlock = int(np.floor(timefile * nListBlocs[StartFile] / (nListBlocs[StartFile]*nBlocsize/64./4./abs(dchanbw)/1e6)))

idx = 0
while nListBlocsCumul[idx]*nBlocsize/64./4./abs(dchanbw)/1e6 <= float(sys.argv[4]):
        idx = idx + 1
StopFile = idx
if StopFile == 0:
        StopBlock = int(np.floor(float(sys.argv[4]) * nListBlocs[StopFile] / (nListBlocs[StopFile]*nBlocsize/64./4./abs(dchanbw)/1e6)))
else:
        timefile = float(sys.argv[4]) - nListBlocsCumul[idx-1]*nBlocsize/64./4./abs(dchanbw)/1e6
        StopBlock = int(np.floor(timefile * nListBlocs[StopFile] / (nListBlocs[StopFile]*nBlocsize/64./4./abs(dchanbw)/1e6)))

print "copy starts with file #",StartFile," - block #",StartBlock
print "copy stops with file #",StopFile," - block #",StopBlock


ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M_%S')

newfilename = sys.argv[5]+fname+"_"+sys.argv[3]+"_"+sys.argv[4]+".raw"
print "writing data to "+newfilename
nf = open(newfilename,'wb')     # open as [b]inary and to [w]rite

for filenum in range(StartFile,StopFile+1):
        f = open(flist[filenum],'rb')
        if StartFile == StopFile:
                for numblk in np.arange(StartBlock,StopBlock+1):
                        f.seek(int(numblk*(nHeaderSize+nBlocsize)),0)
                        nf.write(f.read(int(nHeaderSize+nBlocsize)))
	else:
                if filenum == StartFile:
                        for numblk in numpy.arange(StartBlock,nListBlocs[filenum]):
                                f.seek(int(numblk*(nHeaderSize+nBlocsize)),0)
                                nf.write(f.read(int(nHeaderSize+nBlocsize)))
		elif filenum == StopFile:
                        for numblk in numpy.arange(0,StopBlock+1):
                                f.seek(int(numblk*(nHeaderSize+nBlocsize)),0)
                                nf.write(f.read(int(nHeaderSize+nBlocsize)))
                else:
                        for numblk in numpy.arange(0,nListBlocs[filenum]):
                                f.seek(int(numblk*(nHeaderSize+nBlocsize)),0)
                                nf.write(f.read(int(nHeaderSize+nBlocsize)))
	f.close()

nf.close()


