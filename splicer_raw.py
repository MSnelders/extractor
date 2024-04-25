# raw voltage splicer
# 1st argument is path to directory containing all raw extracted files
# 2nd argument is divider
# 3rd argument is output file name
# example :  python raw_splicer.py "/datax2/greg_raw_splicer/RAWFILE2/" 2 "test.raw"

import os
import sys
import numpy

os.chdir(sys.argv[1])
list_files = os.listdir(sys.argv[1])    # list all files from the data set
list_files.sort()
numfiles = len(list_files)                # counts number of files in dataset

if numfiles == 0:
    print("error : no file corresponding to the dataset indicated")
    sys.exit()
else:
    print "there are " + str(numfiles) + " files in the data set"

idx = -1
cenfreq = [0]*numfiles    # array of central frequencies to order files
obsbw = [0]*numfiles    # array of bandwidths
NumBlocs = [0]*numfiles    # array of number of blocks per raw file

# empty list to store BANKNAM numbers
banknamlist = []

# There is no good reason on why the header should contain more than 300 lines
max_header_lines = 300

# set up standard values
direct_io_size = 512 # bytes
bytes_per_line = 80 # bytes
value_start_idx = 10

for fname in list_files:
    idx = idx + 1
    fread = open(fname,'rb')    # open first file of data set

    nHeaderLines = 0
    for i in range(max_header_lines):
        currline = str(fread.read(bytes_per_line))    # read new header line
        nHeaderLines = nHeaderLines + 1     # counts number of lines in header

        # escape when end of header has been found
        if currline.startswith('END'):
            break

        # the header end should be reached before max_header_lines
        if nHeaderLines == max_header_lines - 1:
            sys.exit("""End of header not found within the first 300 lines.
                        Are you sure you the files are correct and
                        you are calling this function with Python 2?""")

        subline = currline[value_start_idx:] # remove keyword from string
        subline = subline.replace('"', '').replace("'", "").replace(' ', '') # clean string

        if currline[0:9] == 'OBSFREQ =':    # read cenral frequency
            cenfreq[idx] = float(subline)
        if currline[0:9] == 'OBSBW   =':    # read bandwidth
            obsbw[idx] = float(subline)
        if currline[0:9] == 'OBSNCHAN=':    # read number of coarse channels
            obsnchan = float(subline)
        if currline[0:9] == 'NBITS   =':    # read quantization
            nbits = float(subline)
        if currline[0:9] == 'DIRECTIO=':    # read directio flag
            ndirectio = float(subline)
        if currline[0:9] == 'BLOCSIZE=':    # read block size
            nblocsize = float(subline)
        if currline[0:9] == 'PKTSIZE =':    # read packet size
            nPktSize = float(subline)
        if currline[0:9] == 'BANKNAM =':    # read banknam
            banknamlist.append(subline)
    fread.close()

    # the header consists of N lines, each bytes_per_line long, and the last line should be "END" followed by 77 spaces.
    nHeaderSize = nHeaderLines * bytes_per_line
    nPadd = 0

    # if directio is enabled, padding the header is (likely) required to
    # make nHeaderSize % direct_io_size == 0
    # padding not required if nHeaderSize % direct_io_size == 0
    if ndirectio == 1 and not (nHeaderSize % direct_io_size == 0):
        nHeaderSize_previous = nHeaderSize
        nHeaderSize = direct_io_size * ( 1 + nHeaderSize // direct_io_size )
        nPadd = nHeaderSize - nHeaderSize_previous
        assert nPadd >= 0

    statinfo = os.stat(fname)
    nBlocks = float(statinfo.st_size) / float(nblocsize + nHeaderSize)
    assert nBlocks == int(nBlocks)
    nBlocks = int(nBlocks)
    NumBlocs[idx] = nBlocks

# create a string which contains which banknams were spliced together
banknamlist = [str(x.replace("BLP", "").replace(" ", "").replace("'", "")) for x in banknamlist]
banknamesstr = "".join(banknamlist)

nChanSize = nblocsize/obsnchan
TotCenFreq = sum(cenfreq) / float(len(cenfreq))
TotBW = sum(obsbw)

if int(sum(NumBlocs)/len(NumBlocs)) != int(NumBlocs[0]):
    print("all files don''t have the same number of blocks...")
    sys.exit()
else:
    NumBlocsSpliced = NumBlocs[0]

IdxFiles = numpy.argsort(cenfreq)
if TotBW < 0:
    IdxFiles = IdxFiles[::-1]

TotBlocSize = int(nblocsize*len(IdxFiles))
NewTotBlocSize = int(TotBlocSize / float(sys.argv[2]))
TotNumChann = int(obsnchan * len(IdxFiles))

output_file = open(sys.argv[3],"wb")
for nblock in range(int(NumBlocs[0])):
    print "copy block #" + str(nblock+1) + "/" + str(int(NumBlocs[0]))
    for nDivid in range(int(sys.argv[2])):
        fread = open(list_files[0],'rb')    # open first file of data set
        fread.seek(int(nblock*(bytes_per_line*nHeaderLines+nPadd+nblocsize)))    # goes to the header
        currline = fread.read(bytes_per_line)
        output_file.write(currline)
        while str(currline[0:3]) != 'END':        # until reaching end of header
            currline = fread.read(bytes_per_line)        # read new header line
            if str(currline[0:9]) == 'BANKNAM =':
                # write down which banks were spliced together.
                # this might cause an issue if >30 banks are spliced together
                teststr = 'BANKNAM = ''BLP{}'''.format(banknamesstr)
                assert len(teststr) < bytes_per_line
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'OBSFREQ =':    # change central frequency value
                NewVal = TotCenFreq
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'OBSBW   =':    # change bandwidth value
                NewVal = TotBW
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'OBSNCHAN=':    # change number of coarse channels
                NewVal = TotNumChann
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'DIRECTIO=':    # change directio value
                NewVal = 0
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'BLOCSIZE=':    # change block size value
                NewVal = int(NewTotBlocSize)
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'NPKT    =':
                NewVal = int(NewTotBlocSize/nPktSize)
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'PKTIDX  =':
                NewVal = TotBlocSize/nPktSize*nblock + NewTotBlocSize/nPktSize*nDivid
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            if str(currline[0:9]) == 'PKTSTOP =':
                NewVal = TotBlocSize/nPktSize*(NumBlocsSpliced-1) + NewTotBlocSize/nPktSize*(int(sys.argv[2])-1)
                NewValStr = str(NewVal)
                if len(NewValStr) > 20:
                    NewValStr = NewValStr[0:20]
                teststr = currline[0:9] + ' '*(20+1-len(NewValStr)) + NewValStr
                teststr = teststr + ' '*(bytes_per_line-len(teststr))
                currline = teststr
            output_file.write(currline)
        for nChan in range(int(obsnchan)):
            fread.seek(nblock*(nHeaderLines*bytes_per_line+nPadd+nblocsize)+nHeaderLines*bytes_per_line+nPadd+nChan*nChanSize+nDivid*nChanSize/int(sys.argv[2]))
            tmpdata = fread.read(int(nChanSize/int(sys.argv[2])))    # read data block
            output_file.write(tmpdata)    # write data block
        fread.close()            # close current file
        for nFile in range(1,numfiles):
            fread = open(list_files[nFile],'rb')    # open file
            for nChan in range(int(obsnchan)):
                fread.seek(nblock*(nHeaderLines*bytes_per_line+nPadd+nblocsize)+nHeaderLines*bytes_per_line+nPadd+nChan*nChanSize+nDivid*nChanSize/int(sys.argv[2]))
                tmpdata = fread.read(int(nChanSize/int(sys.argv[2])))    # read data block
                output_file.write(tmpdata)    # write data block

            fread.close()            # close current file

output_file.close() # close new file


