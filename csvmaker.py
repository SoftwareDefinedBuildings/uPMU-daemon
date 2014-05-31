#!/usr/bin/python
import csv
import datetime
import os

from sys import argv
from parser import sync_output, parse_sync_output
from utils import check_duplicates, lst_to_rows

if len(argv) != 3:
    print 'Usage: ./csvmaker <directory to process> <num seconds per file>'
    exit()
else:
    DIR = argv[1]
    if not DIR.endswith('/'):
        DIR += '/'
    SECONDS_PER_FILE = int(argv[2])
    
# A list of parsed sync_outputs
parsed = []

# Load files into list "parsed"
num_processed_files = 0
for fname in os.listdir(DIR):
    fpath = DIR + fname
    if os.path.isfile(fpath) and fname.endswith('.dat'):
        with open(fpath, 'rb') as f:
            contents = f.read()
        if len(contents) % sync_output.LENGTH != 0:
            print "File {0} does not have a whole number of 'sync_output's".format(fname)
            print 'Skipping {0}'.format(fname)
        while contents:
            sync_output_struct, contents = parse_sync_output(contents)
            parsed.append(sync_output_struct)
        num_processed_files += 1
        if num_processed_files % 1000 == 0:
            print 'Read {0} files.'.format(num_processed_files)
            
            
print 'Finished reading all files.'
            
# Sort the structs according to the "times" array
parsed.sort(key=lambda x: x.sync_data.times)

# Check for duplicates and print warnings if present
check_duplicates(parsed)

# Create an output folder
currtime = str.replace(str(datetime.datetime.now()), ' ', '_')
currtime = str.replace(currtime, ':', '_')
currtime = str.replace(currtime, '.', '_')
dirname = 'output_{0}/'.format(currtime)
if not os.path.exists(dirname):
    os.mkdir(dirname)
    
# The first row of every csv file has lables
firstrow = ['time', 'lockstate']
for start in ('L', 'C'):
    for num in xrange(1, 4):
        for end in ('Ang', 'Mag'):
            firstrow.append('{0}{1}{2}'.format(start, num, end))
firstrow.extend(['satellites', 'hasFix'])

# Write csv files
numfiles = 0
while parsed:
    outpath = '{0}out{1}.csv'.format(dirname, numfiles)
    with open(outpath, 'wb') as output:
        writer = csv.writer(output)
        writer.writerow(firstrow)
        writer.writerows(lst_to_rows(parsed[:SECONDS_PER_FILE]))
        parsed = parsed[SECONDS_PER_FILE:]
    numfiles += 1
        
print 'Finished writing files'
