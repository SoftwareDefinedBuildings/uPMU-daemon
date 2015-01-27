#!/usr/bin/python

import csv
import os
import pymongo

from parser import parse_sync_output
from utils import lst_to_rows

client = pymongo.MongoClient()
received_files = client.upmu_database.received_files

files = received_files.find({'serial_number': 'P3001097'})

DIRDEPTH = 4

struct_lst = []

# The first row of every csv file has lables
firstrow = ['time', 'lockstate']
for start in ('L', 'C'):
    for num in xrange(1, 4):
        for end in ('Ang', 'Mag'):
            firstrow.append('{0}{1}{2}'.format(start, num, end))
firstrow.extend(['satellites', 'hasFix'])

filesprocessed = 0

# Read the data and write files
for datfile in files:
    data = datfile['data']
    struct_lst = []
    original_data = data
    while data:
        struct, data = parse_sync_output(data)
        struct_lst.append(struct)
    subdirs = datfile['name'].rsplit('/', DIRDEPTH + 1)
    subdirs[0] = 'output'
    if len(subdirs) <= DIRDEPTH + 1:
        print 'WARNING: filepath {0} has insufficient depth'.format(filepath)
    dirtowrite = '/'.join(subdirs[:-1])
    if not os.path.exists(dirtowrite):
        os.makedirs(dirtowrite)
    filename = '{0}/{1}'.format(dirtowrite, subdirs[-1])
    with open(filename, 'wb') as outputdat:
        outputdat.write(original_data)
    if filename.endswith('.dat'):
        filename = filename[:-4]
    filename += '.csv'
    with open(filename, 'wb') as outputcsv:
        writer = csv.writer(outputcsv)
        writer.writerow(firstrow)
        writer.writerows(lst_to_rows(struct_lst))
    filesprocessed += 1
    if filesprocessed % 10000 == 0:
        print 'Processed', filesprocessed, 'files'
