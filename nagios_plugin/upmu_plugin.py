#!/usr/bin/python

# This is a plugin that will work with Nagios for monitoring a uPMU

import argparse
import datetime
import math
import pymongo

parser = argparse.ArgumentParser()
parser.add_argument('serialnum', help='the serial number of the uPMU to check on')
parser.add_argument('-c', '--criticaltime', help='the minimum number of seconds for which inactivity should result in a critical alert; defaults to 20 minutes', type=int, default=1200)
parser.add_argument('-w', '--warningtime', help='the minimum number of seconds for which inactivity should result in a warning alert; defaults to one-half the critical time threshold', type=int)
args = parser.parse_args()

if args.warningtime is None:
    args.warningtime = args.criticaltime / 2
    if args.warningtime == 0:
        args.warningtime = 1
        
try:
    client = pymongo.MongoClient('128.32.37.231')
    latest_time = client.upmu_database.latest_time
except:
    print 'Unknown - could not start Mongo DB'
    exit(3)

doc = latest_time.find_one({"serial_number": args.serialnum})
if doc is None:
    print 'Unknown - no document for a uPMU with serial number "{0}" was found in the Mongo Database'.format(args.serialnum)
    exit(3)

time_received = doc['time_received']
delta = (datetime.datetime.utcnow() - time_received).total_seconds()
if delta >= args.criticaltime:
    print 'Critical - last message from uPMU was received at {0} (UTC)'.format(time_received)
    exit(2)
elif delta >= args.warningtime:
    print 'Warning - last message from uPMU was last received at {0} (UTC)'.format(time_received)
    exit(1)
else:
    print 'OK - last message from uPMU was last received at {0} (UTC)'.format(time_received)
    exit(0)
