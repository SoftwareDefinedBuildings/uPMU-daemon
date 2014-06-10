#!/usr/bin/python

import argparse
import bson
import calendar
import csv
import datetime
import os
import socket
import struct
import thread
import threading
import traceback
import txmongo

from parser import sync_output, parse_sync_output
from sys import argv
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ServerEndpoint
from txmongo._pymongo.binary import Binary
from utils import *

# Maps serial numbers to their aliases
aliases = {}

parsed = [] # Stores parsed sync_output structs

mongoids = [] # Stores ids of mongo documents

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--seconds', help='the number of seconds per output csv file', type=int, default=900)
parser.add_argument('-d', '--depth', help='the depth of the files in the directory structure being sent (top level is at depth 1)', type=int, default=5)
parser.add_argument('-o', '--output', help='the directory in which to store the csv files', default='output/')
parser.add_argument('-p', '--port', help='the port at which to accept incoming messages', type=int, default=1883)
args = parser.parse_args()

NUM_SECONDS_PER_FILE = args.seconds

DIRDEPTH = args.depth

OUTPUTDIR = args.output
if args.output[-1] != '/':
    OUTPUTDIR += '/'

ADDRESSP = args.port
    
# Mongo DB collections (will be set later)
received_files = None
latest_time = None
warnings = None

# The first row of every csv file has lables
firstrow = ['time', 'lockstate']
for start in ('L', 'C'):
    for num in xrange(1, 4):
        for end in ('Ang', 'Mag'):
            firstrow.append('{0}{1}{2}'.format(start, num, end))
firstrow.extend(['satellites', 'hasFix'])

class ConnectionTerminatedException(RuntimeError):
    pass
    
class DatabaseException(RuntimeError):
    pass
    
class ParseException(RuntimeError):
    pass

def parse(string):
    """ Parses data (in the form of STRING) into a series of sync_output
    objects. Returns a list of sync_output objects. If STRING is not of a
    suitable length (i.e., if the number of bytes is not some multiple of
    the length of a sync_output struct) a ParseException is raised. """
    if len(string) % sync_output.LENGTH != 0:
        raise ParseException('Input to \"parse\" does not contain whole number of \"sync_output\"s ({0} extra bytes)'.format(len(string) % sync_output.LENGTH))
    lst = []
    while string:
        obj, string = parse_sync_output(string)
        lst.append(obj)
    return lst
    
class TCPResolver(Protocol):
    def __init__(self):
        self._parsed = []
        self._mongoids = []
        self.firstfilepath = None
        self.serialNum = None
        
    def dataReceived(self, data):
        self.have += data
        if self.sendid is None:
            if len(self.have) >= 4:
                self.sendid = self.have[:4]
                self.have = self.have[4:]
            else:
                return
        if self.lengths is None:
            if len(self.have) >= 12:
                self.lengths, self.lengthserial, self.lengthd = struct.unpack('<III', self.have[:12])
                self.padding1 = ((self.lengths + 3) & 0xFFFFFFFC)
                self.padding2 = ((self.lengthserial + 3) & 0xFFFFFFFC)
                self.have = self.have[12:]
            else:
                return
        if self.filepath is None:
            if len(self.have) >= self.lengths:
                self.filepath = self.have[:self.lengths]
                self.have = self.have[self.padding1:]
            else:
                return
        if not self.gotSerialNum:
            if len(self.have) >= self.lengthserial:
                newSerial = self.have[:self.lengthserial]
                if self.serialNum is not None and newSerial != self.serialNum:
                    print 'WARNING: serial number changed from {0} to {1}'.format(self.serialNum, newSerial)
                    print 'Updating serial number for next write'
                self.serialNum = newSerial
                self.gotSerialNum = True
                self.have = self.have[self.padding2:]
            else:
                return
        if self.data is None:
            if len(self.have) >= self.lengthd:
                self.data = self.have[:self.lengthd]
                self.have = self.have[self.lengthd:]
                if self.have:
                    print 'WARNING: got {0} extra bytes'.format(len(self.have))
                    self.have = ''
            else:
                return
        # if we've reached this point, we have all the data
        print 'Received', self.filepath
        self._processdata()
        self._setup()
            
    def connectionLost(self, reason):
        print 'Connection lost:', self.transport.getPeer()
        print 'Writing pending data...'
        self._writecsv()
        print 'Finished writing pending data'
        
    def connectionMade(self):
        self.have = ''
        self._setup()
        print 'Connected:', self.transport.getPeer()
        
    def _setup(self):
        self.sendid = None
        self.lengths = None
        self.lengthserial = None
        self.lengthd = None
        self.filepath = None
        self.gotSerialNum = False
        self.data = None
        
    def _processdata(self):
        if self.firstfilepath is None:
            self.firstfilepath = self.filepath # the first filepath for the N sec. interval
        parseddata = parse(self.data)
        received_file = {'name': self.filepath,
                         'data': Binary(self.data),
                         'published': False,
                         'time_received': datetime.datetime.utcnow(),
                         'serial_number': self.serialNum}
        docsDeferred = latest_time.update({'serial_number': self.serialNum}, {'$set': {'time_received': received_file['time_received']}}, upsert = True)
        docsDeferred.addErrback(latest_time_error, self.serialNum)
        mongoiddeferred = received_files.insert(received_file)
        mongoiddeferred.addCallback(self._finishprocessing, parseddata)
        mongoiddeferred.addErrback(databaseerror, self.transport)
        
    def _finishprocessing(self, mongoid, parseddata):
        self._parsed.extend(parseddata)
        self._mongoids.append(mongoid)
        self.transport.write(self.sendid)
        if len(self._parsed) >= NUM_SECONDS_PER_FILE:
            try:
                self._writecsv()
            except BaseException as be:
                print 'Could not write to CSV file'
                print 'Details:', be
            
    def _writecsv(self):
        """ Attempts to write data in self._parsed to CSV file. Upon success, updates the mongo
        database to indicate that their data have been published and returns True. Upon failure,
        returns False. """
        success = True
        if not self._parsed:
            return
        parsedcopy = self._parsed
        self._parsed = []
        mongoidscopy = self._mongoids
        self._mongoids = []
        filepath = self.firstfilepath
        self.firstfilepath = None
        try:
            parsedcopy.sort(key=lambda x: x.sync_data.times)
            self._check_duplicates(parsedcopy)
            firstTime = time_to_str(parsedcopy[0].sync_data.times)
            lastTime = time_to_str(parsedcopy[-1].sync_data.times)
            dirtowrite = '{0}{1}/'.format(OUTPUTDIR, aliases.get(self.serialNum, self.serialNum))
            subdirs = filepath.rsplit('/', DIRDEPTH)
            if subdirs[-1].endswith('.dat'):
                subdirs[-1] = subdirs[-1][:-4]
            if len(subdirs) <= DIRDEPTH:
                print 'WARNING: filepath {0} has insufficient depth'.format(filepath)
            dirtowrite += '/'.join(subdirs[1:-1])
            if not os.path.exists(dirtowrite):
                os.makedirs(dirtowrite)
            filename = '{0}/{1}__{2}__{3}.csv'.format(dirtowrite, self.serialNum, firstTime, lastTime)
            print 'Writing file {0}'.format(filename)
            with open(filename, 'wb') as f:
                writer = csv.writer(f)
                writer.writerow(firstrow)
                writer.writerows(lst_to_rows(parsedcopy))
        except KeyboardInterrupt:
            success = False
        except BaseException as be:
            success = False
            print 'WARNING: write could not be completed due to exception'
            print 'Details: {0}'.format(be)
            print 'Traceback:'
            traceback.print_exc()
        finally:
            if success:
                for mongoid in mongoidscopy:
                    d = received_files.update({'_id': mongoid}, {'$set': {'published': True}})
                    d.addErrback(print_mongo_error, 'write')
                return True
            return False
            
    def _check_duplicates(self, sorted_struct_list):
        # Check if the structs have duplicates or missing items, print warnings and update Mongo if so
        dates = tuple(datetime.datetime(*s.sync_data.times) for s in sorted_struct_list)
        i = 1
        while i < len(dates):
            date1 = dates[i-1]
            date2 = dates[i]
            delta = int((date2 - date1).total_seconds() + 0.5) # round difference to nearest second
            if delta == 0:
                d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'duplicate', 'warning_time': datetime.datetime.utcnow(), 'start_time': date2})
                d.addErrback(print_mongo_error, 'warning')
                print 'WARNING: duplicate record for {0}'.format(str(date2))
            elif delta != 1:
                d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'missing', 'warning_time': datetime.datetime.utcnow(), 'start_time': date1, 'end_time': date2})
                d.addErrback(print_mongo_error, 'warning')
                print 'WARNING: missing record(s) (skips from {0} to {1})'.format(str(date1), str(date2))
            i += 1
            
def print_mongo_error(err, task):
    print 'WARNING: could not update Mongo Database with recent {0}'.format(task)
    print 'Details:', err
    
def databaseerror(err, transport):
    print 'Could not update database:', err
    transport.write('\x00\x00\x00\x00')
        
def latest_time_error(err, serialnumber):
    print 'Cannot update latest_time collection for serial number', serialnumber
    print 'Details:', err

class ResolverFactory(Factory):
    def buildProtocol(self, addr):
        return TCPResolver()

def setup(mconn):
     global received_files, latest_time, warnings
     received_files = mconn.upmu_database.received_files_test
     latest_time = mconn.upmu_database.latest_time_test
     warnings = mconn.upmu_database.warnings_test
     try:
         with open('serial_aliases.ini', 'r') as f:
             for line in f:
                 pair = line.rstrip().split('=')
                 aliases[pair[0]] = pair[1]
     except:
         print 'WARNING: Could not read serial_aliases.ini'
     endpoint = TCP4ServerEndpoint(reactor, ADDRESSP)
     endpoint.listen(ResolverFactory())

def termerror(e):
    print "terminal error", e
    lg.error("TERMINAL ERROR: \n%s",e)
    return defer.FAILURE
         
d = txmongo.MongoConnection()
d.addCallbacks(setup, termerror)
d.addErrback(termerror)

reactor.run()
