#!/usr/bin/python

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

ADDRESSP = 1883

NUM_SECONDS_PER_FILE = 10

parsed = [] # Stores parsed sync_output structs

mongoids = [] # Stores ids of mongo documents

# Check command line arguments
if len(argv) != 2:
    print 'Usage: ./receiver.py <num data seconds per file>'
    print ' but for your convenience I am defaulting to 900'
    argv = argv[0] + ["900"]
    
# Mongo DB collection
received_files = None
latest_time = None

NUM_SECONDS_PER_FILE = int(argv[1])
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
    

def process(data, datfilepath, serial):
    """ Converts DATA (in the form of a string) to sync_output objects and
    adds them to the list of processed objects. When enough objects have been
    processed, they are converted to a CSV file"""
    parseddata = parse(data)
    received_file = {'name': datfilepath,
                     'data': Binary(data),
                     'published': False,
                     'time_received': datetime.datetime.utcnow(),
                     'serial_number': serial}
    latest_time.insert({'name': datfilepath, 'time_received': datetime.datetime.utcnow(), 'serial_number': serial})
    mongoiddeferred = received_files.insert(received_file)
    mongoiddeferred.addCallback(finishprocessing, parseddata)
    mongoiddeferred.addErrback(databaseerror, parseddata)
        
def finishprocessing(mongoid, parseddata):
    parsed.extend(parseddata)
    mongoids.append(mongoid)
    if len(parsed) >= NUM_SECONDS_PER_FILE:
        write_csv()
        
def databaseerror(err, parseddata):
    print 'WARNING:', err
    write_backup(parseddata)
    
def write_backup(structs):
    """ Writes a backup of the structs in STRUCTS, a list of sync_output structs, to a file
    in the directory "backup". """
    print 'Writing backup...' # on failure, write data to file if it could not be published
    if not os.path.exists('backup/'):
        os.mkdir('backup/')
    numfiles = len(os.listdir('backup/'))
    numouts = numfiles
    while numouts > 0 and not os.path.exists('backup/backup{0}.dat'.format(numouts)):
        numouts -= 1
    numouts += 1
    backup = open('backup/backup{0}.dat'.format(numouts), 'wb')
    for s in structs:
        backup.write(s.data)
    backup.close()
    print 'Done writing backup.'

def write_csv():
    """ Attempts to write data in PARSED to CSV file. Upon success, updates mongo documents with ids in
    MONGOIDS to indicate that their data have been published and returns True. Upon failure,
    returns False and writes a backup of the relevant files if backup using Mongo DB has been
    disabled. """
    global parsed, mongoids
    success = True
    if not parsed:
        return
    parsedcopy = parsed
    parsed = []
    mongoidscopy = mongoids
    mongoids = []
    try:
        parsedcopy.sort(key=lambda x: x.sync_data.times)
        check_duplicates(parsedcopy)
        firstTime = time_to_str(parsedcopy[0].sync_data.times)
        lastTime = time_to_str(parsedcopy[-1].sync_data.times)
        if not os.path.exists('output/'):
            os.mkdir('output/')
        filename = 'output/out__{0}__{1}.csv'.format(firstTime, lastTime)
        print 'Writing file {0}'.format(filename)
        with open(filename, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(firstrow)
            writer.writerows(lst_to_rows(parsedcopy))
    except KeyboardInterrupt:
        success = False
    except BaseException as be:
        success = False
        print 'WARNING: publish could not be completed due to exception'
        print 'Details: {0}'.format(be)
        print 'Traceback:'
        traceback.print_exc()
    finally:
        if success:
            for mongoid in mongoidscopy:
                d = received_files.update({'_id': mongoid}, {'$set': {'published': True}})
                d.addErrback(print_mongo_error)
            return True
            
def print_mongo_error(err):
    print 'WARNING: could not update Mongo Database with recent write'

class TCPResolver(Protocol):
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
        if self.serialNum is None:
            if len(self.have) >= self.lengthserial:
                self.serialNum = self.have[:self.lengthserial]
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
        try:
            process(self.data, self.filepath, self.serialNum)
        except KeyboardInterrupt:
            self.sendid = '\x00\x00\x00\x00'
            raise
        except BaseException as err: # If there's an exception of any kind, set sendid
            print err
            traceback.print_exc()
            self.sendid = '\x00\x00\x00\x00'
        finally:
            self.transport.write(self.sendid)
            self._setup()
            
    def connectionLost(self, reason):
        print 'Connection lost:', self.transport.getPeer()
        
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
        self.serialNum = None
        self.data = None

class ResolverFactory(Factory):
    def buildProtocol(self, addr):
        return TCPResolver()

def setup(mconn):
     global received_files, latest_time
     received_files = mconn.upmu_database.received_files
     latest_time = mconn.upmu_database.latest_time
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
