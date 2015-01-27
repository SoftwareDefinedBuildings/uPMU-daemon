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


from math import floor
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

pending = {}

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--seconds', help='the number of seconds per output csv file', type=int, default=-1)
parser.add_argument('-d', '--depth', help='the depth of the files in the directory structure being sent (top level is at depth 0)', type=int, default=4)
parser.add_argument('-o', '--output', help='the directory in which to store the csv files', default='output/')
parser.add_argument('-p', '--port', help='the port at which to accept incoming messages', type=int, default=1883)
args = parser.parse_args()

if args.seconds == -1:
    NUM_SECONDS_PER_FILE = 900
    write_csv = False
else:
    NUM_SECONDS_PER_FILE = args.seconds
    write_csv = True

DIRDEPTH = args.depth

OUTPUTDIR = args.output
if args.output[-1] != '/':
    OUTPUTDIR += '/'

ADDRESSP = args.port

currtime = datetime.datetime.utcnow()

BASETIME = datetime.datetime(currtime.year, currtime.month, currtime.day, currtime.hour) # To start the CSV cycle
    
# Mongo DB collections (will be set later)
received_files = None
latest_time = None
warnings = None
warnings_summary = None

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
        self.firstfilepath = None
        self.serialNum = None
        self.cycleTime = None # The time at which this cycle starts
        
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
        print 'Received {0}: serial number is {1}'.format(self.filepath, self.serialNum), '({0}),'.format(aliases.get(self.serialNum, 'alias not known')), 'length is {0}'.format(len(self.data))
        self._processdata()
        self._setup()
            
    def connectionLost(self, reason):
        print 'Connection lost:', self.transport.getPeer()
        pending[self.serialNum] = (self.cycleTime, self.firstfilepath, self._parsed)
        
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
        if self.serialNum in pending:
            if self._parsed:
                print 'WARNING: multiple uPMUs with the same serial number appear to be connected simultaneously'
            self.cycleTime, self.firstfilepath, self._parsed = pending[self.serialNum] # Restore from previous session
            del pending[self.serialNum]
        if self.firstfilepath is None: # To handle the very first file received
            self.firstfilepath = self.filepath
        received_file = {'name': self.filepath,
                         'data': Binary(self.data),
                         'published': False,
                         'time_received': datetime.datetime.utcnow(),
                         'serial_number': self.serialNum}
        docsDeferred = latest_time.update({'serial_number': self.serialNum}, {'$set': {'time_received': received_file['time_received']}}, upsert = True)
        docsDeferred.addErrback(latest_time_error, self.serialNum, self.filepath)
        try:
            parseddata = parse(self.data)
        except:
            print 'ERROR: file', self.filepath, 'does not contain a whole number of sync_outputs. Ignoring file.'
            self.transport.write('\x00\x00\x00\x00')
            mongoiddeferred = received_files.insert(received_file)
            return
        if write_csv and (self.cycleTime is None):
            try:
                secsFromBase = (datetime.datetime(*parseddata[0].sync_data.times) - BASETIME).total_seconds()
                self.cycleTime = BASETIME + datetime.timedelta(0, secsFromBase - (secsFromBase % NUM_SECONDS_PER_FILE))
            except:
                print 'WARNING:', self.filepath, 'has an invalid date'
        mongoiddeferred = received_files.insert(received_file)
        mongoiddeferred.addCallback(self._finishprocessing, parseddata, self.sendid)
        mongoiddeferred.addErrback(databaseerror, self.transport, self.filepath)
        
    def _finishprocessing(self, mongoid, parseddata, sendid):
        print 'Successfully added file to database'
        self.transport.write(sendid)
        print 'Sent confirmation of receipt ({0})'.format(repr(sendid))
        parseddata[-1].mongoid = mongoid
        if write_csv:
            self._parsed.extend(parseddata)
            latest_time = parseddata[0].sync_data.times
            for parsed in parseddata:
                if parsed.sync_data.times > latest_time:
                    latest_time = parsed.sync_data.times
            latest_time = datetime.datetime(*latest_time)
            while (latest_time - self.cycleTime).total_seconds() >= NUM_SECONDS_PER_FILE:
                try:
                    self._writecsv()
                except BaseException as be:
                    print 'Could not write to CSV file'
                    print 'Details:', be
            
    def _writecsv(self):
        """ Attempts to write data in self._parsed to CSV file. Upon success, updates the mongo
        database to indicate that their data have been published and returns True. Upon failure,
        returns False. If a cycle has been skipped, moves to the next cycle and does nothing."""
        success = True
        if not self._parsed:
            return
        self._parsed.sort(key=lambda x: x.sync_data.times)
        nextCycleTime = self.cycleTime + datetime.timedelta(0, NUM_SECONDS_PER_FILE)
        dates = tuple(datetime.datetime(*s.sync_data.times) for s in self._parsed)
        i = binsearch(dates, nextCycleTime)
        while dates[i] >= nextCycleTime and i >= 0:
            i -= 1
        i += 1
        if i == 0:
            d = warnings_summary.insert({'serial_number': self.serialNum, 'time': datetime.datetime.utcnow(), 'csv_start': self.cycleTime, 'next_csv_start': nextCycleTime, 'num_warnings': 1, 'written': False})
            d.addErrback(print_mongo_error, 'warning summary')
            print 'WARNING: missing record(s) (no data from {0} to {1}, no CSV file written)'.format(self.cycleTime, nextCycleTime - datetime.timedelta(0, 1))
            self.cycleTime = nextCycleTime
            return
        parsedcopy = self._parsed[:i]
        self._parsed = self._parsed[i:]
        filepath = self.firstfilepath
        self.firstfilepath = self.filepath # We've received the next CSV already.
        try:
            parsedcopy.sort(key=lambda x: x.sync_data.times)
            num_warnings = self._check_duplicates(dates[:i], nextCycleTime)
            firstTime = time_to_str(parsedcopy[0].sync_data.times)
            lastTime = time_to_str(parsedcopy[-1].sync_data.times)
            dirtowrite = '{0}{1}/'.format(OUTPUTDIR, aliases.get(self.serialNum, self.serialNum))
            subdirs = filepath.rsplit('/', DIRDEPTH + 1)
            if subdirs[-1].endswith('.dat'):
                subdirs[-1] = subdirs[-1][:-4]
            if len(subdirs) <= DIRDEPTH + 1:
                print 'WARNING: filepath {0} has insufficient depth'.format(filepath)
            dirtowrite += '/'.join(subdirs[1:-1])
            if not os.path.exists(dirtowrite):
                os.makedirs(dirtowrite)
            filename = '{0}/{1}__{2}__{3}.csv'.format(dirtowrite, self.serialNum, self.cycleTime, nextCycleTime - datetime.timedelta(0, 1))
            with open(filename, 'wb') as f:
                writer = csv.writer(f)
                writer.writerow(firstrow)
                early, normal, duplicate = self._lst_to_rows(parsedcopy)
                writer.writerows(normal)
                writer.writerow([])
                if duplicate:
                    writer.writerow(['Duplicate records (entries for same times exist in this CSV file):'])
                    writer.writerows(duplicate)
                    writer.writerow([])
                if early:
                    writer.writerow(['Misplaced records (should be in earlier CSV file):'])
                    writer.writerows(early)
            print 'Successfully wrote file', filename
            d = warnings_summary.insert({'serial_number': self.serialNum, 'time': datetime.datetime.utcnow(), 'csv_start': self.cycleTime, 'next_csv_start': nextCycleTime, 'num_warnings': num_warnings, 'written': True})
            d.addErrback(print_mongo_error, 'warning summary')
        except KeyboardInterrupt:
            success = False
        except BaseException as be:
            success = False
            print 'WARNING: write could not be completed due to exception'
            print 'Details: {0}'.format(be)
            print 'Traceback:'
            traceback.print_exc()
        finally:
            self.cycleTime = nextCycleTime
            if success:
                for struct in parsedcopy:
                    if struct.mongoid is not None:
                        d = received_files.update({'_id': struct.mongoid}, {'$set': {'published': True}})
                        d.addErrback(print_mongo_error, 'write')
                return True
            return False
            
    def _lst_to_rows(self, parsed):
        earlyRecords = []
        rows = [[] for _ in xrange(120 * NUM_SECONDS_PER_FILE)]
        duplicates = []
        for s in parsed:
            stime = datetime.datetime(*s.sync_data.times)
            basetime = time_to_nanos(stime)
            early = False
            duplicate = False
            j = int(floor((stime - self.cycleTime).total_seconds() + 0.5)) # Floor it first because it might be negative
            if j < 0:
                early = True
            elif rows[120 * j]:
                duplicate = True
            # it seems s.sync_data.sampleRate is the number of milliseconds between samples
            timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
            for i in xrange(120):
                row = []
                row.append(basetime + int((i * timedelta) + 0.5))
                row.append(s.sync_data.lockstate[i])
                for start in ('L', 'C'):
                    for num in xrange(1, 4):
                        attribute = getattr(s.sync_data, '{0}{1}MagAng'.format(start, num))
                        row.append(attribute[i].angle)
                        row.append(attribute[i].mag)
                row.append(s.gps_stats.satellites)
                row.append(s.gps_stats.hasFix)
                if early:
                    earlyRecords.append(row)
                elif duplicate:
                    duplicates.append(row)
                else:
                    rows[(120 * j) + i] = row
        return earlyRecords, rows, duplicates
            
    def _check_duplicates(self, dates, nextCycleTime):
        """ Returns the total number of duplicates/missing/misplaced records found, and adds warnings to
        Mongo DB as necessary. """
        # DATES is assumed to be in sorted order
        # Check if the structs have duplicates or missing items, print warnings and update Mongo if so
        if not dates:
            return 0
        num_records = 0
        i = binsearch(dates, self.cycleTime)
        while i >= 0 and dates[i] >= self.cycleTime:
            i -= 1
        i += 1
        if i != 0:
            d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'misplaced', 'warning_time': datetime.datetime.utcnow(), 'start_time': dates[0], 'end_time': dates[i - 1], 'prev_time': self.cycleTime})
            d.addErrback(print_mongo_error, 'warning')
            print 'WARNING: misplaced record(s) could not be corrected due to CSV file boundary (CSV file contains records from {0} to {1}, but would normally start at {2})'.format(dates[0], dates[i-1], self.cycleTime)
            num_records = i
        # Check for a gap at the beginning of the CSV
        if dates[i] > self.cycleTime:
            d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'missing', 'warning_time': datetime.datetime.utcnow(), 'start_time': self.cycleTime, 'end_time': dates[i] - datetime.timedelta(0, 1)})
            d.addErrback(print_mongo_error, 'warning')
            print 'WARNING: missing record(s) (no data from {0} to {1})'.format(self.cycleTime, dates[i] - datetime.timedelta(0, 1))
            num_records += 1
        # Check for a gap at the end of the CSV
        lastTime = nextCycleTime - datetime.timedelta(0, 1)
        if dates[-1] < lastTime:
            d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'missing', 'warning_time': datetime.datetime.utcnow(), 'start_time': dates[-1] + datetime.timedelta(0, 1), 'end_time': lastTime})
            d.addErrback(print_mongo_error, 'warning')
            print 'WARNING: missing record(s) (no data from {0} to {1})'.format(dates[-1] + datetime.timedelta(0, 1), lastTime)
            num_records += 1
        j = 1
        while j < len(dates):
            if j == i:
                j += 1
                continue # don't check for gap between last misplaced record and first good record
            num_records += self._check_sorted_gaps(dates[j-1], dates[j])
            j += 1
        return num_records
            
    def _check_sorted_gaps(self, date1, date2):
        """ Checks if date1 and date2 are sequential, assuming date1 <= date2.
        If not, prints an error message and updates Mongo as appropriate.
        Returns 1 if the dates are not sequential and 0 if they are. """
        num_errors = 0
        delta = int((date2 - date1).total_seconds() + 0.5) # round difference to nearest second
        if delta == 0:
            d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'duplicate', 'warning_time': datetime.datetime.utcnow(), 'start_time': date2})
            d.addErrback(print_mongo_error, 'warning')
            print 'WARNING: duplicate record for {0}'.format(date2)
            return 1
        elif delta != 1:
            d = warnings.insert({'serial_number': self.serialNum, 'warning_type': 'missing', 'warning_time': datetime.datetime.utcnow(), 'start_time': date1 + datetime.timedelta(0, 1), 'end_time': date2 - datetime.timedelta(0, 1)})
            d.addErrback(print_mongo_error, 'warning')
            print 'WARNING: missing record(s) (no data from {0} to {1})'.format(date1 + datetime.timedelta(0, 1), date2 - datetime.timedelta(0, 1))
            return 1
        return 0
            
def print_mongo_error(err, task):
    print 'WARNING: could not update Mongo Database with recent {0}'.format(task)
    print 'Details:', err
    
def databaseerror(err, transport, filepath):
    print 'Could not update database with file', filepath, ':', err
    transport.write('\x00\x00\x00\x00')
        
def latest_time_error(err, serialnumber, filepath):
    print 'Cannot update latest_time collection for serial number', serialnumber
    print 'Receipt of file', filepath, 'is not recorded'
    print 'Details:', err

class ResolverFactory(Factory):
    def buildProtocol(self, addr):
        return TCPResolver()

def setup(mconn):
     global received_files, latest_time, warnings, warnings_summary
     received_files = mconn.upmu_database.received_files
     latest_time = mconn.upmu_database.latest_time
     warnings = mconn.upmu_database.warnings
     warnings_summary = mconn.upmu_database.warnings_summary
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
