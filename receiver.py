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

from parser import sync_output, parse_sync_output
from sys import argv
from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ServerEndpoint
from utils import *

backupdb = True # If we can back up to mongo

try:
    import txmongo
    from txmongo._pymongo.binary import Binary
except ImportError:
    print 'Library \"txmongo\" is not installed. Backup to Mongo Database is disabled.'
    backupdb = False

numouts = 0

ADDRESSP = 1883

NUM_SECONDS_PER_FILE = 10

parsed = [] # Stores parsed sync_output structs

mongoids = [] # Stores ids of mongo documents

csv_mode = False

# Check command line arguments
if len(argv) not in (3, 4, 5) or (len(argv) == 5 and argv[1] == '-c') or (len(argv) == 3 and argv[1] != '-c'):
    print 'Usage: ./receiver.py <archiver url> <subscription key> <num clock seconds per publish>'
    print '       ./receiver.py -c <num data seconds per file> to write to CSV file instead'
    print '       Add a \"-n\" at the end to disable backup to Mongo Database'
    exit()
elif argv[1] == '-c':
    csv_mode = True
    print 'In CSV mode'
    
if argv[-1] == '-n':
    if backupdb:
        print 'Backup to Mongo Database is disabled.'
        backupdb = False
    
# Mongo DB collection
received_files = None

if csv_mode:
    NUM_SECONDS_PER_FILE = int(argv[2])
    # The first row of every csv file has lables
    firstrow = ['time', 'lockstate']
    for start in ('L', 'C'):
        for num in xrange(1, 4):
            for end in ('Ang', 'Mag'):
                firstrow.append('{0}{1}{2}'.format(start, num, end))
    firstrow.extend(['satellites', 'hasFix'])
else:
    # Make streams for publishing
    # UUIDs were generated with calls to str(uuid.uuid1()) 5 times after importing uuid
    NUM_SECONDS_PER_FILE = int(argv[3])
    from ssmap import Ssstream
    L1Mag = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'Mag L1', '602e7098-ea93-11e3-a919-0026b6df9cf2', 'ns', 'V', 'UTC', [], argv[1], argv[2]), threading.Lock()]
    L1Ang = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'Ang L1', '60cc155a-ea93-11e3-a919-0026b6df9cf2', 'ns', 'deg', 'UTC', [], argv[1], argv[2]), threading.Lock()]
    C1Mag = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'Mag C1', '61725c76-ea93-11e3-a919-0026b6df9cf2', 'ns', 'A', 'UTC', [], argv[1], argv[2]), threading.Lock()]
    C1Ang = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'Ang C1', '61c86e5e-ea93-11e3-a919-0026b6df9cf2', 'ns', 'deg', 'UTC', [], argv[1], argv[2]), threading.Lock()]
    satellites = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'Num Satellites', '6212599c-ea93-11e3-a919-0026b6df9cf2', 'ns', 'no.', 'UTC', [], argv[1], argv[2]), threading.Lock()]
    streams = (L1Mag, L1Ang, C1Mag, C1Ang, satellites)

# Lock on data (to avoid concurrent writing to "parsed" and "mongoids")
datalock = threading.Lock()

class ConnectionTerminatedException(RuntimeError):
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
    

def process(data, datfilepath):
    """ Converts DATA (in the form of a string) to sync_output objects and
    adds them to the list of processed objects. When enough objects have been
    processed, they are converted to a CSV file"""
    datalock.acquire() # released in finishprocessing
    parsed.extend(parse(data))
    if backupdb:
        received_file = {'name': datfilepath,
                         'data': Binary(data, bson.binary.BINARY_SUBTYPE),
                         'published': False,
                         'time_received': datetime.datetime.utcnow()}
        mongoiddeferred = received_files.insert(received_file)
        mongoiddeferred.addCallback(finishprocessing)
    elif csv_mode and len(parsed) >= NUM_SECONDS_PER_FILE:
        publish()
        
def finishprocessing(mongoid):
    mongoids.append(mongoid)
    datalock.release()
    if csv_mode and len(parsed) >= NUM_SECONDS_PER_FILE:
        publish()
        
# Out of service - do not use
def publish():
    """ Attempts to publish data in PARSED to sMAP. Upon success, updates mongo documents with ids in
    MONGOIDS to indicate that their data have been published and returns True. Upon failure,
    returns False and writes a backup of the relevant files if backup using Mongo DB has been
    disabled. """
    global parsed, mongoids
    success = True
    with datalock:
        if not parsed:
            return True
        parsedcopy = parsed
        parsed = []
        mongoidscopy = mongoids
        mongoids = []
    try:
        print 'Publishing...'
        parsedcopy.sort(key=lambda x: x.sync_data.times)
        check_duplicates(parsedcopy)
        streamLists = tuple([] for _ in streams)
        for s in parsedcopy:
           basetime = time_to_nanos(s.sync_data.times)
           # it seems s.sync_data.sampleRate is the number of milliseconds between samples
           timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
           for i in xrange(120):
               currtime = basetime + int((i * timedelta) + 0.5)
               streamLists[0].append((currtime, s.sync_data.L1MagAng[i].mag))
               streamLists[1].append((currtime, s.sync_data.L1MagAng[i].angle))
               streamLists[2].append((currtime, s.sync_data.C1MagAng[i].mag))
               streamLists[3].append((currtime, s.sync_data.C1MagAng[i].angle))
               streamLists[4].append((currtime, s.gps_stats.satellites))
        streamIndex = 0
        for stream in streams:
            with stream[1]:
                stream[0].set_readings(streamLists[streamIndex])
                if not stream[0].publish():
                    success = False
                    print 'Could not publish stream'
                streamIndex += 1
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
            print 'Successfully published to {0}'.format(argv[1])
            if backupdb:
                try:
                    for mongoid in mongoidscopy:
                        received_files.update({'_id': mongoid}, {'$set': {'published': True}})
                except pymongo.errors.OperationFailure:
                    print 'WARNING: could not update Mongo Database with recent publish'
                    print 'Relevant IDs are:'
                    for mongoid in mongoidscopy:
                        print mongoid
            return True
        elif not backupdb:
            write_backup(parsedcopy) # on failure, write data to file if it could not be published
            return False


def write_csv():
    """ Attempts to write data in PARSED to CSV file. Upon success, updates mongo documents with ids in
    MONGOIDS to indicate that their data have been published and returns True. Upon failure,
    returns False and writes a backup of the relevant files if backup using Mongo DB has been
    disabled. """
    global parsed, mongoids
    success = True
    with datalock:
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
            if backupdb:
                for mongoid in mongoidscopy:
                    d = received_files.update({'_id': mongoid}, {'$set': {'published': True}})
                    d.addErrback(print_mongo_error)
            return True
        elif not backupdb:
            write_backup(parsedcopy)
            return False
            
def print_mongo_error(err):
    print 'WARNING: could not update Mongo Database with recent write'
    
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


t = None # A timer for publishing repeatedly
restart = True # Determines whether data will be published again

if csv_mode:
    publish = write_csv
else:
    # Set up thread for publishing repeatedly
    def publish_repeatedly():
        global t
        if restart:
            t = threading.Timer(NUM_SECONDS_PER_FILE, publish_repeatedly)
            t.start()
        publish()
    # Start publishing repeatedly
    publish_repeatedly()

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
            if len(self.have) >= 8:
                self.lengths, self.lengthd = struct.unpack('<ii', self.have[:8])
                self.paddingbytes = 4 - (self.lengths % 4)
                self.have = self.have[8:]
            else:
                return
        if self.filepath is None:
            if len(self.have) >= self.lengths:
                self.filepath = self.have[:self.lengths]
                self.have = self.have[self.lengths + self.paddingbytes:]
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
            process(self.data, self.filepath)
        except KeyboardInterrupt:
            self.sendid = '\x00\x00\x00\x00'
            raise
        except BaseException as err: # If there's an exception of any kind, set sendid
            print err
            traceback.print_exc()
            self.sendid = '\x00\x00\x00\x00'
        finally:
            self.transport.write(self.sendid)
            self.sendid = None
            self.lengths = None
            self.lengthd = None
            self.filepath = None
            self.data = None
            
    def connectionLost(self, reason):
        print 'Connection lost: ', self.transport.getPeer()
        
    def connectionMade(self):
        self.have = ''
        self.sendid = None
        self.lengths = None
        self.lengthd = None
        self.filepath = None
        self.data = None
        print 'Connected: ', self.transport.getPeer()

class ResolverFactory(Factory):
    def buildProtocol(self, addr):
        return TCPResolver()

def setup(mconn=None):
     global received_files
     if mconn is not None:
         received_files = mconn.upmu_database.received_files
     endpoint = TCP4ServerEndpoint(reactor, ADDRESSP)
     endpoint.listen(ResolverFactory())

def termerror(e):
    print "terminal error", e
    lg.error("TERMINAL ERROR: \n%s",e)
    return defer.FAILURE
         
if backupdb:
    d = txmongo.MongoConnection()
    d.addCallbacks(setup, termerror)
    d.addErrback(termerror)
else:
    setup()

reactor.run()

"""
# Receive and process data
connected = False
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
try:
    server_socket.bind(('', ADDRESSP))
except socket.error as se:
    print 'Could not set up server socket: {0}'.format(se)
    restart = False
    exit()

def receive_all_data(socket, numbytes):
    data = ''
    while numbytes > 0:
        newdata = socket.recv(numbytes)
        if len(newdata) <= 0:
            raise ConnectionTerminatedException('Could not receive data')
        numbytes -= len(newdata)
        data += newdata
    return data

def close_connection():
    if connected:
        connect_socket.shutdown(socket.SHUT_RDWR)
        connect_socket.close()
    server_socket.shutdown(socket.SHUT_RDWR)
    server_socket.close()

try:
    server_socket.listen(10)
    connect_socket, connect_addr = server_socket.accept()
    print 'Accepted connection'
    connected = True
    while True:
        try:
            # Receive the data
            sendid = receive_all_data(connect_socket, 4)
            length_string = receive_all_data(connect_socket, 8)
            lengths, lengthd = struct.unpack('<ii', length_string)
            assert lengths > 4
            datfilepath = receive_all_data(connect_socket, lengths)
            print 'Received {0}'.format(datfilepath)
            filepath = datfilepath[:-4] + '.csv'

            receive_all_data(connect_socket, 4 - (lengths % 4)) # get rid of padding bytes
            data = receive_all_data(connect_socket, lengthd)
            
            # Process the data
            try:
                process(data)
            except KeyboardInterrupt:
                sendid = '\x00\x00\x00\x00'
                raise
            except BaseException as err: # If there's an exception of any kind, set sendid
                print err
                sendid = '\x00\x00\x00\x00'
            finally:
                # Send confirmation of receipt
                bytesSent = 0
                while bytesSent < 4:
                    sentNow = connect_socket.send(sendid[bytesSent:])
                    if sentNow <= 0:
                        raise ConnectionTerminatedException('Could not send confirmation')
                    bytesSent += sentNow
        except (ConnectionTerminatedException, socket.error):
            try:
                connect_socket.shutdown(socket.SHUT_RDWR)
            except socket.error: # it may have been already shut down
                pass
            connect_socket.close()
            connected = False
            print 'Connection was terminated'
            print 'Attempting to reconnect...'
            connect_socket, connect_addr = server_socket.accept()
            print 'Accepted connection'
            connected = True
except KeyboardInterrupt:
    pass
except:
    print 'Exception:'
    traceback.print_exc()
finally:
    try:
        close_connection() # I don't think there will be any problems here...
    except BaseException as be:
        print 'Could not close connection: {0}'.format(be)
    finally: #... but the final publish needs to be done no matter what
        restart = False
        if t is not None:
            t.cancel()
        publish()
        exit()
"""
