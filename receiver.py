#!/usr/bin/python

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

numouts = 0

ADDRESSP = 1883

NUM_SECONDS_PER_FILE = 10

parsed = [] # Stores parsed sync_output structs

csv_mode = False

# Check command line arguments
if len(argv) not in (3, 4) or (len(argv) == 4 and argv[1] == '-c') or (len(argv) == 3 and argv[1] != '-c'):
    print 'Usage: ./receiver.py <archiver url> <subscription key> <num clock seconds per publish>'
    print 'OR ./receiver.py -c <num data seconds per file> to write to CSV file instead (included for testing purposes only)'
    exit()
elif argv[1] == '-c':
    csv_mode = True
    print 'In CSV mode'
    print 'Normal usage: ./receiver.py <archiver url> <subscription key> [<num seconds per publish>]'

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
    NUM_SECONDS_PER_FILE = int(argv[3])
    from ssmap import Ssstream
    L1Mag = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'L1 Mag', None, 'ns', 'V', 'UTC', [], argv[1], argv[2]), []]
    L1Ang = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'L1 Ang', None, 'ns', 'deg', 'UTC', [], argv[1], argv[2]), []]
    C1Mag = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'C1 Mag', None, 'ns', 'A', 'UTC', [], argv[1], argv[2]), []]
    C1Ang = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'C1 Ang', None, 'ns', 'deg', 'UTC', [], argv[1], argv[2]), []]
    satellites = [Ssstream('grizzlypeak', 'Grizzly Peak uPMU', 'uPMU deployment', 'satellites', None, 'ns', 'deg', 'UTC', [], argv[1], argv[2]), []]
    streams = (L1Mag, L1Ang, C1Mag, C1Ang, satellites)

# Lock on data (to avoid concurrent writing to "parsed")
datalock = threading.Lock()

class ConnectionTerminatedException(RuntimeError):
    pass
    
class ParseException(RuntimeError):
    pass

def parse(string):
    """ Parses data (in the form of STRING) into a series of sync_output
objects. Returns a list of sync_output objects. If STRING is not of a
suitable length (i.e., if the number of bytes is not some multiple of
the length of a sync_output struct) a RuntimeError is raised. """
    if len(string) % sync_output.LENGTH != 0:
        raise RuntimeError('Input to \"parse\" does not contain whole number of \"sync_output\"s ({0} extra bytes)'.format(len(string) % sync_output.LENGTH))
    lst = []
    while string:
        obj, string = parse_sync_output(string)
        lst.append(obj)
    return lst

def time_to_nanos(lst):
    """ Converts the time as given in a time[] array into nanoseconds since
the epoch. """
    return 1000000000 * calendar.timegm(datetime.datetime(*lst).utctimetuple())
    

def process(data):
    """ Converts DATA (in the form of a string) to sync_output objects and
adds them to the list of processed objects. When enough objects have been
processed, they are converted to a CSV file"""
    datalock.acquire()
    try:
        parsed.extend(parse(data))
    finally:
        datalock.release()
    if csv_mode and len(parsed) >= NUM_SECONDS_PER_FILE:
        publish()

def check_duplicates(sorted_struct_list):
    # Check if the structs have duplicates or missing items, print warnings if so
    dates = tuple(datetime.datetime(*s.sync_data.times) for s in sorted_struct_list)
    i = 1
    while i < len(dates):
        date1 = dates[i-1]
        date2 = dates[i]
        delta = int((date2 - date1).total_seconds() + 0.5) # round difference to nearest second
        if delta == 0:
            print 'WARNING: duplicate record for {0}'.format(str(date2))
        elif delta != 1:
            print 'WARNING: missing record(s) (skips from {0} to {1})'.format(str(date1), str(date2))
        i += 1
        
def publish():
    global parsed
    success = True
    datalock.acquire()
    if not parsed:
        datalock.release()
        return True
    parsedcopy = parsed
    parsed = []
    datalock.release()
    print 'Publishing...'
    parsedcopy.sort(key=lambda x: x.sync_data.times)
    check_duplicates(parsedcopy)
    for stream in streams:
        stream[1] = []
    for s in parsedcopy:
       basetime = time_to_nanos(s.sync_data.times)
       # it seems s.sync_data.sampleRate is the number of milliseconds between samples
       timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
       for i in xrange(120):
           currtime = basetime + int((i * timedelta) + 0.5)
           L1Mag[1].append((currtime, s.sync_data.L1MagAng[i].mag))
           L1Ang[1].append((currtime, s.sync_data.L1MagAng[i].angle))
           C1Mag[1].append((currtime, s.sync_data.C1MagAng[i].mag))
           C1Ang[1].append((currtime, s.sync_data.C1MagAng[i].angle))
           satellites[1].append((currtime, s.gps_stats.satellites))
    try:
        for stream in streams:
            stream[0].set_readings(stream[1])
            if not stream[0].publish():
                success = False
                print 'Could not publish stream'
        if success:
            print 'Successfully published to {0}'.format(argv[1])
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
            return True
        else:
            print 'Writing backup...' # on failure, write data to file if it could not be published
            if not os.path.exists('output/'):
                os.mkdir('output/')
            numfiles = len(os.listdir('output/'))
            numouts = numfiles
            while numouts > 0 and not os.path.exists('output/out{0}.dat'.format(numouts)):
                numouts -= 1
            numouts += 1
            backup = open('output/out{0}.dat'.format(numouts), 'w')
            for s in parsedcopy:
                backup.write(s.data)
            backup.close()
            print 'Done writing backup.'
            return False

def write_csv():
    global numouts, parsed
    if not parsed:
        return
    parsed.sort(key=lambda x: x.sync_data.times)
    check_duplicates(parsed)
    if not os.path.exists('output/'):
        os.mkdir('output/')
    elif numouts == 0:
        numfiles = len(os.listdir('output/'))
        while not os.path.exists('output/out{0}.csv'.format(numouts)):
            numouts -= 1
        numouts += 1
    else:
        while os.path.exists('output/out{0}.csv'.format(numouts)):
            numouts += 1
    print 'Writing file output/out{0}.csv'.format(numouts)
    f = open('output/out{0}.csv'.format(numouts), 'wb')
    writer = csv.writer(f)
    writer.writerow(firstrow)
    rows = []
    for s in parsed:
        basetime = time_to_nanos(s.sync_data.times)
        # it seems s.sync_data.sampleRate is the number of milliseconds between samples
        timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
        i = 0
        while i < 120:
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
            i += 1
            rows.append(row)
    writer.writerows(rows)
    parsed = []
    return True


t = None # A timer for publishing repeatedly
restart = True # Determines whether data will be published again

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

# Receive and process data
connected = False
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
try:
    server_socket.bind(('', ADDRESSP))
except socket.error as se:
    print 'Could not set up server socket: {0}'.format(se)
    restart = False
    exit()
    
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
            filepath = receive_all_data(connect_socket, lengths)
            print 'Received {0}'.format(filepath)
            filepath = filepath[:-4] + '.csv'

            receive_all_data(connect_socket, 4 - (lengths % 4)) # get rid of padding bytes
            data = receive_all_data(connect_socket, lengthd)
            
            # Process the data
            process(data)
            
            # Send confirmation of receipt
            bytesSent = 0
            while bytesSent < 4:
                sentNow = connect_socket.send(sendid[bytesSent:])
                if sentNow <= 0:
                    raise ConnectionTerminatedException('Could not send confirmation')
                bytesSent += sentNow
        except (ConnectionTerminatedException, socket.error):
            connect_socket.shutdown(socket.SHUT_RDWR)
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
        print 'Exception:'
        traceback.print_exc()
    finally: #... but the final publish needs to be done no matter what
        restart = False
        if t is not None:
            t.cancel()
        publish()
        exit()
