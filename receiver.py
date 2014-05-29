#!/usr/bin/python

import calendar
import csv
import datetime
import os
import socket
import struct

from parser import sync_output, parse_sync_output
from sys import argv

numouts = 0

ADDRESSP = 1883

NUM_SECONDS_PER_FILE = 15 * 60

parsed = [] # Stores parsed sync_output structs

# Check command line arguments
if len(argv) not in (3, 4, 1):
    print 'Usage: ./receiver.py <archiver url> <subscription key> [<num seconds per publish>]'
    print 'OR ./receiver.py to write to CSV file instead (included for testing purposes only)'
    exit()

if len(argv) == 4:
    NUM_SECONDS_PER_FILE = int(argv[3])

if len(argv) != 1:
    from ssmap import Ssstream
    L1Mag = Ssstream(unitofTime='ns', unitofMeasure='mag', url=argv[1], subkey=argv[2])
    L1Ang = Ssstream(unitofTime='ns', unitofMeasure='deg', url=argv[1], subkey=argv[2])
    C1Mag = Ssstream(unitofTime='ns', unitofMeasure='mag', url=argv[1], subkey=argv[2])
    C1Ang = Ssstream(unitofTime='ns', unitofMeasure='deg', url=argv[1], subkey=argv[2])
    satellites = Ssstream(unitofTime='ns', unitofMeasure='', url=argv[1], subkey=argv[2])

class ConnectionTerminatedException(RuntimeError):
    pass
    
class ParseException(RuntimeError):
    pass

# The first row of every csv file has lables
firstrow = ['time', 'lockstate']
for start in ('L', 'C'):
    for num in xrange(1, 4):
        for end in ('Ang', 'Mag'):
            firstrow.append('{0}{1}{2}'.format(start, num, end))
firstrow.extend(['satellites', 'hasFix'])

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
    parsed.extend(parse(data))
    if len(parsed) >= NUM_SECONDS_PER_FILE:
        parsed.sort(key=lambda x: x.sync_data.times)
        # Check if the structs have duplicates or missing items, print warnings if so
        dates = tuple(datetime.datetime(*s.sync_data.times) for s in parsed)
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
        publish()

def publish():
    global parsed
    for stream in ('L1Mag', 'L1Ang', 'C1Mag', 'C1Ang', 'satellites'):
        exec(stream + 'Data = []') # for each stream, initialize a list containing data
    for s in parsed:
       basetime = time_to_nanos(s.sync_data.times)
       # it seems s.sync_data.sampleRate is the number of milliseconds between samples
       timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
       for i in xrange(120):
           currtime = basetime + int((i * timedelta) + 0.5)
           L1MagData.append((currtime, s.sync_data.L1MagAng[i].mag))
           L1AngData.append((currtime, s.sync_data.L1MagAng[i].angle))
           C1MagData.append((currtime, s.sync_data.C1MagAng[i].mag))
           C1AngData.append((currtime, s.sync_data.C1MagAng[i].angle))
           satellitesData.append((currtime, s.gps_stats.satellites))
    success = True
    for stream in ('L1Mag', 'L1Ang', 'C1Mag', 'C1Ang', 'satellites'):
        getattr(eval(stream), 'set_readings')(eval(stream + 'Data'))
        if not getattr(eval(stream), 'publish')():
            success = False
            print 'Could not publish stream {0}'.format(stream)
    if success:
        print 'Successfully published to {0}'.format(argv[1])
        parsed = []

def write_csv():
    global numouts, parsed
    if not os.path.exists('output/'):
        os.mkdir('output/')
    elif numouts == 0:
        numfiles = len(tuple(_ for _ in os.listdir('output/')))
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

if len(argv) == 1:
    publish = write_csv

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
    server_socket.shutdown(socket.SHUT_RDWR)
    server_socket.close()
    if connected:
        connect_socket.shutdown(socket.SHUT_RDWR)
        connect_socket.close()

# Receive and process data
connected = False
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
try:
    server_socket.bind(('localhost', ADDRESSP))
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
except KeyboardInterrupt:
    pass
except:
    raise
finally:
    publish()
    close_connection()
