#!/usr/bin/python

import calendar
import csv
import datetime
import os
import socket
import struct

from parser import sync_output, parse_sync_output

numouts = 0

ADDRESSP = 1883

NUM_SECONDS_PER_FILE = 15 * 60

processed = []

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
    processed.extend(parse(data))
    if len(processed) >= NUM_SECONDS_PER_FILE:
        processed.sort(key=lambda x: x.sync_data.times)
        # Check if the structs have duplicates or missing items, print warnings if so
        dates = tuple(datetime.datetime(*s.sync_data.times) for s in processed)
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
        write_csv()
    
    
  
            
def write_csv():
    global numouts, processed
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
    f = open('output/out{0}.csv'.format(numouts), 'wb')
    writer = csv.writer(f)
    writer.writerow(firstrow)
    rows = []
    for s in processed:
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
    processed = []
    
def receive_all_data(socket, numbytes):
    data = ''
    while numbytes > 0:
        newdata = socket.recv(numbytes)
        numbytes -= len(newdata)
        data += newdata
    return data

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
            bytesSent += sentNow
except Exception as e:
    server_socket.shutdown(socket.SHUT_RDWR)
    server_socket.close()
    if connected:
        connect_socket.shutdown(socket.SHUT_RDWR)
        connect_socket.close()
    raise
