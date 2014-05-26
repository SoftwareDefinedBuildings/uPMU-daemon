#!/usr/bin/python

import calendar
import csv
import datetime
import os
import socket
import struct

from parser import sync_output, parse_sync_output

ADDRESSP = 1883

processed = []

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
    

def process(structs, filepath):
    """ Converts DATA (a list of sync_object structs) to a csv file in the
    directory FILEPATH relative to the directory "output". """
    # Create a list of lists, where each list represents a row
    rows = []
    # The first row has lables
    firstrow = ['time', 'lockstate']
    for start in ('L', 'C'):
        for num in xrange(1, 4):
            for end in ('Ang', 'Mag'):
                firstrow.append('{0}{1}{2}'.format(start, num, end))
    firstrow.extend(['satellites', 'hasFix'])
    rows.append(firstrow)
    
    for s in structs:
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
            
    # Write to a csv file in the folder output
    sl = filepath.index('/')
    if not os.path.exists('output/{0}'.format(filepath[:sl])):
        os.makedirs('output/{0}'.format(filepath[:sl]))
    f = open('output/' + filepath, 'wb')
    writer = csv.writer(f)
    writer.writerows(rows)
    processed.append(structs)

# Receive and process data
closed = True
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
try:
    server_socket.bind(('localhost', ADDRESSP))
    server_socket.listen(10)
    while True:
        connect_socket, connect_addr = server_socket.accept()
        closed = False
        
        # Receive the data
        length_string = connect_socket.recv(8)
        lengths, lengthd = struct.unpack('<ii', length_string)
        assert lengths > 4
        filepath = connect_socket.recv(lengths)
        print 'Received {0}'.format(filepath)
        filepath = filepath[:-4] + '.csv'
        connect_socket.recv((4 - (lengths % 4)) % 4) # get rid of padding bytes
        data = connect_socket.recv(lengthd)
        process(parse(data), filepath)
        
        # Terminate the connection
        connect_socket.shutdown(socket.SHUT_RDWR);
        connect_socket.close();
        closed = True
except KeyboardInterrupt:
    server_socket.shutdown(socket.SHUT_RDWR);
    server_socket.close()
    if not closed:
        connect_socket.shutdown(socket.SHUT_RDWR);
        connect_socket.close();
