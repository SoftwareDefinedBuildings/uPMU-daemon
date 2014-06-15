#!/usr/bin/python

import argparse
import datetime
import pymongo
import smtplib
import sys
import time

from email.mime.text import MIMEText

last_warning_check = datetime.datetime.utcnow() # when we last checked for warnings
last_email_time = last_warning_check # when we last sent messages

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--checktime', help='the number of seconds to wait after polling the database', type=int, default=300)
parser.add_argument('-a', '--alerttime', help='the minimum number of seconds for which inactivity from a uPMU is considered abnormal', type=int, default=1800)
parser.add_argument('-e', '--emailtime', help='the minimum number of seconds between emails', type=int, default=900)
parser.add_argument('senderaddr', help='the email address from which messages will be sent')
parser.add_argument('receiveraddrs', help='the email addresses to which messages will be sent', nargs='+')
args = parser.parse_args()

CHECKTIME = args.checktime
ALERTTIME = args.alerttime
EMAILTIME = args.emailtime
SENDERADDR = args.senderaddr
RECEIVERADDRS = args.receiveraddrs

if len(sys.argv) < 3:
    print 'Usage: ./emailer.py <sender email address> <receiver email addresses>'
    exit()
    
    
alert = False # True if a uPMU has not send messages in ALERTTIME seconds

aliases = {}

try:
    with open('serial_aliases.ini', 'r') as f:
        for line in f:
            pair = line.rstrip().split('=')
            aliases[pair[0]] = pair[1]
except:
    print 'WARNING: Could not read serial_aliases.ini'

client = pymongo.MongoClient()
latest_time = client.upmu_database.latest_time
warnings = client.upmu_database.warnings

inactive_serials = set()
events = {} # maps each serial number to a list of EventMessages

def add_event(serialNumber, event):
    if serialNumber not in events:
        events[serialNumber] = []
    events[serialNumber].append(event)
    
def seconds_until_next_email():
    """ Returns the number of seconds that must elapse until the next email can be sent. """
    return EMAILTIME - (datetime.datetime.utcnow() - last_email_time).total_seconds()
    
def send_messages():
    global last_email_time, alert
    if len(events) == 0:
        return # nothing to send
    skeleton = """Hello,
This email is to inform you that events have ocurred regarding some uPMUs. The details are given below, listed by the serial number of each device and its alias, if known. All times listed below are in UTC.

{0}

This is an automated message. You should not reply to it."""
    lines = [] # A list of lines in the message
    for serialNum in events:
        alias = aliases.get(serialNum, 'no alias found')
        lines.append('Events regarding uPMU with serial number {0} ({1}):'.format(serialNum, alias))
        for event in events[serialNum]:
            lines.append('    {0}'.format(event))
        lines.append('') # A blank line to separate serial numbers
    txt = MIMEText(skeleton.format('\n'.join(lines)))
    del txt['Subject']
    if alert:
        txt['Subject'] = 'ALERT: one or more uPMUs are inactive'
    else:
        txt['Subject'] = 'Automated uPMU Notification'
    del txt['From']
    txt['From'] = SENDERADDR
    del txt['To']
    txt['To'] = ', '.join(RECEIVERADDRS)
    created = False
    try:
        emailsender = smtplib.SMTP('localhost')
        created = True
        refused = emailsender.sendmail(SENDERADDR, RECEIVERADDRS, txt.as_string())
        events.clear() # So we don't get an entry in the next send for this serial number unless there was actually an event
        emailsender.quit()
        created = False
        print 'Successfully sent email'
        alert = False
        for recipient in refused:
            print 'WARNING: recipient {0} was refused by the server and did not receive the email'.format(recipient)
    except smtplib.SMTPSenderRefused:
        print 'WARNING: email not sent because sender ({0}) was refused'.format(SENDERADDR)
    except smtplib.SMTPRecipientsRefused:
        print 'WARNING: email not sent because all recipients were refused'
    except BaseException as be:
        print 'WARNING: email not sent'
        print 'Details:', be
    finally:
        if created:
            emailsender.quit()
    last_email_time = datetime.datetime.utcnow()
        
    
class EventMessage(object):
    warning = False
    def __init__(self, description, event_time):
        self.description = description
        self.event_time = event_time
    def __str__(self):
        if self.description == 'active':
            return 'NOTE: no messages had been received from the uPMU for at least {0} seconds, but device resumed to send messages at {1}'.format(ALERTTIME, self.event_time)
        elif self.description == 'inactive':
            return 'NOTE: no messages have been received for at least {0} seconds; last message received at {1}'.format(ALERTTIME, self.event_time)
        return 'The event {0} ocurred at {1}.'.format(self.description, self.event_time)
    
class WarningMessage(EventMessage):
    warning = True
    def __init__(self, description, gen_time, start_time, end_time = None, prev_time = None):
        EventMessage.__init__(self, description, gen_time)
        self.start_time = start_time
        self.end_time = end_time
        self.prev_time = prev_time
    def __str__(self):
        if self.description == 'duplicate':
            return 'WARNING: duplicate record for {0} (message generated at {1})'.format(self.start_time, self.event_time)
        elif self.description == 'missing':
            return 'WARNING: missing record(s): no data from {0} to {1} (message generated at {2})'.format(self.start_time, self.end_time, self.event_time)
        elif self.description == 'misplaced':
            return 'WARNING: misplaced records(s) left uncorrected due to CSV boundary: new CSV file contains records from {0} to {1}, but would normally start at {2} (message generated at {3})'.format(self.start_time, self.end_time, self.prev_time, self.event_time)

while True:
    # Add messages to events queue
    for document in latest_time.find(): # Check for inactivity
        serialNumber = document['serial_number']
        lastReceived = document['time_received']
        if (datetime.datetime.utcnow() - lastReceived).total_seconds() < ALERTTIME:
            if serialNumber in inactive_serials: # if it's been fixed, mark it as active
                inactive_serials.remove(serialNumber)
                add_event(serialNumber, EventMessage('active', lastReceived))
        elif serialNumber not in inactive_serials: # only add an event if hasn't been marked inactive it so we don't send too much spam
            inactive_serials.add(serialNumber)
            add_event(serialNumber, EventMessage('inactive', lastReceived))
            alert = True
    warning_check = datetime.datetime.utcnow() # The time of this warning check
    for document in warnings.find({'warning_time': {'$gt': last_warning_check}}): # Check for warnings for missing/duplicate entries since the last time we checked
        add_event(document['serial_number'], WarningMessage(document['warning_type'], document['warning_time'], document['start_time'], document.get('end_time', None), document.get('prev_time', None)))
    last_warning_check = warning_check
    # Wait before checking again
    seconds = seconds_until_next_email()
    if seconds <= CHECKTIME:
        time.sleep(max(seconds, 0))
        send_messages()
        time.sleep(min(CHECKTIME, CHECKTIME - seconds))
    else:
        time.sleep(CHECKTIME)
