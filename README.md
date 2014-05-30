uPMU-daemon
===========
The file "sender.c" contains code to detect when new directories and ".dat"
files are created in a given directory and send the ".dat" files to a server
using TCP.

The file "receiver.py" contains code to receive the ".dat" files and publish
them to sMAP, or write them to ".csv" files. Data has been published to
http://archiver.upmu.cal-sdb.org:8079.

The file "receiverold.py" contains an older version of receiver.py that blocks
input while it publishes.
