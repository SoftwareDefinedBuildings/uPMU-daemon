uPMU-daemon
===========
This repository contains some of the tools needed to process the data produced
by uPMUs and store their metadata. sender.c and receivercsv.py are programs
that send data from the uPMUs to a server where the files are stored in Mongo
DM. The "manager2" program deploys sender.c on the uPMUs and updates their
metadata according to a configuration file. emailer.py allows one to receive
email notifications about irregularities in data collection.

sender and its controller S80txagent run on the uPMUs. All other programs run
on a server.
