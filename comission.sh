#!/bin/bash

SERIALNUM=P3001244
NAME=soda_a
HNAME=up_soda_a
IP=166.140.215.116
PUBKEY=$(cat upmukey.pub)

if [ $(cat /etc/hosts | grep $HNAME | wc -l) -eq 0 ]
then
	echo $IP $HNAME >> /etc/hosts
else
	echo "Skipping hostname add"
fi


ssh -i upmukey admin@$HNAME "su -c 'mkdir -p /home/admin/.ssh/; echo $PUBKEY > /home/admin/.ssh/authorized_keys'"
scp -i upmukey S80txagent admin@$HNAME:/tmp/
ssh -i upmukey admin@$HNAME "su -c 'cp /tmp/S80txagent /etc/init.d/; chmod a+x /etc/init.d/S80txagent'"

if [ $(cat serial_aliases.ini | grep $NAME | wc -l) -eq 0 ]
then
        echo $SERIALNUM=$NAME >> serial_aliases.ini
else
        echo "Skipping alias add"
fi

