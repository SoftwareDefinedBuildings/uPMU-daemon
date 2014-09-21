#!/bin/bash

set -x

#Compile a new binary
export PATH=/home/sam/arm-2011.03/bin:$PATH

make crosscompile
target=184.23.19.175
for port in 8081 8082
#for target in up_soda_b
do
	scp -i upmukey -P $port sender-arm admin@$target:/tmp/newtxagent
	ssh -i upmukey -p $port admin@$target "su -c 'killall 410txagent ; rm -f /tmp/410txagent.log && cp /tmp/newtxagent /root/410txagent && /etc/init.d/S80txagent start'"
done 
