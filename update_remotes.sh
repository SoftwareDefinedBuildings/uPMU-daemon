#!/bin/bash

set -x

#Compile a new binary
export PATH=/srv/uPMU-daemon/arm-2011.03/bin:$PATH

make crosscompile

for target in up_soda_a up_soda_b up_71 up_grizzly up_switch
#for target in up_soda_b
do
	scp -i upmukey sender-arm admin@$target:/tmp/newtxagent
	ssh -i upmukey admin@$target "su -c 'killall 410txagent ; rm -f /tmp/410txagent.log && cp /tmp/newtxagent /root/410txagent && /etc/init.d/S80txagent start'"
done 
